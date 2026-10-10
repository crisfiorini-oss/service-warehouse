package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	bq "cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// Scan reads a table through a BigQuery Storage Read API session, which is not a
// query job: the identity needs bigquery.readsessions.create and
// bigquery.tables.getData (and bigquery.tables.get, for the metadata that checks
// the request against the table), and never bigquery.jobs.create.
//
// The filter becomes the session's row restriction (restriction.go) and the
// selected columns its selected fields. The session is split into streams that
// are read in parallel, at most SWH_SCAN_STREAMS of them, and merged into one
// result; a stream that breaks is reopened at the row it reached. The estimate
// the session makes of the bytes it will scan is the header's before any batch,
// and is held to SWH_MAX_QUERY_BYTES as the byte cap of a query is.
func (b *Backend) Scan(ctx context.Context, ref backend.TableRef, opts backend.ScanOptions) (*backend.QueryResult, error) {
	const op = "Scan"
	if err := backend.ValidateScan(op, opts); err != nil {
		return nil, err
	}
	ref, err := b.resolve(op, ref)
	if err != nil {
		return nil, err
	}

	var cancel context.CancelFunc
	if timeout := limit(0, b.cfg.QueryTimeout); timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	// The context outlives this call when the streams are still to be read, and is
	// then cancelled when the reader closes.
	handedOver := false
	defer func() {
		if !handedOver {
			cancel()
		}
	}()

	md, err := b.client.Dataset(ref.Dataset).Table(ref.Table).Metadata(ctx)
	if err != nil {
		return nil, classify(op, err)
	}
	if tableKind(md.Type) == backend.KindView {
		return nil, serr.New(serr.Unsupported, op, "a view cannot be read without a query; scan its table or query it")
	}
	columns, err := scanColumns(md.Schema, opts.Selected)
	if err != nil {
		return nil, err
	}
	where, err := restriction(md.Schema, opts.Filter)
	if err != nil {
		return nil, err
	}

	client, err := b.readClient(ctx)
	if err != nil {
		return nil, err
	}
	req := &storagepb.CreateReadSessionRequest{
		Parent: "projects/" + b.project,
		ReadSession: &storagepb.ReadSession{
			Table:      fmt.Sprintf("projects/%s/datasets/%s/tables/%s", b.project, ref.Dataset, ref.Table),
			DataFormat: storagepb.DataFormat_ARROW,
			ReadOptions: &storagepb.ReadSession_TableReadOptions{
				RowRestriction: where,
			},
		},
		MaxStreamCount: int32(b.scanStreams()),
	}
	if len(opts.Selected) > 0 {
		req.ReadSession.ReadOptions.SelectedFields = columnNames(columns)
	}
	created := time.Now()
	session, err := client.CreateReadSession(ctx, req)
	if err != nil {
		return nil, classify(op, err)
	}
	estimate := session.GetEstimatedTotalBytesScanned()
	if ceiling := b.cfg.MaxQueryBytes; ceiling > 0 && estimate > ceiling {
		return nil, serr.New(serr.Throttled, op, fmt.Sprintf(
			"the scan is estimated to read %d bytes, over the limit of %d bytes (SWH_MAX_QUERY_BYTES)", estimate, ceiling))
	}

	layout, err := newScanLayout(session.GetArrowSchema().GetSerializedSchema(), columns)
	if err != nil {
		return nil, err
	}
	header, err := arrowipc.SchemaMessage(layout.out)
	if err != nil {
		return nil, serr.Wrap(serr.Internal, op, err)
	}
	enc, err := arrowipc.NewEncoder(layout.out)
	if err != nil {
		return nil, serr.Wrap(serr.Internal, op, err)
	}

	streams := make([]string, 0, len(session.GetStreams()))
	for _, s := range session.GetStreams() {
		streams = append(streams, s.GetName())
	}
	r := newScanReader(ctx, cancel, client, layout, enc, b.maxMessageBytes(), streams)
	handedOver = true
	return &backend.QueryResult{
		// A scan has no job to look up, so there is no ID; the estimate is the one
		// figure the session gives before it reads.
		Job: backend.Job{
			State:   backend.JobRunning,
			Created: created,
			Started: created,
			Stats:   backend.JobStats{BytesScanned: estimate},
		},
		Schema:      columnsFromSchema(fieldsOf(columns)),
		ArrowSchema: header,
		Batches:     r,
	}, nil
}

// scanColumns resolves the selected names against the table's schema, giving the
// columns a scan returns in the order it returns them: the selection's, or every
// column's when the selection is empty. A name the table lacks is InvalidArgument.
func scanColumns(schema bq.Schema, selected []string) ([]*bq.FieldSchema, error) {
	if len(selected) == 0 {
		return schema, nil
	}
	out := make([]*bq.FieldSchema, len(selected))
	for i, name := range selected {
		f := findField(schema, name)
		if f == nil {
			return nil, serr.New(serr.InvalidArgument, "Scan", fmt.Sprintf("the table has no column %q", name))
		}
		out[i] = f
	}
	return out, nil
}

func columnNames(fields []*bq.FieldSchema) []string {
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return names
}

func fieldsOf(fields []*bq.FieldSchema) bq.Schema { return bq.Schema(fields) }

// scanStreams is how many streams to ask the session for.
func (b *Backend) scanStreams() int {
	if b.cfg.ScanStreams > 0 {
		return b.cfg.ScanStreams
	}
	return defaultScanStreams
}

// maxMessageBytes is the most one batch message of a scan may hold.
func (b *Backend) maxMessageBytes() int {
	if b.cfg.MaxArrowMessageBytes > 0 {
		return int(b.cfg.MaxArrowMessageBytes)
	}
	return int(arrowipc.DefaultMaxMessageBytes)
}

const (
	// defaultScanStreams is the streams a session is split into when
	// SWH_SCAN_STREAMS is unset.
	defaultScanStreams = 4
	// readAttempts is how many times in a row one stream may fail without
	// delivering a row before the scan fails.
	readAttempts = 5
	// maxReadMessageBytes bounds one response of the Storage Read API, which
	// BigQuery sizes and this server does not control: far above the batches this
	// server sends, and still a bound on what it will decode.
	maxReadMessageBytes = 128 << 20
)

// readBackoff is the pause before a broken stream is reopened. A variable so a
// test need not wait.
var readBackoff = gax.Backoff{Initial: 100 * time.Millisecond, Max: 5 * time.Second}

// readClient is the Storage Read API surface a scan uses.
type readClient interface {
	CreateReadSession(ctx context.Context, req *storagepb.CreateReadSessionRequest, opts ...gax.CallOption) (*storagepb.ReadSession, error)
	ReadRows(ctx context.Context, req *storagepb.ReadRowsRequest, opts ...gax.CallOption) (storagepb.BigQueryRead_ReadRowsClient, error)
	Close() error
}

// readSessions holds the Storage Read API client a Backend makes on its first
// scan, so a server that never scans never opens a second connection.
type readSessions struct {
	mu     sync.Mutex
	client readClient
	// open makes the client. The server builds it from the credentials of the BigQuery
	// client; a test replaces it with one that talks to a fake.
	open func(ctx context.Context) (readClient, error)
}

func newReadSessions(opts []option.ClientOption) *readSessions {
	return &readSessions{open: func(ctx context.Context) (readClient, error) {
		c, err := storage.NewBigQueryReadClient(ctx, opts...)
		if err != nil {
			return nil, err
		}
		return c, nil
	}}
}

func (b *Backend) readClient(ctx context.Context) (readClient, error) {
	b.sessions.mu.Lock()
	defer b.sessions.mu.Unlock()
	if b.sessions.client != nil {
		return b.sessions.client, nil
	}
	// The client keeps the context it is made with for refreshing credentials, so
	// it must not die with the request that happened to make it.
	c, err := b.sessions.open(context.WithoutCancel(ctx))
	if err != nil {
		return nil, serr.Wrap(serr.Internal, "Scan", fmt.Errorf("create the BigQuery Storage Read client: %w", err))
	}
	b.sessions.client = c
	return c, nil
}

func (s *readSessions) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		return nil
	}
	err := s.client.Close()
	s.client = nil
	return err
}

// scanLayout is how the columns of a session's batches become the columns of a
// scan's: the session names them in the table's order and the scan returns them
// in the order the request chose.
type scanLayout struct {
	// session is the schema the session's serialized schema message describes,
	// and the one its batches are read against.
	session []byte
	// out is the schema of what a scan returns.
	out *arrow.Schema
	// pick[i] is the index in the session's batches of output column i.
	pick []int
}

// emptySource is an Arrow stream with no messages after its schema.
type emptySource struct{}

func (emptySource) Next() ([]byte, error) { return nil, io.EOF }

func newScanLayout(sessionSchema []byte, columns []*bq.FieldSchema) (*scanLayout, error) {
	const op = "Scan"
	if len(sessionSchema) == 0 {
		return nil, serr.New(serr.Internal, op, "the read session carried no Arrow schema")
	}
	rd, err := arrowipc.NewReader(sessionSchema, emptySource{}, arrowipc.WithMaxMessageBytes(maxReadMessageBytes))
	if err != nil {
		return nil, serr.Wrap(serr.Internal, op, fmt.Errorf("the read session's Arrow schema cannot be read: %w", err))
	}
	defer rd.Release()
	in := rd.Schema()

	l := &scanLayout{session: sessionSchema, pick: make([]int, len(columns))}
	fields := make([]arrow.Field, len(columns))
	for i, c := range columns {
		at := -1
		for j, f := range in.Fields() {
			if strings.EqualFold(f.Name, c.Name) {
				at = j
				break
			}
		}
		if at < 0 {
			return nil, serr.New(serr.Internal, op, fmt.Sprintf("the read session has no column %q that the table has", c.Name))
		}
		l.pick[i], fields[i] = at, in.Field(at)
	}
	if len(in.Fields()) != len(columns) {
		return nil, serr.New(serr.Internal, op, fmt.Sprintf("the read session returns %d columns, and %d were asked for", len(in.Fields()), len(columns)))
	}
	meta := in.Metadata()
	l.out = arrow.NewSchema(fields, &meta)
	return l, nil
}

// reorder is rec with its columns in the scan's order.
func (l *scanLayout) reorder(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	if int(rec.NumCols()) != len(l.pick) {
		return nil, serr.New(serr.Internal, "Scan", fmt.Sprintf("a batch has %d columns, and the read session described %d", rec.NumCols(), len(l.pick)))
	}
	cols := make([]arrow.Array, len(l.pick))
	for i, at := range l.pick {
		cols[i] = rec.Column(at)
	}
	return array.NewRecordBatch(l.out, cols, rec.NumRows()), nil
}

// scanReader merges the streams of a read session into one sequence of batch
// messages. Each stream is read by a goroutine that decodes its responses,
// reorders their columns and cuts them to the size of a result batch; the one
// consumer of Next encodes them, so the Arrow encoder is never shared.
type scanReader struct {
	layout  *scanLayout
	enc     *arrowipc.Encoder
	maxSize int
	cancel  context.CancelFunc

	// out carries cut batches from the streams to Next, and is closed, with err
	// set, once every stream has ended. A small buffer keeps the streams a little
	// ahead of the consumer and no further: memory does not grow with the table.
	out chan arrow.RecordBatch
	err error

	// halves are batches that were cut again because their message came out over
	// the size limit; only Next touches them.
	halves []arrow.RecordBatch

	closeOnce sync.Once
	closed    bool
}

func newScanReader(ctx context.Context, cancel context.CancelFunc, client readClient, layout *scanLayout,
	enc *arrowipc.Encoder, maxSize int, streams []string) *scanReader {
	r := &scanReader{
		layout:  layout,
		enc:     enc,
		maxSize: maxSize,
		cancel:  cancel,
		out:     make(chan arrow.RecordBatch, max(1, len(streams))),
	}
	group, ctx := errgroup.WithContext(ctx)
	for _, name := range streams {
		group.Go(func() error {
			return r.readStream(ctx, client, name)
		})
	}
	go func() {
		// classify turns the first failure of any stream into its serr code once, so
		// a permission failure stays one and a cause is logged for the operator.
		r.err = classify("Scan", group.Wait())
		close(r.out)
	}()
	return r
}

// emit hands one batch to Next, or gives up when the scan is over.
func (r *scanReader) emit(ctx context.Context, rec arrow.RecordBatch) error {
	select {
	case r.out <- rec:
		return nil
	case <-ctx.Done():
		rec.Release()
		return ctx.Err()
	}
}

// readStream reads one stream to its end, reopening it at the row it reached
// after a failure that a retry can cure. The attempts are for a stream that makes
// no headway: a failure after rows were delivered starts the count, and the
// backoff, over, so a long stream that breaks now and then never runs out while
// it keeps moving.
func (r *scanReader) readStream(ctx context.Context, client readClient, name string) error {
	var offset int64
	backoff, failures := readBackoff, 0
	for {
		reached := offset
		rows, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: name, Offset: offset})
		if err == nil {
			if offset, err = r.consume(ctx, rows, offset); err == nil {
				// A stream that ends cleanly as the scan's time runs out may have been
				// cut short; reporting it complete would hand back fewer rows as all.
				return ctx.Err()
			}
		}
		if offset > reached {
			backoff, failures = readBackoff, 0
		}
		if ctx.Err() != nil || !retryable(err) {
			return err
		}
		if failures++; failures >= readAttempts {
			return err
		}
		if err := gax.Sleep(ctx, backoff.Pause()); err != nil {
			return err
		}
	}
}

// retryable is whether a failed read may succeed if the stream is opened again.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Aborted, codes.DeadlineExceeded, codes.Internal, codes.Unavailable, codes.ResourceExhausted:
		return true
	}
	return false
}

// consume reads responses until the stream ends and returns the offset it has
// reached: the number of rows of this stream that were handed on. The offset
// moves only after a response's rows are delivered, so a reopened stream neither
// repeats a row nor skips one.
func (r *scanReader) consume(ctx context.Context, rows storagepb.BigQueryRead_ReadRowsClient, offset int64) (int64, error) {
	for {
		response, err := rows.Recv()
		if errors.Is(err, io.EOF) {
			return offset, nil
		}
		if err != nil {
			return offset, err
		}
		batch := response.GetArrowRecordBatch().GetSerializedRecordBatch()
		if len(batch) == 0 {
			continue
		}
		n, err := r.deliver(ctx, batch)
		if err != nil {
			return offset, err
		}
		offset += n
	}
}

// deliver decodes one response, reorders its columns and hands it on in batches
// of the size a result carries. It returns the rows the response held.
func (r *scanReader) deliver(ctx context.Context, message []byte) (int64, error) {
	rd, err := arrowipc.NewReader(r.layout.session, &oneMessage{message: message}, arrowipc.WithMaxMessageBytes(maxReadMessageBytes))
	if err != nil {
		return 0, serr.Wrap(serr.Internal, "Scan", fmt.Errorf("a response of the read session cannot be read: %w", err))
	}
	defer rd.Release()
	var rows int64
	for rd.Next() {
		rec, err := r.layout.reorder(rd.RecordBatch())
		if err != nil {
			return rows, err
		}
		rows += rec.NumRows()
		err = r.cut(ctx, rec)
		rec.Release()
		if err != nil {
			return rows, err
		}
	}
	if err := rd.Err(); err != nil {
		return rows, serr.Wrap(serr.Internal, "Scan", fmt.Errorf("a response of the read session cannot be read: %w", err))
	}
	return rows, nil
}

// oneMessage is an Arrow source of exactly one message.
type oneMessage struct {
	message []byte
	sent    bool
}

func (o *oneMessage) Next() ([]byte, error) {
	if o.sent {
		return nil, io.EOF
	}
	o.sent = true
	return o.message, nil
}

// cut hands rec on in batches of at most batchMaxRows rows and about
// batchTargetBytes bytes, the size a Query result carries, so one response of any
// size becomes messages a client can receive.
func (r *scanReader) cut(ctx context.Context, rec arrow.RecordBatch) error {
	step := rowsPerBatch(rec)
	for from := int64(0); from < rec.NumRows(); from += step {
		piece := rec.NewSlice(from, min(from+step, rec.NumRows()))
		if err := r.emit(ctx, piece); err != nil {
			return err
		}
	}
	return nil
}

// rowsPerBatch is how many rows of rec make a batch of the size a result carries,
// by the average row of rec. Next cuts a batch again if its message still comes
// out too large.
func rowsPerBatch(rec arrow.RecordBatch) int64 {
	rows := rec.NumRows()
	if rows <= 0 {
		return batchMaxRows
	}
	var bytes int64
	for _, col := range rec.Columns() {
		bytes += dataBytes(col.Data())
	}
	perRow := max(1, bytes/rows)
	return min(batchMaxRows, max(1, batchTargetBytes/perRow))
}

// dataBytes is the size of the buffers an array and its children hold.
func dataBytes(d arrow.ArrayData) int64 {
	var n int64
	for _, buf := range d.Buffers() {
		if buf != nil {
			n += int64(buf.Len())
		}
	}
	for _, child := range d.Children() {
		n += dataBytes(child)
	}
	return n
}

// Next returns the next batch message. A batch whose message is over the size
// limit is cut in two and tried again; one row that is over it alone cannot be
// sent, and fails the scan.
func (r *scanReader) Next() ([]byte, error) {
	if r.closed {
		return nil, io.EOF
	}
	for {
		rec, ok := r.pop()
		if !ok {
			if r.err != nil {
				return nil, r.err
			}
			return nil, io.EOF
		}
		msg, err := r.enc.Encode(rec)
		if err != nil {
			rec.Release()
			return nil, serr.Wrap(serr.Internal, "Scan", err)
		}
		if len(msg) <= r.maxSize {
			rec.Release()
			return msg, nil
		}
		rows := rec.NumRows()
		if rows <= 1 {
			rec.Release()
			return nil, serr.New(serr.Unsupported, "Scan", fmt.Sprintf(
				"a row is larger than the limit of %d bytes for one message (SWH_MAX_ARROW_MESSAGE_BYTES), so it cannot be sent", r.maxSize))
		}
		first, second := rec.NewSlice(0, rows/2), rec.NewSlice(rows/2, rows)
		rec.Release()
		// The second half goes under the first, so the order of rows is kept.
		r.halves = append(r.halves, second, first)
	}
}

// pop takes the next batch: a half still owed first, else the next from the
// streams. It reports false at the end of the scan.
func (r *scanReader) pop() (arrow.RecordBatch, bool) {
	if n := len(r.halves); n > 0 {
		rec := r.halves[n-1]
		r.halves = r.halves[:n-1]
		return rec, true
	}
	rec, ok := <-r.out
	return rec, ok
}

// Close stops the streams and releases what they had not delivered.
func (r *scanReader) Close() error {
	r.closeOnce.Do(func() {
		r.closed = true
		r.cancel()
		for _, rec := range r.halves {
			rec.Release()
		}
		r.halves = nil
		// The streams stop on the cancelled context; what they had queued is
		// released, and out closes when the last of them has gone.
		for rec := range r.out {
			rec.Release()
		}
		_ = r.enc.Close()
	})
	return nil
}
