package bigquery

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"sync"
	"testing"

	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
)

// The tests of Scan run the backend against a fake of the Storage Read API's gRPC
// service (BigQueryRead), served from this process over an in-memory connection,
// and a REST fake (fake_test.go) for the table metadata a scan reads first. The
// fake is not BigQuery: it serves the streams a test scripts and records what the
// client asked, so a test can assert the session the backend creates and the way
// it reads, merges and resumes the streams. What the real service does is the
// integration test's to say.

// sessionSchema is the schema the fake's sessions describe: id and name, in the
// table's order.
var sessionSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// response is one ReadRowsResponse: some rows of (id, name), sent as the
// serialized record batch a session sends (an Arrow IPC record-batch message with
// no schema) over the columns the session was created with.
type response struct {
	ids   []int64
	names []string
}

func (r response) rows() int64 { return int64(len(r.ids)) }

// scriptedStream is what a stream serves, and how it fails.
type scriptedStream struct {
	responses []response
	// dropAfter, when > 0, ends the first read of the stream with dropErr after
	// that many responses; later reads are served in full.
	dropAfter int
	dropErr   error
	// failAlways ends every read of the stream with this error, before any row.
	failAlways error
	// hold keeps a read open after its last response until the client goes away.
	hold bool
}

type fakeRead struct {
	storagepb.UnimplementedBigQueryReadServer

	mu       sync.Mutex
	create   []*storagepb.CreateReadSessionRequest
	reads    []*storagepb.ReadRowsRequest
	session  *storagepb.ReadSession
	streams  map[string]*scriptedStream
	createFn func(*storagepb.CreateReadSessionRequest) (*storagepb.ReadSession, error)
	// columns are the columns of the session created last, in the table's order.
	columns []string
	// gone is closed when a held read notices its client left.
	gone     chan struct{}
	goneOnce sync.Once
}

func (f *fakeRead) CreateReadSession(_ context.Context, req *storagepb.CreateReadSessionRequest) (*storagepb.ReadSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.create = append(f.create, req)
	if f.createFn != nil {
		return f.createFn(req)
	}
	if f.session == nil {
		return nil, status.Error(codes.Internal, "the fake has no session")
	}
	// As BigQuery does, the session describes only the selected fields, in the
	// table's order and not the order they were listed in.
	selected := req.GetReadSession().GetReadOptions().GetSelectedFields()
	f.columns = nil
	for _, field := range sessionSchema.Fields() {
		if len(selected) == 0 || contains(selected, field.Name) {
			f.columns = append(f.columns, field.Name)
		}
	}
	session := proto.Clone(f.session).(*storagepb.ReadSession)
	schema, err := arrowipc.SchemaMessage(projected(f.columns))
	if err != nil {
		return nil, err
	}
	session.Schema = &storagepb.ReadSession_ArrowSchema{ArrowSchema: &storagepb.ArrowSchema{SerializedSchema: schema}}
	return session, nil
}

func contains(list []string, name string) bool {
	for _, n := range list {
		if n == name {
			return true
		}
	}
	return false
}

// projected is sessionSchema restricted to the named columns.
func projected(names []string) *arrow.Schema {
	var fields []arrow.Field
	for _, f := range sessionSchema.Fields() {
		if contains(names, f.Name) {
			fields = append(fields, f)
		}
	}
	return arrow.NewSchema(fields, nil)
}

func (f *fakeRead) ReadRows(req *storagepb.ReadRowsRequest, stream storagepb.BigQueryRead_ReadRowsServer) error {
	f.mu.Lock()
	f.reads = append(f.reads, req)
	script := f.streams[req.GetReadStream()]
	columns := f.columns
	first := 0
	for _, r := range f.reads {
		if r.GetReadStream() == req.GetReadStream() {
			first++
		}
	}
	f.mu.Unlock()

	if script == nil {
		return status.Errorf(codes.NotFound, "no such stream %s", req.GetReadStream())
	}
	if script.hold {
		// However the read ends, it ends because the client went away.
		defer f.goneOnce.Do(func() { close(f.gone) })
	}
	if script.failAlways != nil {
		return script.failAlways
	}
	// The stream is served from the response that starts at the offset asked for.
	var start int64
	i := 0
	for i < len(script.responses) && start < req.GetOffset() {
		start += script.responses[i].rows()
		i++
	}
	if start != req.GetOffset() {
		return status.Errorf(codes.InvalidArgument, "offset %d is not a response boundary", req.GetOffset())
	}
	for sent, r := range script.responses[i:] {
		if first == 1 && script.dropAfter > 0 && sent >= script.dropAfter {
			return script.dropErr
		}
		message, err := serialize(r, columns)
		if err != nil {
			return err
		}
		err = stream.Send(&storagepb.ReadRowsResponse{
			Rows:     &storagepb.ReadRowsResponse_ArrowRecordBatch{ArrowRecordBatch: &storagepb.ArrowRecordBatch{SerializedRecordBatch: message, RowCount: r.rows()}},
			RowCount: r.rows(),
		})
		if err != nil {
			return err
		}
	}
	if script.hold {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return nil
}

func (f *fakeRead) requests() (create []*storagepb.CreateReadSessionRequest, reads []*storagepb.ReadRowsRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append(create, f.create...), append(reads, f.reads...)
}

// newSession is a session over the named streams, with the estimate given. Its
// schema is filled in when it is created, from the fields the request selected.
func newSession(estimate int64, names ...string) *storagepb.ReadSession {
	s := &storagepb.ReadSession{
		Name:                       "projects/test-project/locations/us/sessions/s1",
		EstimatedTotalBytesScanned: estimate,
	}
	for _, n := range names {
		s.Streams = append(s.Streams, &storagepb.ReadStream{Name: n})
	}
	return s
}

// responses makes rows of (id, name) in responses of the given sizes, ids
// counting up from firstID, each name "<prefix><id>".
func responses(firstID int64, prefix string, sizes ...int) []response {
	var out []response
	id := firstID
	for _, n := range sizes {
		r := response{ids: make([]int64, n), names: make([]string, n)}
		for i := range r.ids {
			r.ids[i], r.names[i] = id, fmt.Sprintf("%s%d", prefix, id)
			id++
		}
		out = append(out, r)
	}
	return out
}

// serialize is the record-batch message of a response over the given columns.
func serialize(r response, columns []string) ([]byte, error) {
	schema := projected(columns)
	enc, err := arrowipc.NewEncoder(schema)
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	ib := array.NewInt64Builder(memory.DefaultAllocator)
	defer ib.Release()
	sb := array.NewStringBuilder(memory.DefaultAllocator)
	defer sb.Release()
	ib.AppendValues(r.ids, nil)
	sb.AppendValues(r.names, nil)
	ic, sc := ib.NewArray(), sb.NewArray()
	defer ic.Release()
	defer sc.Release()
	var cols []arrow.Array
	for _, f := range schema.Fields() {
		if f.Name == "id" {
			cols = append(cols, ic)
		} else {
			cols = append(cols, sc)
		}
	}
	rec := array.NewRecordBatch(schema, cols, int64(len(r.ids)))
	defer rec.Release()
	return enc.Encode(rec)
}

// scanFixture is a backend whose tables come from a REST fake and whose scans go
// to a gRPC fake.
type scanFixture struct {
	rest *fake
	read *fakeRead
	b    *Backend

	mu     sync.Mutex
	status int
	body   any
}

// newScanFixture serves table d.t with columns id (INT64) and name (STRING).
func newScanFixture(t *testing.T, cfg backend.Config) *scanFixture {
	t.Helper()
	quiet(t)
	f := newFake(t)
	sf := &scanFixture{rest: f}
	sf.table(200, map[string]any{
		"tableReference": tableRef("d", "t"),
		"type":           "TABLE",
		"schema": map[string]any{"fields": []map[string]any{
			{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
			{"name": "name", "type": "STRING", "mode": "NULLABLE"},
		}},
	})
	f.handleFunc("GET "+tablePath, func(w http.ResponseWriter, _ *http.Request) {
		sf.mu.Lock()
		defer sf.mu.Unlock()
		writeJSON(w, sf.status, sf.body)
	})
	fr := &fakeRead{streams: map[string]*scriptedStream{}, gone: make(chan struct{})}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	storagepb.RegisterBigQueryReadServer(srv, fr)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	b := f.open(t, cfg)
	b.sessions.open = func(ctx context.Context) (readClient, error) {
		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// What the generated client sets on the connection it makes itself.
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))
		if err != nil {
			return nil, err
		}
		c, err := storage.NewBigQueryReadClient(ctx, option.WithGRPCConn(conn), option.WithoutAuthentication())
		if err != nil {
			return nil, err
		}
		return c, nil
	}

	previous := readBackoff
	readBackoff.Initial, readBackoff.Max = 1, 2
	t.Cleanup(func() { readBackoff = previous })
	sf.read, sf.b = fr, b
	return sf
}

// table sets what the REST fake answers for table d.t.
func (f *scanFixture) table(status int, body any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status, f.body = status, body
}

// scanned is a drained scan: the messages as sent, and the rows they hold.
type scanned struct {
	res      *backend.QueryResult
	messages [][]byte
	ids      []int64
	names    []string
	rowsIn   []int64 // the rows of each message
}

// drainScan reads a scan to its end as a client would: each message is decoded
// against the header's schema.
func drainScan(t *testing.T, res *backend.QueryResult) *scanned {
	t.Helper()
	out := &scanned{res: res}
	defer func() { _ = res.Batches.Close() }()
	for {
		msg, err := res.Batches.Next()
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
		out.messages = append(out.messages, msg)
		rd, err := arrowipc.NewReader(res.ArrowSchema, &oneMessage{message: msg})
		require.NoError(t, err)
		n := int64(0)
		for rd.Next() {
			rec := rd.RecordBatch()
			n += rec.NumRows()
			for i := 0; i < int(rec.NumRows()); i++ {
				id, name := columnOf(rec, "id"), columnOf(rec, "name")
				if id != nil {
					out.ids = append(out.ids, id.(*array.Int64).Value(i))
				}
				if name != nil {
					out.names = append(out.names, name.(*array.String).Value(i))
				}
			}
		}
		require.NoError(t, rd.Err())
		rd.Release()
		out.rowsIn = append(out.rowsIn, n)
	}
	return out
}

func columnOf(rec arrow.RecordBatch, name string) arrow.Array {
	for i, f := range rec.Schema().Fields() {
		if f.Name == name {
			return rec.Column(i)
		}
	}
	return nil
}

func (f *scanFixture) scan(t *testing.T, opts backend.ScanOptions) (*backend.QueryResult, error) {
	t.Helper()
	return f.b.Scan(context.Background(), backend.TableRef{Dataset: "d", Table: "t"}, opts)
}
