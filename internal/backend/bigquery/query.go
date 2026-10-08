package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	bq "cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// cancelGrace bounds the best-effort cancellation of a job whose caller gave up.
const cancelGrace = 5 * time.Second

// Job ids on the wire are "<location>.<id>" when BigQuery reported a location: a
// job outside the US and EU multi-regions can only be found again with it, and a
// client holds the id, not the server's configuration. Neither part can contain a
// dot, so the split is unambiguous. An id with no dot is a BigQuery job id as it
// is, looked up in the configured location.
const jobIDSeparator = "."

func encodeJobID(location, id string) string {
	if location == "" || id == "" {
		return id
	}
	return location + jobIDSeparator + id
}

func (b *Backend) parseJobID(op, wire string) (id, location string, err error) {
	id, location = wire, b.cfg.Location
	if before, after, found := strings.Cut(wire, jobIDSeparator); found {
		location, id = before, after
	}
	if !jobID.MatchString(id) || (location != "" && !locationID.MatchString(location)) {
		return "", "", serr.New(serr.InvalidArgument, op, fmt.Sprintf("%q is not a job id", wire))
	}
	return id, location, nil
}

// limit is the effective limit of a request: the caller's own, never above the
// server's when it has one. Zero on both sides is no limit.
func limit[T ~int64 | ~int](requested, ceiling T) T {
	switch {
	case requested > 0 && ceiling > 0:
		return min(requested, ceiling)
	case requested > 0:
		return requested
	default:
		return ceiling
	}
}

// Query runs SQL and streams its result as Arrow batches. The job is complete
// before this returns, because the header carries the result's schema, which
// BigQuery knows only then; for the same reason CancelJob cannot reach a query
// that is still running, and a caller that gives up cancels the call instead,
// which also cancels the job.
//
// A dry run only validates and prices the statement; a DML or DDL statement
// returns a result with no schema and no batches.
func (b *Backend) Query(ctx context.Context, sql string, opts backend.QueryOptions) (*backend.QueryResult, error) {
	const op = "Query"
	if strings.TrimSpace(sql) == "" {
		return nil, serr.New(serr.InvalidArgument, op, "the SQL is empty")
	}
	params, err := bindParams(opts.Params)
	if err != nil {
		return nil, err
	}
	dataset := opts.DefaultDataset
	if dataset == "" {
		dataset = b.cfg.DefaultDataset
	}
	if dataset != "" {
		if err := checkDatasetID(op, dataset); err != nil {
			return nil, err
		}
	}

	timeout := limit(opts.Timeout, b.cfg.QueryTimeout)
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	// The context outlives this call when rows are still to be read, and is then
	// cancelled when the reader closes.
	handedOver := false
	defer func() {
		if !handedOver {
			cancel()
		}
	}()

	q := b.client.Query(sql)
	q.Parameters = params
	if dataset != "" {
		q.DefaultProjectID, q.DefaultDatasetID = b.project, dataset
	}
	q.DryRun = opts.DryRun
	q.MaxBytesBilled = limit(opts.MaxBytesBilled, b.cfg.MaxQueryBytes)
	if timeout > 0 {
		q.JobTimeout = timeout
	}

	job, err := q.Run(ctx)
	if err != nil {
		return nil, classify(op, err)
	}
	if opts.DryRun {
		return b.dryRun(job)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		cancelAbandoned(ctx, job)
		return nil, classify(op, err)
	}
	if err := status.Err(); err != nil {
		return nil, classify(op, err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		cancelAbandoned(ctx, job)
		return nil, classify(op, err)
	}

	result := &backend.QueryResult{Job: jobOut(job, status)}
	result.Job.Stats.RowsProduced = int64(it.TotalRows)
	if len(it.Schema) == 0 {
		return result, nil // DML or DDL: nothing to read.
	}
	result.Schema = columnsFromSchema(it.Schema)
	enc, err := newResultEncoder(result.Schema)
	if err != nil {
		return nil, err
	}
	result.ArrowSchema = enc.schemaMessage()
	result.Batches = &batchReader{ctx: ctx, it: it, enc: enc, job: job, cancel: cancel}
	handedOver = true
	return result, nil
}

// dryRun reports what BigQuery says a statement would cost and return.
func (b *Backend) dryRun(job *bq.Job) (*backend.QueryResult, error) {
	status := job.LastStatus()
	result := &backend.QueryResult{Job: jobOut(job, status)}
	result.Job.State = backend.JobDone
	if status != nil && status.Statistics != nil {
		if qs, ok := status.Statistics.Details.(*bq.QueryStatistics); ok && len(qs.Schema) > 0 {
			result.Schema = columnsFromSchema(qs.Schema)
			enc, err := newResultEncoder(result.Schema)
			if err != nil {
				return nil, err
			}
			defer enc.release()
			result.ArrowSchema = enc.schemaMessage()
		}
	}
	return result, nil
}

// cancelAbandoned asks BigQuery to stop a job whose caller stopped waiting, so a
// timeout or a hung-up client does not leave it running, and billing, behind.
func cancelAbandoned(ctx context.Context, job *bq.Job) {
	if ctx.Err() == nil {
		return
	}
	// The call's own context is done; the cancellation needs one that is not.
	fresh, stop := context.WithTimeout(context.WithoutCancel(ctx), cancelGrace)
	defer stop()
	_ = job.Cancel(fresh)
}

// batchReader turns the rows of a finished query into Arrow batches, a batch at
// a time, so a large result is never held in memory.
type batchReader struct {
	ctx    context.Context
	it     *bq.RowIterator
	enc    *resultEncoder
	job    *bq.Job
	cancel context.CancelFunc
	done   bool
}

func (r *batchReader) Next() ([]byte, error) {
	for !r.done && !r.enc.full() {
		var row []bq.Value
		err := r.it.Next(&row)
		if errors.Is(err, iterator.Done) {
			r.done = true
			break
		}
		if err != nil {
			cancelAbandoned(r.ctx, r.job)
			return nil, classify("Query", err)
		}
		if err := r.enc.add(row); err != nil {
			return nil, err
		}
	}
	if !r.enc.pending() {
		return nil, io.EOF
	}
	return r.enc.flush()
}

func (r *batchReader) Close() error {
	r.enc.release()
	r.cancel()
	return nil
}

// GetJob reports a job's state and accounting.
func (b *Backend) GetJob(ctx context.Context, id string) (*backend.Job, error) {
	const op = "GetJob"
	jobID, location, err := b.parseJobID(op, id)
	if err != nil {
		return nil, err
	}
	job, err := b.client.JobFromIDLocation(ctx, jobID, location)
	if err != nil {
		return nil, classify(op, err)
	}
	out := jobOut(job, job.LastStatus())
	return &out, nil
}

// CancelJob asks BigQuery to cancel a job. It returns once the request is
// accepted; the job reports JobCancelled when the cancellation has taken effect.
func (b *Backend) CancelJob(ctx context.Context, id string) error {
	const op = "CancelJob"
	jobID, location, err := b.parseJobID(op, id)
	if err != nil {
		return err
	}
	job, err := b.client.JobFromIDLocation(ctx, jobID, location)
	if err != nil {
		return classify(op, err)
	}
	return classify(op, job.Cancel(ctx))
}

// jobOut is the portable view of a job and its accounting. The failure of a job
// is reported in this server's words, as every other failure is.
func jobOut(job *bq.Job, status *bq.JobStatus) backend.Job {
	out := backend.Job{ID: encodeJobID(job.Location(), job.ID()), State: backend.JobDone}
	if status == nil {
		return out
	}
	switch status.State {
	case bq.Pending:
		out.State = backend.JobPending
	case bq.Running:
		out.State = backend.JobRunning
	}
	if err := status.Err(); err != nil {
		out.State = backend.JobFailed
		if f, ok := inspect(err); ok && containsReason(f, "stopped") {
			out.State = backend.JobCancelled
		}
		_, out.Error = normalize(err)
	}
	if stats := status.Statistics; stats != nil {
		out.Created, out.Started, out.Ended = stats.CreationTime, stats.StartTime, stats.EndTime
		out.Stats.BytesScanned = stats.TotalBytesProcessed
		out.Stats.SlotMillis = stats.TotalSlotDuration.Milliseconds()
		if qs, ok := stats.Details.(*bq.QueryStatistics); ok {
			out.Stats.BytesBilled = qs.TotalBytesBilled
			out.Stats.CacheHit = qs.CacheHit
			out.Stats.RowsAffected = qs.NumDMLAffectedRows
			if qs.SlotMillis > 0 {
				out.Stats.SlotMillis = qs.SlotMillis
			}
			if qs.TotalBytesProcessed > 0 {
				out.Stats.BytesScanned = qs.TotalBytesProcessed
			}
		}
	}
	return out
}

func containsReason(f failure, reason string) bool {
	for _, r := range f.reasons {
		if r == reason {
			return true
		}
	}
	return false
}
