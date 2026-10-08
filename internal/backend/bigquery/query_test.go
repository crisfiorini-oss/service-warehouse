package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

const (
	jobsPath    = "/projects/test-project/jobs"
	resultsPath = "/projects/test-project/queries/job_1"
	jobPath     = "/projects/test-project/jobs/job_1"
)

// jobResource is what BigQuery answers a job insert or a job read with.
func jobResource(state string, statistics, extra map[string]any) map[string]any {
	job := map[string]any{
		"kind":         "bigquery#job",
		"jobReference": map[string]any{"projectId": "test-project", "jobId": "job_1", "location": "EU"},
		"status":       map[string]any{"state": state},
		"statistics": map[string]any{
			"creationTime": "1700000000000", "startTime": "1700000001000", "endTime": "1700000002000",
		},
		"configuration": map[string]any{"query": map[string]any{"query": "SELECT 1", "useLegacySql": false}},
	}
	for k, v := range statistics {
		job["statistics"].(map[string]any)[k] = v
	}
	for k, v := range extra {
		job[k] = v
	}
	return job
}

func resultsResource(fields []map[string]any, rows ...[]any) map[string]any {
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		cells := make([]map[string]any, len(row))
		for j, v := range row {
			cells[j] = map[string]any{"v": v}
		}
		out[i] = map[string]any{"f": cells}
	}
	return map[string]any{
		"kind": "bigquery#getQueryResultsResponse", "jobComplete": true,
		"jobReference": map[string]any{"projectId": "test-project", "jobId": "job_1", "location": "EU"},
		"schema":       map[string]any{"fields": fields},
		"totalRows":    fmt.Sprint(len(rows)),
		"rows":         out,
	}
}

// queryFake serves one successful query job over the fake.
func queryFake(t *testing.T, results map[string]any, stats map[string]any) *fake {
	t.Helper()
	f := newFake(t)
	f.handle("POST "+jobsPath, 200, jobResource("DONE", stats, nil))
	f.handle("GET "+jobPath, 200, jobResource("DONE", stats, nil))
	f.handle("GET "+resultsPath, 200, results)
	return f
}

// decode reads a result's batches back with a plain Arrow reader, exactly as a
// client does: the header's schema message followed by each batch message.
func decode(t *testing.T, res *backend.QueryResult) (schema *arrow.Schema, records []arrow.RecordBatch) {
	t.Helper()
	require.NotEmpty(t, res.ArrowSchema)
	require.NotNil(t, res.Batches)
	for {
		batch, err := res.Batches.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		reader, err := arrowipc.NewReader(res.ArrowSchema, &sliceReader{payloads: [][]byte{batch}})
		require.NoError(t, err)
		schema = reader.Schema()
		for reader.Next() {
			rec := reader.RecordBatch()
			rec.Retain()
			records = append(records, rec)
		}
		require.NoError(t, reader.Err())
		reader.Release()
	}
	t.Cleanup(func() {
		for _, rec := range records {
			rec.Release()
		}
	})
	return schema, records
}

func TestQueryStreamsTheResultAsArrowAndBindsParameters(t *testing.T) {
	fields := []map[string]any{
		field("id", "INTEGER", "NULLABLE"),
		field("name", "STRING", "NULLABLE"),
		field("score", "FLOAT", "NULLABLE"),
	}
	f := queryFake(t,
		resultsResource(fields, []any{"1", "ada", "1.5"}, []any{"2", nil, "2.5"}),
		map[string]any{"totalBytesProcessed": "2048", "query": map[string]any{
			"totalBytesBilled": "10485760", "cacheHit": true, "totalBytesProcessed": "2048",
		}})
	b := f.open(t, backend.Config{Location: "EU", DefaultDataset: "dflt"})

	res, err := b.Query(context.Background(), "SELECT id, name, score FROM t WHERE id > @min AND name = @who", backend.QueryOptions{
		Params: []backend.QueryParam{
			{Name: "min", Type: backend.TypeInt64, Value: "0"},
			{Name: "@who", Type: backend.TypeString, IsNull: true},
		},
		MaxBytesBilled: 5000,
		Timeout:        90 * time.Second,
	})
	require.NoError(t, err)
	defer res.Batches.Close()

	require.Equal(t, "EU.job_1", res.Job.ID, "the job id carries the location it ran in")
	require.Equal(t, backend.JobDone, res.Job.State)
	require.EqualValues(t, 2048, res.Job.Stats.BytesScanned)
	require.EqualValues(t, 10485760, res.Job.Stats.BytesBilled)
	require.True(t, res.Job.Stats.CacheHit)
	require.EqualValues(t, 2, res.Job.Stats.RowsProduced)

	require.Equal(t, []backend.Column{
		{Name: "id", Type: backend.TypeInt64, Nullable: true, NativeType: "INT64"},
		{Name: "name", Type: backend.TypeString, Nullable: true, NativeType: "STRING"},
		{Name: "score", Type: backend.TypeFloat64, Nullable: true, NativeType: "FLOAT64"},
	}, res.Schema)

	schema, records := decode(t, res)
	require.Equal(t, "id", schema.Field(0).Name)
	require.Len(t, records, 1)
	require.EqualValues(t, 2, records[0].NumRows())
	require.Equal(t, int64(2), records[0].Column(0).(*array.Int64).Value(1))
	require.True(t, records[0].Column(1).IsNull(1))
	require.Equal(t, 1.5, records[0].Column(2).(*array.Float64).Value(0))

	insert := f.seen("POST", "/jobs")
	require.Len(t, insert, 1)
	query := insert[0].Body["configuration"].(map[string]any)["query"].(map[string]any)
	require.Equal(t, false, query["useLegacySql"])
	require.Equal(t, "5000", query["maximumBytesBilled"])
	require.Equal(t, "90000", insert[0].Body["configuration"].(map[string]any)["jobTimeoutMs"])
	require.Equal(t, map[string]any{"projectId": "test-project", "datasetId": "dflt"}, query["defaultDataset"])
	require.Equal(t, []any{
		map[string]any{
			"name": "min", "parameterType": map[string]any{"type": "INT64"},
			"parameterValue": map[string]any{"value": "0"},
		},
		map[string]any{
			"name": "who", "parameterType": map[string]any{"type": "STRING"},
			"parameterValue": map[string]any{"value": nil},
		},
	}, query["queryParameters"], "a null is a typed parameter whose value is null, not the empty string")
	require.Equal(t, "EU", insert[0].Body["jobReference"].(map[string]any)["location"], "the job is placed in the configured location")
}

func TestQuerySplitsALargeResultIntoBatchesThatEachDecode(t *testing.T) {
	rows := make([][]any, 2*batchMaxRows+5)
	for i := range rows {
		rows[i] = []any{fmt.Sprint(i)}
	}
	f := queryFake(t, resultsResource([]map[string]any{field("id", "INTEGER", "NULLABLE")}, rows...), nil)
	b := f.open(t, backend.Config{})

	res, err := b.Query(context.Background(), "SELECT id FROM t", backend.QueryOptions{})
	require.NoError(t, err)
	defer res.Batches.Close()
	_, records := decode(t, res)
	require.Len(t, records, 3)
	total := int64(0)
	for _, rec := range records {
		total += rec.NumRows()
	}
	require.EqualValues(t, len(rows), total)
	require.EqualValues(t, 2*batchMaxRows, records[2].Column(0).(*array.Int64).Value(0))
	_, err = res.Batches.Next()
	require.ErrorIs(t, err, io.EOF, "a finished result keeps answering end of stream")
}

func TestQueryOfAnEmptyResultHasASchemaAndNoBatches(t *testing.T) {
	f := queryFake(t, resultsResource([]map[string]any{field("id", "INTEGER", "NULLABLE")}), nil)
	b := f.open(t, backend.Config{})

	res, err := b.Query(context.Background(), "SELECT id FROM t WHERE FALSE", backend.QueryOptions{})
	require.NoError(t, err)
	defer res.Batches.Close()
	require.Len(t, res.Schema, 1)
	require.NotEmpty(t, res.ArrowSchema)
	_, err = res.Batches.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestQueryDryRunPricesTheStatementWithoutReadingAnything(t *testing.T) {
	f := newFake(t)
	f.handle("POST "+jobsPath, 200, jobResource("DONE", map[string]any{
		"totalBytesProcessed": "4096",
		"query": map[string]any{
			"totalBytesProcessed": "4096",
			"schema":              map[string]any{"fields": []map[string]any{field("n", "INTEGER", "NULLABLE")}},
		},
	}, nil))
	b := f.open(t, backend.Config{})

	res, err := b.Query(context.Background(), "SELECT COUNT(*) AS n FROM t", backend.QueryOptions{DryRun: true})
	require.NoError(t, err)
	require.Nil(t, res.Batches, "a dry run has a header and no batches")
	require.EqualValues(t, 4096, res.Job.Stats.BytesScanned)
	require.Equal(t, backend.JobDone, res.Job.State)
	require.Len(t, res.Schema, 1)
	require.NotEmpty(t, res.ArrowSchema)

	insert := f.seen("POST", "/jobs")
	require.Len(t, insert, 1)
	require.Equal(t, true, insert[0].Body["configuration"].(map[string]any)["dryRun"])
	require.Empty(t, f.seen("GET", "/queries/"), "nothing is fetched for a dry run")
}

func TestQueryOfADMLStatementReturnsRowsAffectedAndNoResult(t *testing.T) {
	f := newFake(t)
	stats := map[string]any{"query": map[string]any{"statementType": "UPDATE", "numDmlAffectedRows": "7"}}
	f.handle("POST "+jobsPath, 200, jobResource("DONE", stats, nil))
	f.handle("GET "+jobPath, 200, jobResource("DONE", stats, nil))
	f.handle("GET "+resultsPath, 200, resultsResource(nil))
	b := f.open(t, backend.Config{})

	res, err := b.Query(context.Background(), "UPDATE t SET x = 1 WHERE TRUE", backend.QueryOptions{})
	require.NoError(t, err)
	require.Nil(t, res.Batches)
	require.Empty(t, res.Schema)
	require.Empty(t, res.ArrowSchema)
	require.EqualValues(t, 7, res.Job.Stats.RowsAffected)
}

func TestQueryOfAColumnWithNoPortableTypeIsUnsupportedNotALookalike(t *testing.T) {
	rangeField := map[string]any{"name": "span", "type": "RANGE", "mode": "NULLABLE", "rangeElementType": map[string]any{"type": "DATE"}}
	f := queryFake(t, resultsResource([]map[string]any{rangeField}), nil)
	b := f.open(t, backend.Config{})

	res, err := b.Query(context.Background(), "SELECT span FROM t", backend.QueryOptions{})
	require.Nil(t, res)
	require.True(t, serr.Is(err, serr.Unsupported), "got %v", err)
	require.Contains(t, err.Error(), "span")
}

func TestQueryReportsEveryFailureInNormalizedTerms(t *testing.T) {
	logged := quiet(t)
	for name, tc := range map[string]struct {
		status  int
		reason  string
		message string
		want    serr.Code
	}{
		"invalid sql":   {400, "invalidQuery", "Syntax error: Unexpected identifier \"foo\" at [1:8]", serr.InvalidArgument},
		"no such table": {404, "notFound", "Not found: Table test-project:d.nope", serr.NotFound},
		"denied":        {403, "accessDenied", "Access Denied: User svc@test-project.iam.gserviceaccount.com does not have permission", serr.PermissionDenied},
		"over the cap":  {400, "bytesBilledLimitExceeded", "Query exceeded limit for bytes billed: 1000. 10485760 or higher required.", serr.Throttled},
		"quota":         {403, "quotaExceeded", "Quota exceeded: Your project exceeded quota", serr.Throttled},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.handle("POST "+jobsPath, tc.status, apiError(tc.status, tc.reason, tc.message))
			b := f.open(t, backend.Config{})

			_, err := b.Query(context.Background(), "SELECT 1", backend.QueryOptions{})
			require.True(t, serr.Is(err, tc.want), "got %v", err)
			require.NotContains(t, err.Error(), tc.reason, "a reason token is the vendor's")
			require.NotContains(t, err.Error(), "SOME_VENDOR_STATUS")
			if tc.want == serr.InvalidArgument {
				require.Contains(t, err.Error(), "Syntax error", "the diagnosis of the caller's own SQL is kept")
			} else {
				require.NotContains(t, err.Error(), tc.message, "the vendor's text is the operator's, not the client's")
				require.NotContains(t, err.Error(), "svc@")
			}
		})
	}
	require.NotEmpty(t, *logged, "what is not a client's to read is logged for the operator")
}

func TestQueryReportsAJobThatFailsWhileWaiting(t *testing.T) {
	quiet(t)
	f := newFake(t)
	f.handle("POST "+jobsPath, 200, jobResource("RUNNING", nil, nil))
	f.handle("GET "+resultsPath, 400, apiError(400, "bytesBilledLimitExceeded", "Query exceeded limit for bytes billed"))
	b := f.open(t, backend.Config{})

	_, err := b.Query(context.Background(), "SELECT * FROM big", backend.QueryOptions{})
	require.True(t, serr.Is(err, serr.Throttled), "got %v", err)
}

func TestQueryHonorsTheTighterOfTheCallersAndTheServersLimits(t *testing.T) {
	for name, tc := range map[string]struct {
		serverBytes, askBytes int64
		serverTime, askTime   time.Duration
		wantBytes, wantTimeMS string
	}{
		"the caller asks for more than the server allows": {1000, 5000, time.Minute, time.Hour, "1000", "60000"},
		"the caller asks for less":                        {1000, 500, time.Minute, time.Second, "500", "1000"},
		"the caller asks for nothing":                     {1000, 0, time.Minute, 0, "1000", "60000"},
		"the server has no limit":                         {0, 700, 0, 2 * time.Second, "700", "2000"},
		"neither has a limit":                             {0, 0, 0, 0, "", ""},
	} {
		t.Run(name, func(t *testing.T) {
			f := queryFake(t, resultsResource([]map[string]any{field("id", "INTEGER", "NULLABLE")}, []any{"1"}), nil)
			b := f.open(t, backend.Config{MaxQueryBytes: tc.serverBytes, QueryTimeout: tc.serverTime})
			res, err := b.Query(context.Background(), "SELECT 1", backend.QueryOptions{MaxBytesBilled: tc.askBytes, Timeout: tc.askTime})
			require.NoError(t, err)
			_ = res.Batches.Close()

			config := f.seen("POST", "/jobs")[0].Body["configuration"].(map[string]any)
			query := config["query"].(map[string]any)
			if tc.wantBytes == "" {
				require.NotContains(t, query, "maximumBytesBilled")
				require.NotContains(t, config, "jobTimeoutMs")
				return
			}
			require.Equal(t, tc.wantBytes, query["maximumBytesBilled"])
			require.Equal(t, tc.wantTimeMS, config["jobTimeoutMs"])
		})
	}
}

func TestQueryThatOutlivesItsTimeLimitIsCancelledAndReportedAsDeadlineExceeded(t *testing.T) {
	quiet(t)
	f := newFake(t)
	f.handle("POST "+jobsPath, 200, jobResource("RUNNING", nil, nil))
	f.handleFunc("GET "+resultsPath, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"kind": "bigquery#getQueryResultsResponse", "jobComplete": false})
	})
	f.handle("POST "+jobPath+"/cancel", 200, map[string]any{"kind": "bigquery#jobCancelResponse", "job": jobResource("RUNNING", nil, nil)})
	b := f.open(t, backend.Config{})

	_, err := b.Query(context.Background(), "SELECT slow()", backend.QueryOptions{Timeout: 300 * time.Millisecond})
	require.True(t, serr.Is(err, serr.DeadlineExceeded), "got %v", err)
	require.Len(t, f.seen("POST", "/cancel"), 1, "the abandoned job is cancelled so it stops billing")
}

func TestQueryRefusesWhatIsNotAQueryBeforeAskingBigQuery(t *testing.T) {
	f := newFake(t)
	b := f.open(t, backend.Config{})
	for name, tc := range map[string]struct {
		sql  string
		opts backend.QueryOptions
		want serr.Code
	}{
		"empty sql":         {"  ", backend.QueryOptions{}, serr.InvalidArgument},
		"bad parameter":     {"SELECT 1", backend.QueryOptions{Params: []backend.QueryParam{{Name: "n", Type: backend.TypeInt64, Value: "x"}}}, serr.InvalidArgument},
		"array parameter":   {"SELECT 1", backend.QueryOptions{Params: []backend.QueryParam{{Name: "n", Type: backend.TypeArray, Value: "[1]"}}}, serr.Unsupported},
		"bad default":       {"SELECT 1", backend.QueryOptions{DefaultDataset: "a/b"}, serr.InvalidArgument},
		"untyped parameter": {"SELECT 1", backend.QueryOptions{Params: []backend.QueryParam{{Name: "n", Value: "1"}}}, serr.InvalidArgument},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := b.Query(context.Background(), tc.sql, tc.opts)
			require.True(t, serr.Is(err, tc.want), "got %v", err)
		})
	}
	require.Empty(t, f.requests, "nothing was sent to BigQuery")
}

func TestGetJobReportsStateAndAccounting(t *testing.T) {
	quiet(t)
	for name, tc := range map[string]struct {
		state string
		extra map[string]any
		want  backend.JobState
		fail  bool
	}{
		"running": {"RUNNING", nil, backend.JobRunning, false},
		"pending": {"PENDING", nil, backend.JobPending, false},
		"done":    {"DONE", nil, backend.JobDone, false},
		"failed": {"DONE", map[string]any{"status": map[string]any{"state": "DONE", "errorResult": map[string]any{
			"reason": "invalidQuery", "message": "Syntax error: bad sql"}}}, backend.JobFailed, true},
		"cancelled": {"DONE", map[string]any{"status": map[string]any{"state": "DONE", "errorResult": map[string]any{
			"reason": "stopped", "message": "Job execution was cancelled: User requested cancellation"}}}, backend.JobCancelled, true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.handle("GET "+jobPath, 200, jobResource(tc.state, map[string]any{
				"totalBytesProcessed": "100",
				"totalSlotMs":         "250",
				"query":               map[string]any{"totalBytesBilled": "10485760", "totalBytesProcessed": "100", "numDmlAffectedRows": "3"},
			}, tc.extra))
			b := f.open(t, backend.Config{})

			job, err := b.GetJob(context.Background(), "EU.job_1")
			require.NoError(t, err)
			require.Equal(t, "EU.job_1", job.ID)
			require.Equal(t, tc.want, job.State)
			require.EqualValues(t, 100, job.Stats.BytesScanned)
			require.EqualValues(t, 10485760, job.Stats.BytesBilled)
			require.EqualValues(t, 3, job.Stats.RowsAffected)
			require.Equal(t, time.UnixMilli(1700000000000).UTC(), job.Created.UTC())
			if tc.fail {
				require.NotEmpty(t, job.Error)
				require.NotContains(t, job.Error, "invalidQuery")
				require.NotContains(t, job.Error, "stopped")
			} else {
				require.Empty(t, job.Error)
			}
			require.Equal(t, "EU", f.seen("GET", "/jobs/job_1")[0].Query["location"][0], "the location in the id is the one asked for")
		})
	}
}

func TestJobIDsAreCheckedAndFallBackToTheConfiguredLocation(t *testing.T) {
	quiet(t)
	f := newFake(t)
	f.handle("GET "+jobPath, 200, jobResource("DONE", nil, nil))
	f.handle("POST "+jobPath+"/cancel", 200, map[string]any{"job": jobResource("DONE", nil, nil)})
	f.handle("GET /projects/test-project/jobs/missing", 404, apiError(404, "notFound", "Not found: Job"))
	b := f.open(t, backend.Config{Location: "asia-northeast1"})

	_, err := b.GetJob(context.Background(), "job_1")
	require.NoError(t, err)
	require.Equal(t, "asia-northeast1", f.seen("GET", "/jobs/job_1")[0].Query["location"][0])

	require.NoError(t, b.CancelJob(context.Background(), "EU.job_1"))
	require.Equal(t, "EU", f.seen("POST", "/cancel")[0].Query["location"][0])

	for _, bad := range []string{"", "a/b", "EU.a/b", "../x", "EU.job_1.extra", "e u.job"} {
		_, err := b.GetJob(context.Background(), bad)
		require.True(t, serr.Is(err, serr.InvalidArgument), "%q: %v", bad, err)
		require.True(t, serr.Is(b.CancelJob(context.Background(), bad), serr.InvalidArgument), "%q", bad)
	}
	_, err = b.GetJob(context.Background(), "missing")
	require.True(t, serr.Is(err, serr.NotFound), "got %v", err)
}

func TestQueryIdentifiesAJobWithoutALocationByItsBareID(t *testing.T) {
	require.Equal(t, "job_1", encodeJobID("", "job_1"))
	require.Equal(t, "EU.job_1", encodeJobID("EU", "job_1"))
	require.Equal(t, "", encodeJobID("EU", ""), "a dry run has no job to name")
}
