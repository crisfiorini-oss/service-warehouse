package bigquery

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// sliceReader hands a fixed list of payloads to InsertRows.
type sliceReader struct {
	payloads [][]byte
	next     int
}

func (r *sliceReader) Next() ([]byte, error) {
	if r.next >= len(r.payloads) {
		return nil, io.EOF
	}
	p := r.payloads[r.next]
	r.next++
	return p, nil
}
func (r *sliceReader) Close() error { return nil }

// batchOf builds an Arrow record batch and its framing: the header's schema
// message and the batch message.
func batchOf(t *testing.T, schema *arrow.Schema, fill func(b *array.RecordBuilder)) (schemaMsg, batch []byte) {
	t.Helper()
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	fill(builder)
	rec := builder.NewRecordBatch()
	defer rec.Release()
	schemaMsg, err := arrowipc.SchemaMessage(schema)
	require.NoError(t, err)
	enc, err := arrowipc.NewEncoder(schema)
	require.NoError(t, err)
	defer enc.Close()
	batch, err = enc.Encode(rec)
	require.NoError(t, err)
	return schemaMsg, batch
}

var eventSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

func eventBatch(t *testing.T, ids []int64, names []string) (schemaMsg, batch []byte) {
	return batchOf(t, eventSchema, func(b *array.RecordBuilder) {
		for i, id := range ids {
			b.Field(0).(*array.Int64Builder).Append(id)
			b.Field(1).(*array.StringBuilder).Append(names[i])
		}
	})
}

// tableMetadata is what the fake answers a metadata read of the table with.
func tableMetadata(fields ...map[string]any) map[string]any {
	return map[string]any{
		"kind": "bigquery#table", "type": "TABLE",
		"tableReference": map[string]any{"projectId": "test-project", "datasetId": "d", "tableId": "t"},
		"schema":         map[string]any{"fields": fields},
	}
}

func field(name, typ, mode string) map[string]any {
	return map[string]any{"name": name, "type": typ, "mode": mode}
}

var eventTable = tableMetadata(field("id", "INTEGER", "REQUIRED"), field("name", "STRING", "NULLABLE"))

const insertPath = "/projects/test-project/datasets/d/tables/t/insertAll"

func insertRows(t *testing.T, b *Backend, header []byte, payloads ...[]byte) (*backend.InsertResult, error) {
	t.Helper()
	return b.InsertRows(context.Background(), backend.TableRef{Dataset: "d", Table: "t"}, header, &sliceReader{payloads: payloads})
}

func TestInsertRowsStoresRowsAndAsksBigQueryToSkipInvalidOnes(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{"kind": "bigquery#tableDataInsertAllResponse"})
	b := f.open(t, backend.Config{})

	header, batch := eventBatch(t, []int64{1, 2, 3}, []string{"a", "b", "c"})
	res, err := insertRows(t, b, header, batch)
	require.NoError(t, err)
	require.EqualValues(t, 3, res.RowsInserted)
	require.Empty(t, res.Errors)

	sent := f.seen("POST", "insertAll")
	require.Len(t, sent, 1)
	require.Equal(t, true, sent[0].Body["skipInvalidRows"])
	require.NotEqual(t, true, sent[0].Body["ignoreUnknownValues"], "an unknown value is a refusal, not something to drop")
	rows := sent[0].Body["rows"].([]any)
	require.Len(t, rows, 3)
	first := rows[0].(map[string]any)["json"].(map[string]any)
	require.Equal(t, map[string]any{"id": float64(1), "name": "a"}, first)
}

func TestInsertRowsReportsTheRowsBigQueryRefusedByIndexAndKeepsTheRest(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{
		"kind": "bigquery#tableDataInsertAllResponse",
		"insertErrors": []map[string]any{{
			"index": 1,
			"errors": []map[string]any{{
				"reason": "invalid", "location": "id",
				"message": "VENDOR TEXT must never reach a client: bad value", "debugInfo": "secret",
			}},
		}},
	})
	b := f.open(t, backend.Config{})

	header, batch := eventBatch(t, []int64{1, 2, 3}, []string{"a", "b", "c"})
	res, err := insertRows(t, b, header, batch)
	require.NoError(t, err)
	require.EqualValues(t, 2, res.RowsInserted)
	require.Len(t, res.Errors, 1)
	require.EqualValues(t, 1, res.Errors[0].RowIndex)
	require.Equal(t, backend.RefusalInvalidValue, res.Errors[0].Reason)
	require.Contains(t, res.Errors[0].Error, `"id"`)
	require.NotContains(t, res.Errors[0].Error, "VENDOR")
	require.NotContains(t, res.Errors[0].Error, "secret")
}

func TestInsertRowsFailsTheCallWhenARowFailedForAReasonThatSaysNothingAgainstIt(t *testing.T) {
	quiet(t)
	for reason, want := range map[string]serr.Code{
		"backendError":      serr.Internal,
		"timeout":           serr.DeadlineExceeded,
		"rateLimitExceeded": serr.Throttled,
		"quotaExceeded":     serr.Throttled,
		"stopped":           serr.Internal,
	} {
		t.Run(reason, func(t *testing.T) {
			f := newFake(t)
			f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
			f.handle("POST "+insertPath, 200, map[string]any{
				"insertErrors": []map[string]any{{"index": 0, "errors": []map[string]any{{"reason": reason, "message": "VENDOR"}}}},
			})
			b := f.open(t, backend.Config{})

			header, batch := eventBatch(t, []int64{1, 2}, []string{"a", "b"})
			res, err := insertRows(t, b, header, batch)
			require.Nil(t, res, "a failure that is not the row's is never reported as a refusal of it")
			require.True(t, serr.Is(err, want), "got %v", err)
			require.NotContains(t, err.Error(), "VENDOR")
		})
	}
}

func TestInsertRowsFailsTheCallWhenTheRequestFails(t *testing.T) {
	quiet(t)
	for name, tc := range map[string]struct {
		status int
		reason string
		want   serr.Code
	}{
		// The client library retries a backendError, a rateLimitExceeded and any
		// 5xx until the call's context ends, so those are classified in errors_test.go.
		"quota":     {403, "quotaExceeded", serr.Throttled},
		"denied":    {403, "accessDenied", serr.PermissionDenied},
		"too large": {400, "invalid", serr.InvalidArgument},
		"missing":   {404, "notFound", serr.NotFound},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
			f.handle("POST "+insertPath, tc.status, apiError(tc.status, tc.reason, "VENDOR MESSAGE"))
			b := f.open(t, backend.Config{})

			header, batch := eventBatch(t, []int64{1}, []string{"a"})
			_, err := insertRows(t, b, header, batch)
			require.True(t, serr.Is(err, tc.want), "got %v", err)
		})
	}
}

func TestInsertRowsRefusesEveryRowWhenTheBatchDisagreesWithTheTable(t *testing.T) {
	for name, table := range map[string]map[string]any{
		"a column the table lacks": tableMetadata(field("id", "INTEGER", "REQUIRED")),
		"a required column the batch lacks": tableMetadata(
			field("id", "INTEGER", "REQUIRED"), field("name", "STRING", "NULLABLE"), field("region", "STRING", "REQUIRED")),
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.handle("GET /projects/test-project/datasets/d/tables/t", 200, table)
			f.handle("POST "+insertPath, 200, map[string]any{})
			b := f.open(t, backend.Config{})

			header, batch := eventBatch(t, []int64{1, 2}, []string{"a", "b"})
			res, err := insertRows(t, b, header, batch)
			require.NoError(t, err)
			require.Zero(t, res.RowsInserted)
			require.Len(t, res.Errors, 2)
			for i, e := range res.Errors {
				require.EqualValues(t, i, e.RowIndex)
				require.Equal(t, backend.RefusalSchemaMismatch, e.Reason, "the table, not the row, is what disagrees")
			}
			require.Empty(t, f.seen("POST", "insertAll"), "nothing is sent for a batch the table cannot take")
		})
	}
}

func TestInsertRowsMatchesColumnNamesWithoutRegardToCase(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200,
		tableMetadata(field("ID", "INTEGER", "REQUIRED"), field("Name", "STRING", "NULLABLE")))
	f.handle("POST "+insertPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})

	header, batch := eventBatch(t, []int64{1}, []string{"a"})
	res, err := insertRows(t, b, header, batch)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.RowsInserted)
}

func TestInsertRowsSplitsRequestsAndKeepsRowIndexesAcrossThem(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})

	ids := make([]int64, maxRowsPerInsert+1)
	names := make([]string, len(ids))
	for i := range ids {
		ids[i], names[i] = int64(i), "n"
	}
	header, first := eventBatch(t, ids[:300], names[:300])
	_, second := eventBatch(t, ids[300:], names[300:])
	res, err := insertRows(t, b, header, first, second)
	require.NoError(t, err)
	require.EqualValues(t, maxRowsPerInsert+1, res.RowsInserted)

	sent := f.seen("POST", "insertAll")
	require.Len(t, sent, 2, "501 rows are two requests")
	require.Len(t, sent[0].Body["rows"], maxRowsPerInsert)
	require.Len(t, sent[1].Body["rows"], 1)
}

func TestInsertRowsIndexesRefusalsByTheirPlaceInTheWholeStream(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	calls := 0
	f.handleFunc("POST "+insertPath, func(w http.ResponseWriter, r *http.Request) {
		calls++
		resp := map[string]any{}
		if calls == 2 { // the request that holds the stream's second half
			resp["insertErrors"] = []map[string]any{{"index": 0, "errors": []map[string]any{{"reason": "invalid"}}}}
		}
		writeJSON(w, 200, resp)
	})
	b := f.open(t, backend.Config{})

	ids := make([]int64, maxRowsPerInsert+2)
	names := make([]string, len(ids))
	for i := range ids {
		ids[i], names[i] = int64(i), "n"
	}
	header, batch := eventBatch(t, ids, names)
	res, err := insertRows(t, b, header, batch)
	require.NoError(t, err)
	require.Len(t, res.Errors, 1)
	require.EqualValues(t, maxRowsPerInsert, res.Errors[0].RowIndex, "the first row of the second request is row 500 of the stream")
	require.EqualValues(t, maxRowsPerInsert+1, res.RowsInserted)
}

func TestInsertRowsRefusesARowTooLargeToSendAndStoresTheOthers(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})

	huge := strings.Repeat("x", maxRequestBytes)
	header, batch := eventBatch(t, []int64{1, 2, 3}, []string{"a", huge, "c"})
	res, err := insertRows(t, b, header, batch)
	require.NoError(t, err)
	require.EqualValues(t, 2, res.RowsInserted)
	require.Len(t, res.Errors, 1)
	require.EqualValues(t, 1, res.Errors[0].RowIndex)
	require.Equal(t, backend.RefusalRowTooLarge, res.Errors[0].Reason)

	var sentIDs []float64
	for _, req := range f.seen("POST", "insertAll") {
		for _, row := range req.Body["rows"].([]any) {
			sentIDs = append(sentIDs, row.(map[string]any)["json"].(map[string]any)["id"].(float64))
		}
	}
	require.Equal(t, []float64{1, 3}, sentIDs)
}

func TestInsertRowsSplitsARequestThatWouldPassTheByteLimit(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})

	big := strings.Repeat("x", maxRequestBytes/2)
	header, batch := eventBatch(t, []int64{1, 2, 3}, []string{big, big, big})
	res, err := insertRows(t, b, header, batch)
	require.NoError(t, err)
	require.EqualValues(t, 3, res.RowsInserted)
	require.Len(t, f.seen("POST", "insertAll"), 3, "two such rows already pass the limit with their envelope")
}

func TestInsertRowsRefusesAValueBigQueryCannotBeGiven(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})

	header, batch := eventBatch(t, []int64{1, 2}, []string{"fine", "bad \xff text"})
	res, err := insertRows(t, b, header, batch)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.RowsInserted)
	require.Len(t, res.Errors, 1)
	require.EqualValues(t, 1, res.Errors[0].RowIndex)
	require.Equal(t, backend.RefusalInvalidValue, res.Errors[0].Reason)
}

func TestInsertRowsReadsSelfDescribingBatchesWhenTheHeaderHasNoSchema(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})

	header, batch := eventBatch(t, []int64{1, 2}, []string{"a", "b"})
	res, err := insertRows(t, b, nil, append(append([]byte{}, header...), batch...))
	require.NoError(t, err)
	require.EqualValues(t, 2, res.RowsInserted)

	_, err = insertRows(t, b, nil, batch)
	require.True(t, serr.Is(err, serr.InvalidArgument), "a batch with no schema anywhere cannot be read: %v", err)
}

func TestInsertRowsAcceptsAHeaderSchemaThatEndsTheStream(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})

	header, batch := eventBatch(t, []int64{1}, []string{"a"})
	endOfStream := []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}
	res, err := insertRows(t, b, append(append([]byte{}, header...), endOfStream...), batch)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.RowsInserted)
}

func TestInsertRowsRefusesMalformedAndMismatchedArrow(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})
	header, batch := eventBatch(t, []int64{1}, []string{"a"})

	_, err := insertRows(t, b, []byte("not an arrow schema"), batch)
	require.True(t, serr.Is(err, serr.InvalidArgument), "got %v", err)

	_, err = insertRows(t, b, header, []byte{0xff, 0xff, 0xff, 0xff, 0x10, 0, 0, 0, 1, 2, 3})
	require.True(t, serr.Is(err, serr.InvalidArgument), "got %v", err)

	other := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	otherHeader, otherBatch := batchOf(t, other, func(rb *array.RecordBuilder) { rb.Field(0).(*array.Int64Builder).Append(1) })
	_, err = insertRows(t, b, nil, append(append([]byte{}, header...), batch...), append(append([]byte{}, otherHeader...), otherBatch...))
	require.True(t, serr.Is(err, serr.InvalidArgument), "batches that disagree on their schema: %v", err)
}

// A transport that fails mid-stream is the call's failure, not the client's
// malformed Arrow: it is returned as itself and never as an invalid argument.
func TestInsertRowsReturnsAFailedSourceAsItself(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})
	header, batch := eventBatch(t, []int64{1}, []string{"a"})

	lost := serr.New(serr.Throttled, "recv", "the client went away")
	_, err := b.InsertRows(context.Background(), backend.TableRef{Dataset: "d", Table: "t"}, header,
		&failingReader{payloads: [][]byte{batch}, err: lost})
	require.Same(t, lost, err)
}

// failingReader hands out its payloads and then fails instead of ending.
type failingReader struct {
	payloads [][]byte
	err      error
}

func (r *failingReader) Next() ([]byte, error) {
	if len(r.payloads) == 0 {
		return nil, r.err
	}
	p := r.payloads[0]
	r.payloads = r.payloads[1:]
	return p, nil
}
func (r *failingReader) Close() error { return nil }

// SWH_MAX_ARROW_MESSAGE_BYTES reaches the reader: a message whose body is over it
// is the client's invalid argument, and nothing of the call is sent to BigQuery.
func TestInsertRowsRefusesAMessageOverTheConfiguredBound(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{})
	header, batch := eventBatch(t, []int64{1, 2, 3}, []string{"a", "b", "c"})

	small := f.open(t, backend.Config{MaxArrowMessageBytes: 16})
	_, err := insertRows(t, small, header, batch)
	require.True(t, serr.Is(err, serr.InvalidArgument), "got %v", err)
	require.Empty(t, f.seen("POST", "insertAll"))

	roomy := f.open(t, backend.Config{MaxArrowMessageBytes: 1 << 20})
	res, err := insertRows(t, roomy, header, batch)
	require.NoError(t, err)
	require.EqualValues(t, 3, res.RowsInserted)
}

func TestInsertRowsIsUnsupportedForAnArrowTypeBigQueryHasNoEquivalentOf(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})

	mapType := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "attrs", Type: mapType}}, nil)
	header, batch := batchOf(t, schema, func(rb *array.RecordBuilder) {
		rb.Field(0).(*array.Int64Builder).Append(1)
		rb.Field(1).AppendNull()
	})
	_, err := insertRows(t, b, header, batch)
	require.True(t, serr.Is(err, serr.Unsupported), "got %v", err)
	require.Empty(t, f.seen("POST", "insertAll"))
}

func TestInsertRowsReportsAMissingTableAsNotFound(t *testing.T) {
	quiet(t)
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 404, apiError(404, "notFound", "Not found: Table test-project:d.t"))
	b := f.open(t, backend.Config{})

	header, batch := eventBatch(t, []int64{1}, []string{"a"})
	_, err := insertRows(t, b, header, batch)
	require.True(t, serr.Is(err, serr.NotFound), "got %v", err)
	require.NotContains(t, err.Error(), "test-project", "the server's project is not the client's to read")
}

func TestInsertRowsUsesTheDefaultDatasetAndRefusesBadNames(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/dflt/tables/t", 200, eventTable)
	f.handle("POST /projects/test-project/datasets/dflt/tables/t/insertAll", 200, map[string]any{})
	b := f.open(t, backend.Config{DefaultDataset: "dflt"})

	header, batch := eventBatch(t, []int64{1}, []string{"a"})
	res, err := b.InsertRows(context.Background(), backend.TableRef{Table: "t"}, header, &sliceReader{payloads: [][]byte{batch}})
	require.NoError(t, err)
	require.EqualValues(t, 1, res.RowsInserted)

	noDefault := newFake(t).open(t, backend.Config{})
	for _, ref := range []backend.TableRef{
		{Table: "t"}, // no dataset anywhere
		{Dataset: "d/tables/x", Table: "t"},
		{Dataset: "d", Table: "t/../x"},
		{Dataset: "d", Table: ""},
	} {
		_, err := noDefault.InsertRows(context.Background(), ref, header, &sliceReader{payloads: [][]byte{batch}})
		require.True(t, serr.Is(err, serr.InvalidArgument), "%+v: %v", ref, err)
	}
}

func TestInsertRowsRefusesABatchWithTwoColumnsOfOneName(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d/tables/t", 200, eventTable)
	f.handle("POST "+insertPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})

	twice := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "ID", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	header, batch := batchOf(t, twice, func(rb *array.RecordBuilder) {
		rb.Field(0).(*array.Int64Builder).Append(1)
		rb.Field(1).(*array.Int64Builder).Append(2)
	})
	_, err := insertRows(t, b, header, batch)
	require.True(t, serr.Is(err, serr.InvalidArgument), "got %v", err)
	require.Empty(t, f.seen("POST", "insertAll"), "a value is never dropped silently")
}
