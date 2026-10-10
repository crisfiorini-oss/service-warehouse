package server

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	whv0 "github.com/codefly-dev/service-warehouse/gen/codefly/warehouse/v0"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/backend/mem"
)

func scanStream(t *testing.T, c whv0.WarehouseClient, req *whv0.ScanRequest) (*whv0.QueryHeader, [][]any, error) {
	t.Helper()
	stream, err := c.Scan(context.Background(), req)
	require.NoError(t, err)
	header, recs, err := collect(t, stream)
	if err != nil {
		return header, nil, err
	}
	var rows [][]any
	for _, rec := range recs {
		for r := 0; r < int(rec.NumRows()); r++ {
			row := make([]any, rec.NumCols())
			for i := range row {
				if !rec.Column(i).IsNull(r) {
					row[i] = rec.Column(i).GetOneForMarshal(r)
				}
			}
			rows = append(rows, row)
		}
	}
	return header, rows, nil
}

func pred(field string, op whv0.PredicateOp, typ whv0.ColumnType, values ...string) *whv0.Predicate {
	return &whv0.Predicate{Field: field, Op: op, Type: typ, Values: values}
}

func filterOf(ps ...*whv0.Predicate) *whv0.Filter { return &whv0.Filter{Predicates: ps} }

// A client reads a filtered scan with nothing but the generated stubs and an
// Arrow reader.
func TestScanRoundTripsOverGRPC(t *testing.T) {
	ctx := context.Background()
	c := dialDuckDB(t)

	_, err := c.CreateTable(ctx, &whv0.CreateTableRequest{
		Ref: &whv0.TableRef{Dataset: "main", Table: "audit"},
		Columns: []*whv0.Column{
			{Name: "id", Type: whv0.ColumnType_COLUMN_TYPE_INT64},
			{Name: "actor", Type: whv0.ColumnType_COLUMN_TYPE_STRING, Nullable: true},
			{Name: "at", Type: whv0.ColumnType_COLUMN_TYPE_TIMESTAMPTZ, Nullable: true},
		},
	})
	require.NoError(t, err)
	_, _, err = query(t, c, &whv0.QueryRequest{Sql: `INSERT INTO audit VALUES
		(1, 'ada',   TIMESTAMPTZ '2024-01-01 00:00:00+00'),
		(2, 'alan',  TIMESTAMPTZ '2024-02-01 00:00:00+00'),
		(3, 'grace', TIMESTAMPTZ '2024-03-01 00:00:00+00'),
		(4, NULL,    TIMESTAMPTZ '2024-04-01 00:00:00+00'),
		(5, 'it''s "q" \ ; --', NULL)`})
	require.NoError(t, err)

	table := &whv0.TableRef{Dataset: "main", Table: "audit"}
	const (
		tInt  = whv0.ColumnType_COLUMN_TYPE_INT64
		tStr  = whv0.ColumnType_COLUMN_TYPE_STRING
		tTime = whv0.ColumnType_COLUMN_TYPE_TIMESTAMPTZ
	)

	t.Run("a conjunction, a selection and the header", func(t *testing.T) {
		header, rows, err := scanStream(t, c, &whv0.ScanRequest{
			Table:          table,
			SelectedFields: []string{"actor", "id"},
			Filter: filterOf(
				pred("id", whv0.PredicateOp_PREDICATE_OP_GE, tInt, "2"),
				pred("at", whv0.PredicateOp_PREDICATE_OP_LT, tTime, "2024-04-01T00:00:00Z"),
				pred("actor", whv0.PredicateOp_PREDICATE_OP_IS_NOT_NULL, whv0.ColumnType_COLUMN_TYPE_UNSPECIFIED),
			),
		})
		require.NoError(t, err)
		require.Equal(t, [][]any{{"alan", int64(2)}, {"grace", int64(3)}}, rows)
		require.Equal(t, "actor", header.GetSchema()[0].GetName())
		require.Equal(t, "id", header.GetSchema()[1].GetName())
		require.Empty(t, header.GetJob().GetId(), "a scan has no job to look up")
		require.NotEmpty(t, header.GetArrowIpcSchema())
	})

	t.Run("IN and IS NULL", func(t *testing.T) {
		_, rows, err := scanStream(t, c, &whv0.ScanRequest{
			Table: table, SelectedFields: []string{"id"},
			Filter: filterOf(pred("id", whv0.PredicateOp_PREDICATE_OP_IN, tInt, "1", "4", "9")),
		})
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(1)}, {int64(4)}}, rows)

		_, rows, err = scanStream(t, c, &whv0.ScanRequest{
			Table: table, SelectedFields: []string{"id"},
			Filter: filterOf(pred("actor", whv0.PredicateOp_PREDICATE_OP_IS_NULL, whv0.ColumnType_COLUMN_TYPE_UNSPECIFIED)),
		})
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(4)}}, rows)
	})

	t.Run("no filter reads every row", func(t *testing.T) {
		_, rows, err := scanStream(t, c, &whv0.ScanRequest{Table: table})
		require.NoError(t, err)
		require.Len(t, rows, 5)
	})

	t.Run("a value with quotes and backslashes is only a value", func(t *testing.T) {
		_, rows, err := scanStream(t, c, &whv0.ScanRequest{
			Table: table, SelectedFields: []string{"id"},
			Filter: filterOf(pred("actor", whv0.PredicateOp_PREDICATE_OP_EQ, tStr, "it's \"q\" \\ ; --")),
		})
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(5)}}, rows)
	})
}

func TestScanStatusCodes(t *testing.T) {
	c := dialDuckDB(t)
	_, err := c.CreateTable(context.Background(), &whv0.CreateTableRequest{
		Ref:     &whv0.TableRef{Dataset: "main", Table: "t"},
		Columns: []*whv0.Column{{Name: "id", Type: whv0.ColumnType_COLUMN_TYPE_INT64}},
	})
	require.NoError(t, err)
	table := &whv0.TableRef{Dataset: "main", Table: "t"}
	const tInt = whv0.ColumnType_COLUMN_TYPE_INT64

	for name, tc := range map[string]struct {
		req  *whv0.ScanRequest
		want codes.Code
	}{
		"a column the table lacks":  {&whv0.ScanRequest{Table: table, Filter: filterOf(pred("nope", whv0.PredicateOp_PREDICATE_OP_EQ, tInt, "1"))}, codes.InvalidArgument},
		"a selection it lacks":      {&whv0.ScanRequest{Table: table, SelectedFields: []string{"nope"}}, codes.InvalidArgument},
		"no operator":               {&whv0.ScanRequest{Table: table, Filter: filterOf(pred("id", whv0.PredicateOp_PREDICATE_OP_UNSPECIFIED, tInt, "1"))}, codes.InvalidArgument},
		"an operator nobody knows":  {&whv0.ScanRequest{Table: table, Filter: filterOf(pred("id", whv0.PredicateOp(99), tInt, "1"))}, codes.InvalidArgument},
		"EQ with two values":        {&whv0.ScanRequest{Table: table, Filter: filterOf(pred("id", whv0.PredicateOp_PREDICATE_OP_EQ, tInt, "1", "2"))}, codes.InvalidArgument},
		"values with no type":       {&whv0.ScanRequest{Table: table, Filter: filterOf(pred("id", whv0.PredicateOp_PREDICATE_OP_EQ, whv0.ColumnType_COLUMN_TYPE_UNSPECIFIED, "1"))}, codes.InvalidArgument},
		"a type on IS NULL":         {&whv0.ScanRequest{Table: table, Filter: filterOf(pred("id", whv0.PredicateOp_PREDICATE_OP_IS_NULL, tInt))}, codes.InvalidArgument},
		"an ARRAY value":            {&whv0.ScanRequest{Table: table, Filter: filterOf(pred("id", whv0.PredicateOp_PREDICATE_OP_EQ, whv0.ColumnType_COLUMN_TYPE_ARRAY, "1"))}, codes.Unimplemented},
		"a table that is not there": {&whv0.ScanRequest{Table: &whv0.TableRef{Dataset: "main", Table: "absent"}}, codes.NotFound},
		"no table named":            {&whv0.ScanRequest{}, codes.InvalidArgument},
	} {
		t.Run(name, func(t *testing.T) {
			stream, err := c.Scan(context.Background(), tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			require.Equal(t, tc.want, status.Code(err), "%v", err)
		})
	}
}

func TestScanIsUnimplementedWhereThereAreNoRows(t *testing.T) {
	c := dial(t) // mem
	stream, err := c.Scan(context.Background(), &whv0.ScanRequest{Table: &whv0.TableRef{Dataset: "d", Table: "t"}})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

// estimatingBackend scans with an estimate, as a backend that can price a read
// before it makes it does.
type estimatingBackend struct {
	*mem.Backend
	got backend.ScanOptions
}

type listReader struct{ msgs [][]byte }

func (l *listReader) Next() ([]byte, error) {
	if len(l.msgs) == 0 {
		return nil, io.EOF
	}
	m := l.msgs[0]
	l.msgs = l.msgs[1:]
	return m, nil
}
func (l *listReader) Close() error { return nil }

func (e *estimatingBackend) Scan(_ context.Context, _ backend.TableRef, opts backend.ScanOptions) (*backend.QueryResult, error) {
	e.got = opts
	return &backend.QueryResult{
		Job:         backend.Job{State: backend.JobRunning, Stats: backend.JobStats{BytesScanned: 424242}},
		Schema:      []backend.Column{{Name: "id", Type: backend.TypeInt64}},
		ArrowSchema: []byte("schema"),
		Batches:     &listReader{msgs: [][]byte{[]byte("one"), []byte("two")}},
	}, nil
}

// The estimate reaches the client in the header, ahead of every batch, and the
// request reaches the backend as the structured filter it was.
func TestScanHeaderCarriesTheEstimateBeforeAnyBatch(t *testing.T) {
	be := &estimatingBackend{Backend: mem.New(backend.Config{})}
	c := dialBackend(t, be)
	stream, err := c.Scan(context.Background(), &whv0.ScanRequest{
		Table:          &whv0.TableRef{Dataset: "d", Table: "t"},
		SelectedFields: []string{"id"},
		Filter:         filterOf(pred("id", whv0.PredicateOp_PREDICATE_OP_GT, whv0.ColumnType_COLUMN_TYPE_INT64, "5")),
	})
	require.NoError(t, err)

	first, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, first.GetHeader(), "the header comes first")
	require.EqualValues(t, 424242, first.GetHeader().GetJob().GetStats().GetBytesScanned())
	require.Equal(t, whv0.JobState_JOB_STATE_RUNNING, first.GetHeader().GetJob().GetState())

	var batches []string
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		batches = append(batches, string(msg.GetArrowBatch()))
	}
	require.Equal(t, []string{"one", "two"}, batches)

	require.Equal(t, []string{"id"}, be.got.Selected)
	require.Equal(t, []backend.Predicate{{Field: "id", Op: backend.OpGt, Type: backend.TypeInt64, Values: []string{"5"}}}, be.got.Filter)
}
