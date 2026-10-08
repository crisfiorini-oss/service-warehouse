package server

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	whv0 "github.com/codefly-dev/service-warehouse/gen/codefly/warehouse/v0"
	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/backend/duckdb"
)

// dialDuckDB serves the Warehouse API over the real embedded engine, so these
// tests exercise what a client of the running server sees: the wire types, the
// Arrow framing and the status codes.
func dialDuckDB(t *testing.T) whv0.WarehouseClient {
	t.Helper()
	be, err := duckdb.Open(context.Background(), backend.Config{Kind: "duckdb"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	whv0.RegisterWarehouseServer(srv, New(be))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return whv0.NewWarehouseClient(conn)
}

// streamedBatches adapts the batches of a Query stream to arrowipc.Source.
type streamedBatches struct{ batches [][]byte }

func (s *streamedBatches) Next() ([]byte, error) {
	if len(s.batches) == 0 {
		return nil, io.EOF
	}
	b := s.batches[0]
	s.batches = s.batches[1:]
	return b, nil
}

// query runs a Query and returns its header and decoded records.
func query(t *testing.T, c whv0.WarehouseClient, req *whv0.QueryRequest) (*whv0.QueryHeader, []arrow.RecordBatch, error) {
	t.Helper()
	stream, err := c.Query(context.Background(), req)
	require.NoError(t, err)
	first, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}
	header := first.GetHeader()
	require.NotNil(t, header, "the first message is the header")
	var batches [][]byte
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return header, nil, err
		}
		batches = append(batches, msg.GetArrowBatch())
	}
	if len(header.GetArrowIpcSchema()) == 0 {
		require.Empty(t, batches)
		return header, nil, nil
	}
	rd, err := arrowipc.NewReader(header.GetArrowIpcSchema(), &streamedBatches{batches})
	require.NoError(t, err)
	t.Cleanup(rd.Release)
	var recs []arrow.RecordBatch
	for rd.Next() {
		rec := rd.RecordBatch()
		rec.Retain()
		t.Cleanup(rec.Release)
		recs = append(recs, rec)
	}
	require.NoError(t, rd.Err())
	return header, recs, nil
}

func TestDuckDBCapabilitiesOverTheWire(t *testing.T) {
	c := dialDuckDB(t)
	caps, err := c.Capabilities(context.Background(), &whv0.CapabilitiesRequest{})
	require.NoError(t, err)
	require.Equal(t, "duckdb", caps.GetBackend())
	require.True(t, caps.GetDdl())
	require.True(t, caps.GetDryRun())
	require.True(t, caps.GetParameters())
	require.True(t, caps.GetStreamingInsert())
	require.True(t, caps.GetArrowResults())
	require.False(t, caps.GetAsyncJobs())
	require.False(t, caps.GetLoad())
	require.False(t, caps.GetUnload())
	require.Empty(t, caps.GetNativeVerbs())
}

func TestDuckDBStatusCodes(t *testing.T) {
	ctx := context.Background()
	c := dialDuckDB(t)

	_, err := c.CreateDataset(ctx, &whv0.CreateDatasetRequest{Name: "analytics"})
	require.NoError(t, err)
	_, err = c.CreateDataset(ctx, &whv0.CreateDatasetRequest{Name: "analytics"})
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	_, err = c.GetTable(ctx, &whv0.GetTableRequest{Ref: &whv0.TableRef{Dataset: "analytics", Table: "absent"}})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = c.DropDataset(ctx, &whv0.DropDatasetRequest{Name: "absent"})
	require.Equal(t, codes.NotFound, status.Code(err))

	_, err = c.CreateTable(ctx, &whv0.CreateTableRequest{
		Ref:     &whv0.TableRef{Dataset: "analytics", Table: "events"},
		Columns: []*whv0.Column{{Name: "id", Type: whv0.ColumnType_COLUMN_TYPE_INT64}},
	})
	require.NoError(t, err)
	_, err = c.DropDataset(ctx, &whv0.DropDatasetRequest{Name: "analytics"})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a dataset that holds a table needs cascade")

	_, err = c.CreateTable(ctx, &whv0.CreateTableRequest{
		Ref:         &whv0.TableRef{Dataset: "analytics", Table: "partitioned"},
		Columns:     []*whv0.Column{{Name: "id", Type: whv0.ColumnType_COLUMN_TYPE_INT64}},
		PartitionBy: []string{"id"},
	})
	require.Equal(t, codes.Unimplemented, status.Code(err))

	_, err = c.GetJob(ctx, &whv0.GetJobRequest{Id: "x"})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = c.CancelJob(ctx, &whv0.CancelJobRequest{Id: "x"})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = c.Load(ctx, &whv0.LoadRequest{Dest: &whv0.TableRef{Dataset: "main", Table: "t"}, SourceUris: []string{"gs://b/x"}})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = c.Unload(ctx, &whv0.UnloadRequest{Source: &whv0.UnloadRequest_Sql{Sql: "SELECT 1"}, DestUri: "gs://b/out"})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = c.Native(ctx, &whv0.NativeRequest{Verb: "checkpoint"})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestDuckDBQueryErrorsKeepTheEngineOffTheWire(t *testing.T) {
	c := dialDuckDB(t)
	for _, tc := range []struct {
		sql  string
		want codes.Code
	}{
		{"SELEC 1", codes.InvalidArgument},
		{"SELECT * FROM absent", codes.NotFound},
		{"SELECT * FROM read_csv('/etc/hosts')", codes.PermissionDenied},
		{"SET threads = 1", codes.Unimplemented},
		{"SELECT 1; SELECT 2", codes.InvalidArgument},
	} {
		_, _, err := query(t, c, &whv0.QueryRequest{Sql: tc.sql})
		require.Equal(t, tc.want, status.Code(err), tc.sql)
		for _, leak := range []string{"Error:", "LINE 1", "/etc/hosts", "DuckDB"} {
			require.NotContains(t, status.Convert(err).Message(), leak, tc.sql)
		}
	}
	_, _, err := query(t, c, &whv0.QueryRequest{Sql: "SELECT 1", MaxBytesBilled: 1 << 20})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

// A client reads the result with nothing but an Arrow reader: the header's
// schema, then the batches.
func TestDuckDBQueryStreamsArrow(t *testing.T) {
	ctx := context.Background()
	c := dialDuckDB(t)

	_, err := c.CreateTable(ctx, &whv0.CreateTableRequest{
		Ref: &whv0.TableRef{Dataset: "main", Table: "people"},
		Columns: []*whv0.Column{
			{Name: "id", Type: whv0.ColumnType_COLUMN_TYPE_INT64},
			{Name: "name", Type: whv0.ColumnType_COLUMN_TYPE_STRING, Nullable: true},
		},
	})
	require.NoError(t, err)
	_, _, err = query(t, c, &whv0.QueryRequest{Sql: "INSERT INTO people VALUES (1, 'ada'), (2, 'alan'), (3, 'grace')"})
	require.NoError(t, err)

	header, recs, err := query(t, c, &whv0.QueryRequest{
		Sql:    "SELECT id, name FROM people WHERE id >= $min ORDER BY id",
		Params: []*whv0.QueryParam{{Name: "min", Type: whv0.ColumnType_COLUMN_TYPE_INT64, Value: "2"}},
	})
	require.NoError(t, err)
	require.Equal(t, whv0.JobState_JOB_STATE_DONE, header.GetJob().GetState())
	require.Len(t, header.GetSchema(), 2)
	require.Equal(t, whv0.ColumnType_COLUMN_TYPE_INT64, header.GetSchema()[0].GetType())
	require.Equal(t, "BIGINT", header.GetSchema()[0].GetNativeType())
	require.Equal(t, whv0.ColumnType_COLUMN_TYPE_STRING, header.GetSchema()[1].GetType())

	require.Len(t, recs, 1)
	require.Equal(t, int64(2), recs[0].NumRows())
	names := recs[0].Column(1).(*array.String)
	require.Equal(t, "alan", names.Value(0))
	require.Equal(t, "grace", names.Value(1))

	// A dry run is the header alone.
	header, recs, err = query(t, c, &whv0.QueryRequest{Sql: "SELECT id FROM people", DryRun: true})
	require.NoError(t, err)
	require.Empty(t, recs)
	require.Len(t, header.GetSchema(), 1)
	require.NotEmpty(t, header.GetArrowIpcSchema())

	// DDL and DML are a header with an empty schema and the rows changed.
	header, recs, err = query(t, c, &whv0.QueryRequest{Sql: "DELETE FROM people WHERE id = 1"})
	require.NoError(t, err)
	require.Empty(t, recs)
	require.Empty(t, header.GetSchema())
	require.Empty(t, header.GetArrowIpcSchema())
	require.Equal(t, int64(1), header.GetJob().GetStats().GetRowsAffected())
}

// InsertRows over a client stream: the header, then Arrow batches; a refused row
// comes back by offset and the rest are written.
func TestDuckDBInsertRowsOverTheWire(t *testing.T) {
	ctx := context.Background()
	c := dialDuckDB(t)
	_, _, err := query(t, c, &whv0.QueryRequest{Sql: "CREATE TABLE ledger (id BIGINT PRIMARY KEY, memo VARCHAR)"})
	require.NoError(t, err)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "memo", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	build := func(ids []int64, memos []string) arrow.RecordBatch {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()
		b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		b.Field(1).(*array.StringBuilder).AppendValues(memos, nil)
		return b.NewRecordBatch()
	}
	first, second := build([]int64{1, 2}, []string{"a", "b"}), build([]int64{2, 3}, []string{"dup", "c"})
	defer first.Release()
	defer second.Release()

	header, err := arrowipc.SchemaMessage(schema)
	require.NoError(t, err)
	enc, err := arrowipc.NewEncoder(schema)
	require.NoError(t, err)
	defer enc.Close()

	stream, err := c.InsertRows(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&whv0.InsertRowsRequest{Kind: &whv0.InsertRowsRequest_Header{Header: &whv0.InsertHeader{
		Table: &whv0.TableRef{Dataset: "main", Table: "ledger"}, ArrowIpcSchema: header,
	}}}))
	for _, rec := range []arrow.RecordBatch{first, second} {
		msg, err := enc.Encode(rec)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&whv0.InsertRowsRequest{Kind: &whv0.InsertRowsRequest_ArrowBatch{ArrowBatch: msg}}))
	}
	res, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, int64(3), res.GetRowsInserted())
	require.Len(t, res.GetErrors(), 1)
	require.Equal(t, int64(2), res.GetErrors()[0].GetRowIndex(), "the first row of the second batch is offset 2 in the stream")
	require.NotContains(t, res.GetErrors()[0].GetError(), "Error:")

	_, recs, err := query(t, c, &whv0.QueryRequest{Sql: "SELECT memo FROM ledger ORDER BY id"})
	require.NoError(t, err)
	memos := recs[0].Column(0).(*array.String)
	require.Equal(t, []string{"a", "b", "c"}, []string{memos.Value(0), memos.Value(1), memos.Value(2)})
}
