package server

import (
	"context"
	"net"
	"testing"

	whv0 "github.com/codefly-dev/service-warehouse/gen/codefly/warehouse/v0"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/backend/mem"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// dial spins up the Warehouse service over an in-memory listener backed by the
// mem backend and returns a connected client.
func dial(t *testing.T) whv0.WarehouseClient {
	t.Helper()
	return dialBackend(t, mem.New(backend.Config{}))
}

// dialBackend is dial over any backend.
func dialBackend(t *testing.T, be backend.Backend) whv0.WarehouseClient {
	t.Helper()
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

func TestServerCatalogRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := dial(t)

	_, err := c.CreateDataset(ctx, &whv0.CreateDatasetRequest{Name: "analytics"})
	require.NoError(t, err)

	ref := &whv0.TableRef{Dataset: "analytics", Table: "events"}
	_, err = c.CreateTable(ctx, &whv0.CreateTableRequest{
		Ref:     ref,
		Columns: []*whv0.Column{{Name: "id", Type: whv0.ColumnType_COLUMN_TYPE_INT64}},
	})
	require.NoError(t, err)

	sc, err := c.GetTable(ctx, &whv0.GetTableRequest{Ref: ref})
	require.NoError(t, err)
	require.Equal(t, "id", sc.GetColumns()[0].GetName())
	require.Equal(t, whv0.ColumnType_COLUMN_TYPE_INT64, sc.GetColumns()[0].GetType())

	ds, err := c.ListDatasets(ctx, &whv0.ListDatasetsRequest{})
	require.NoError(t, err)
	require.Len(t, ds.GetDatasets(), 1)
}

func TestServerQueryUnimplemented(t *testing.T) {
	c := dial(t)
	stream, err := c.Query(context.Background(), &whv0.QueryRequest{Sql: "SELECT 1"})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestServerCapabilities(t *testing.T) {
	c := dial(t)
	caps, err := c.Capabilities(context.Background(), &whv0.CapabilitiesRequest{})
	require.NoError(t, err)
	require.Equal(t, "mem", caps.GetBackend())
	require.True(t, caps.GetDdl())
}

// refusingBackend is mem with an InsertRows that refuses one row for each
// portable reason, so the wire mapping of RowError is exercised without a
// warehouse.
type refusingBackend struct{ *mem.Backend }

func (refusingBackend) InsertRows(_ context.Context, _ backend.TableRef, _ []byte, batches backend.BatchReader) (*backend.InsertResult, error) {
	for {
		if _, err := batches.Next(); err != nil {
			break
		}
	}
	return &backend.InsertResult{
		RowsInserted: 1,
		Errors: []backend.RowError{
			{RowIndex: 0, Error: "no reason given", Reason: backend.RefusalUnspecified},
			{RowIndex: 1, Error: "bad value", Reason: backend.RefusalInvalidValue},
			{RowIndex: 2, Error: "too big", Reason: backend.RefusalRowTooLarge},
			{RowIndex: 3, Error: "table differs", Reason: backend.RefusalSchemaMismatch},
			{RowIndex: 4, Error: "a reason this server was never taught", Reason: backend.RowRefusal(99)},
		},
	}, nil
}

func TestServerInsertRowsCarriesPerRowRefusalReasons(t *testing.T) {
	ctx := context.Background()
	c := dialBackend(t, refusingBackend{mem.New(backend.Config{})})

	stream, err := c.InsertRows(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&whv0.InsertRowsRequest{Kind: &whv0.InsertRowsRequest_Header{
		Header: &whv0.InsertHeader{Table: &whv0.TableRef{Dataset: "d", Table: "t"}},
	}}))
	require.NoError(t, stream.Send(&whv0.InsertRowsRequest{Kind: &whv0.InsertRowsRequest_ArrowBatch{ArrowBatch: []byte("batch")}}))
	res, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.EqualValues(t, 1, res.GetRowsInserted())
	got := make([]whv0.RowRefusal, 0, len(res.GetErrors()))
	for i, e := range res.GetErrors() {
		require.EqualValues(t, i, e.GetRowIndex())
		got = append(got, e.GetReason())
	}
	require.Equal(t, []whv0.RowRefusal{
		whv0.RowRefusal_ROW_REFUSAL_UNSPECIFIED,
		whv0.RowRefusal_ROW_REFUSAL_INVALID_VALUE,
		whv0.RowRefusal_ROW_REFUSAL_ROW_TOO_LARGE,
		whv0.RowRefusal_ROW_REFUSAL_SCHEMA_MISMATCH,
		// A reason the server does not know is left unspecified, never guessed.
		whv0.RowRefusal_ROW_REFUSAL_UNSPECIFIED,
	}, got)
	require.Equal(t, "bad value", res.GetErrors()[1].GetError())
}

func TestServerInsertRowsUnimplementedOnMem(t *testing.T) {
	ctx := context.Background()
	c := dial(t)
	stream, err := c.InsertRows(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&whv0.InsertRowsRequest{Kind: &whv0.InsertRowsRequest_Header{
		Header: &whv0.InsertHeader{Table: &whv0.TableRef{Dataset: "d", Table: "t"}},
	}}))
	_, err = stream.CloseAndRecv()
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
