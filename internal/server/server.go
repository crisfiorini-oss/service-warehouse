// Package server implements the uniform Warehouse gRPC service on top of a
// backend.Backend. It owns only translation and streaming mechanics; every
// provider decision lives in the backend. Errors flow back through toStatus so
// clients branch on gRPC codes, never on a backend's SQLSTATE.
package server

import (
	"context"
	"errors"
	"io"

	whv0 "github.com/codefly-dev/service-warehouse/gen/codefly/warehouse/v0"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server adapts a backend.Backend to the generated WarehouseServer.
type Server struct {
	whv0.UnimplementedWarehouseServer
	be backend.Backend
}

// New builds a Server over the given backend.
func New(be backend.Backend) *Server { return &Server{be: be} }

// Query executes SQL and streams Arrow result batches after a header.
func (s *Server) Query(req *whv0.QueryRequest, stream whv0.Warehouse_QueryServer) error {
	res, err := s.be.Query(stream.Context(), req.GetSql(), queryOptsIn(req))
	if err != nil {
		return toStatus(err)
	}
	if res.Batches != nil {
		defer res.Batches.Close()
	}
	header := &whv0.QueryResponse{Kind: &whv0.QueryResponse_Header{Header: &whv0.QueryHeader{
		Job:            jobOut(res.Job),
		Schema:         columnsOut(res.Schema),
		ArrowIpcSchema: res.ArrowSchema,
	}}}
	if err := stream.Send(header); err != nil {
		return err
	}
	if res.Batches == nil {
		return nil // dry run or DDL/DML — header only.
	}
	for {
		batch, err := res.Batches.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return toStatus(err)
		}
		if err := stream.Send(&whv0.QueryResponse{Kind: &whv0.QueryResponse_ArrowBatch{ArrowBatch: batch}}); err != nil {
			return err
		}
	}
}

func (s *Server) GetJob(ctx context.Context, req *whv0.GetJobRequest) (*whv0.Job, error) {
	j, err := s.be.GetJob(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return jobOut(*j), nil
}

func (s *Server) CancelJob(ctx context.Context, req *whv0.CancelJobRequest) (*whv0.CancelJobResult, error) {
	if err := s.be.CancelJob(ctx, req.GetId()); err != nil {
		return nil, toStatus(err)
	}
	return &whv0.CancelJobResult{}, nil
}

func (s *Server) ListDatasets(ctx context.Context, req *whv0.ListDatasetsRequest) (*whv0.ListDatasetsResult, error) {
	res, err := s.be.ListDatasets(ctx, backend.ListDatasetsOptions{
		PageToken: req.GetPageToken(),
		Limit:     req.GetLimit(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	out := &whv0.ListDatasetsResult{NextPageToken: res.NextPageToken}
	for _, d := range res.Datasets {
		out.Datasets = append(out.Datasets, datasetOut(d))
	}
	return out, nil
}

func (s *Server) ListTables(ctx context.Context, req *whv0.ListTablesRequest) (*whv0.ListTablesResult, error) {
	res, err := s.be.ListTables(ctx, backend.ListTablesOptions{
		Dataset:   req.GetDataset(),
		PageToken: req.GetPageToken(),
		Limit:     req.GetLimit(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	out := &whv0.ListTablesResult{NextPageToken: res.NextPageToken}
	for _, t := range res.Tables {
		out.Tables = append(out.Tables, tableInfoOut(t))
	}
	return out, nil
}

func (s *Server) GetTable(ctx context.Context, req *whv0.GetTableRequest) (*whv0.TableSchema, error) {
	sc, err := s.be.GetTable(ctx, tableRefIn(req.GetRef()))
	if err != nil {
		return nil, toStatus(err)
	}
	return tableSchemaOut(*sc), nil
}

func (s *Server) CreateDataset(ctx context.Context, req *whv0.CreateDatasetRequest) (*whv0.Dataset, error) {
	d, err := s.be.CreateDataset(ctx, req.GetName(), backend.CreateDatasetOptions{
		Location:    req.GetLocation(),
		Description: req.GetDescription(),
		Labels:      req.GetLabels(),
		IfNotExists: req.GetIfNotExists(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return datasetOut(*d), nil
}

func (s *Server) DropDataset(ctx context.Context, req *whv0.DropDatasetRequest) (*whv0.DropResult, error) {
	if err := s.be.DropDataset(ctx, req.GetName(), backend.DropDatasetOptions{
		Cascade:  req.GetCascade(),
		IfExists: req.GetIfExists(),
	}); err != nil {
		return nil, toStatus(err)
	}
	return &whv0.DropResult{}, nil
}

func (s *Server) CreateTable(ctx context.Context, req *whv0.CreateTableRequest) (*whv0.TableSchema, error) {
	sc, err := s.be.CreateTable(ctx, tableRefIn(req.GetRef()), backend.CreateTableOptions{
		Columns:     columnsIn(req.GetColumns()),
		PartitionBy: req.GetPartitionBy(),
		ClusterBy:   req.GetClusterBy(),
		IfNotExists: req.GetIfNotExists(),
		AsSelect:    req.GetAsSelect(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return tableSchemaOut(*sc), nil
}

func (s *Server) DropTable(ctx context.Context, req *whv0.DropTableRequest) (*whv0.DropResult, error) {
	if err := s.be.DropTable(ctx, tableRefIn(req.GetRef()), backend.DropTableOptions{
		IfExists: req.GetIfExists(),
	}); err != nil {
		return nil, toStatus(err)
	}
	return &whv0.DropResult{}, nil
}

func (s *Server) Load(ctx context.Context, req *whv0.LoadRequest) (*whv0.Job, error) {
	j, err := s.be.Load(ctx, backend.LoadOptions{
		Dest:             tableRefIn(req.GetDest()),
		SourceURIs:       req.GetSourceUris(),
		Format:           loadFormatIn(req.GetFormat()),
		WriteDisposition: protoToWriteDisp[req.GetWriteDisposition()],
		Schema:           columnsIn(req.GetSchema()),
		Options:          req.GetOptions(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return jobOut(*j), nil
}

func (s *Server) Unload(ctx context.Context, req *whv0.UnloadRequest) (*whv0.Job, error) {
	opts := backend.UnloadOptions{
		DestURI: req.GetDestUri(),
		Format:  loadFormatIn(req.GetFormat()),
		Options: req.GetOptions(),
	}
	switch src := req.GetSource().(type) {
	case *whv0.UnloadRequest_Table:
		ref := tableRefIn(src.Table)
		opts.Table = &ref
	case *whv0.UnloadRequest_Sql:
		opts.SQL = src.Sql
	default:
		return nil, status.Error(codes.InvalidArgument, "Unload: source (table or sql) is required")
	}
	j, err := s.be.Unload(ctx, opts)
	if err != nil {
		return nil, toStatus(err)
	}
	return jobOut(*j), nil
}

// InsertRows reads a header, then streams Arrow batches into the backend.
func (s *Server) InsertRows(stream whv0.Warehouse_InsertRowsServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	h, ok := first.GetKind().(*whv0.InsertRowsRequest_Header)
	if !ok {
		return status.Error(codes.InvalidArgument, "InsertRows: first message must be the header")
	}
	reader := &streamBatchReader{stream: stream}
	res, err := s.be.InsertRows(stream.Context(), tableRefIn(h.Header.GetTable()), h.Header.GetArrowIpcSchema(), reader)
	if err != nil {
		return toStatus(err)
	}
	out := &whv0.InsertRowsResult{RowsInserted: res.RowsInserted}
	for _, e := range res.Errors {
		out.Errors = append(out.Errors, rowErrorOut(e))
	}
	return stream.SendAndClose(out)
}

func (s *Server) Capabilities(_ context.Context, _ *whv0.CapabilitiesRequest) (*whv0.BackendCapabilities, error) {
	return capabilitiesOut(s.be.Capabilities()), nil
}

func (s *Server) Native(ctx context.Context, req *whv0.NativeRequest) (*whv0.NativeResult, error) {
	values, err := s.be.Native(ctx, req.GetVerb(), req.GetParams())
	if err != nil {
		return nil, toStatus(err)
	}
	return &whv0.NativeResult{Values: values}, nil
}

// streamBatchReader adapts the incoming InsertRows stream to a
// backend.BatchReader: each Next pulls the next Arrow batch off the wire. A
// header seen mid-stream is a client error.
type streamBatchReader struct {
	stream whv0.Warehouse_InsertRowsServer
}

func (r *streamBatchReader) Next() ([]byte, error) {
	msg, err := r.stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	b, ok := msg.GetKind().(*whv0.InsertRowsRequest_ArrowBatch)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "InsertRows: expected an arrow_batch after the header")
	}
	return b.ArrowBatch, nil
}

func (r *streamBatchReader) Close() error { return nil }
