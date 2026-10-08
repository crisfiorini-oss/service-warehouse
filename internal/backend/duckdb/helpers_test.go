package duckdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// open returns an in-memory engine, closed with the test.
func open(t *testing.T, mutate ...func(*backend.Config)) *Backend {
	t.Helper()
	cfg := backend.Config{Kind: "duckdb"}
	for _, m := range mutate {
		m(&cfg)
	}
	be, err := Open(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	return be.(*Backend)
}

// result is a drained query result.
type result struct {
	res  *backend.QueryResult
	recs []arrow.RecordBatch
}

func (r *result) rows() int {
	n := 0
	for _, rec := range r.recs {
		n += int(rec.NumRows())
	}
	return n
}

// column concatenates one column of the result as Go values.
func (r *result) column(i int) []any {
	var out []any
	for _, rec := range r.recs {
		arr := rec.Column(i)
		for row := 0; row < arr.Len(); row++ {
			if arr.IsNull(row) {
				out = append(out, nil)
				continue
			}
			out = append(out, arr.GetOneForMarshal(row))
		}
	}
	return out
}

// run executes sql and decodes every batch through the wire framing, so a test
// reads a result exactly as a client would.
func run(t *testing.T, be backend.Backend, sql string, opts ...backend.QueryOptions) *result {
	t.Helper()
	var o backend.QueryOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	res, err := be.Query(context.Background(), sql, o)
	require.NoError(t, err, sql)
	out := &result{res: res}
	if res.Batches == nil {
		return out
	}
	t.Cleanup(func() { _ = res.Batches.Close() })
	rd, err := arrowipc.NewReader(res.ArrowSchema, res.Batches)
	require.NoError(t, err)
	defer rd.Release()
	for rd.Next() {
		rec := rd.RecordBatch()
		rec.Retain()
		out.recs = append(out.recs, rec)
		t.Cleanup(rec.Release)
	}
	require.NoError(t, rd.Err())
	return out
}

func mustExec(t *testing.T, be backend.Backend, sql string) {
	t.Helper()
	_, err := be.Query(context.Background(), sql, backend.QueryOptions{})
	require.NoError(t, err, sql)
}

// code runs sql and returns the code of the error it fails with.
func code(t *testing.T, be backend.Backend, sql string, opts ...backend.QueryOptions) serr.Code {
	t.Helper()
	var o backend.QueryOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	res, err := be.Query(context.Background(), sql, o)
	if err == nil {
		if res.Batches != nil {
			_ = res.Batches.Close()
		}
		t.Fatalf("expected %q to fail", sql)
	}
	var se *serr.Error
	require.True(t, errors.As(err, &se), "not a serr.Error: %v", err)
	return se.Code
}

// sliceReader is a backend.BatchReader over prepared messages.
type sliceReader struct {
	msgs [][]byte
	err  error
}

func (s *sliceReader) Next() ([]byte, error) {
	if len(s.msgs) == 0 {
		if s.err != nil {
			return nil, s.err
		}
		return nil, io.EOF
	}
	m := s.msgs[0]
	s.msgs = s.msgs[1:]
	return m, nil
}

func (s *sliceReader) Close() error { return nil }

// encode turns records into the header schema and the batch messages an
// InsertRows call carries.
func encode(t *testing.T, recs ...arrow.RecordBatch) ([]byte, *sliceReader) {
	t.Helper()
	schema := recs[0].Schema()
	header, err := arrowipc.SchemaMessage(schema)
	require.NoError(t, err)
	enc, err := arrowipc.NewEncoder(schema)
	require.NoError(t, err)
	defer enc.Close()
	var msgs [][]byte
	for _, rec := range recs {
		m, err := enc.Encode(rec)
		require.NoError(t, err)
		msgs = append(msgs, m)
	}
	return header, &sliceReader{msgs: msgs}
}

// record builds a record batch from builders-by-name.
func record(schema *arrow.Schema, fill func(b []array.Builder)) arrow.RecordBatch {
	builders := make([]array.Builder, schema.NumFields())
	for i, f := range schema.Fields() {
		builders[i] = array.NewBuilder(arrowAllocator, f.Type)
	}
	fill(builders)
	cols := make([]arrow.Array, len(builders))
	n := int64(0)
	for i, b := range builders {
		cols[i] = b.NewArray()
		n = int64(cols[i].Len())
		b.Release()
	}
	rec := array.NewRecordBatch(schema, cols, n)
	for _, c := range cols {
		c.Release()
	}
	return rec
}

// rowsOf builds a record batch of schema from Go values, one slice per row. A
// nil is NULL; a list is a []any and a struct a map[string]any.
func rowsOf(schema *arrow.Schema, rows ...[]any) arrow.RecordBatch {
	return record(schema, func(b []array.Builder) {
		for _, row := range rows {
			for i, v := range row {
				appendAny(b[i], v)
			}
		}
	})
}

func appendAny(b array.Builder, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch b := b.(type) {
	case *array.BooleanBuilder:
		b.Append(v.(bool))
	case *array.Int8Builder:
		b.Append(v.(int8))
	case *array.Int16Builder:
		b.Append(v.(int16))
	case *array.Int32Builder:
		b.Append(v.(int32))
	case *array.Int64Builder:
		b.Append(v.(int64))
	case *array.Uint64Builder:
		b.Append(v.(uint64))
	case *array.Float32Builder:
		b.Append(v.(float32))
	case *array.Float64Builder:
		b.Append(v.(float64))
	case *array.StringBuilder:
		b.Append(v.(string))
	case *array.LargeStringBuilder:
		b.Append(v.(string))
	case *array.BinaryBuilder:
		b.Append(v.([]byte))
	case *array.Date32Builder:
		b.Append(arrow.Date32FromTime(v.(time.Time)))
	case *array.Date64Builder:
		b.Append(arrow.Date64FromTime(v.(time.Time)))
	case *array.Time64Builder:
		b.Append(arrow.Time64(v.(int64)))
	case *array.TimestampBuilder:
		ts, err := arrow.TimestampFromTime(v.(time.Time), b.Type().(*arrow.TimestampType).Unit)
		if err != nil {
			panic(err)
		}
		b.Append(ts)
	case *array.Decimal128Builder:
		b.Append(v.(decimal128.Num))
	case *array.MonthDayNanoIntervalBuilder:
		b.Append(v.(arrow.MonthDayNanoInterval))
	case *array.ListBuilder:
		b.Append(true)
		for _, e := range v.([]any) {
			appendAny(b.ValueBuilder(), e)
		}
	case *array.StructBuilder:
		b.Append(true)
		m := v.(map[string]any)
		for i, f := range b.Type().(*arrow.StructType).Fields() {
			appendAny(b.FieldBuilder(i), m[f.Name])
		}
	default:
		panic(fmt.Sprintf("appendAny: no case for %T", b))
	}
}

// field is a short arrow.Field.
func field(name string, t arrow.DataType) arrow.Field {
	return arrow.Field{Name: name, Type: t, Nullable: true}
}

// insert sends records through InsertRows exactly as a client's stream would.
func insert(t *testing.T, be backend.Backend, table string, recs ...arrow.RecordBatch) (*backend.InsertResult, error) {
	t.Helper()
	header, batches := encode(t, recs...)
	return be.InsertRows(context.Background(), backend.TableRef{Dataset: "main", Table: table}, header, batches)
}
