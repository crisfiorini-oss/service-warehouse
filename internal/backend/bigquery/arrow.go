package bigquery

import (
	"fmt"
	"math/big"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// This file is the Arrow side of the backend, in both directions: BigQuery
// result values into Arrow-IPC batches for Query, and Arrow-IPC batches into
// the values a streaming insert sends for InsertRows.
//
// The framing, which is the contract a client reads against, is not here: the
// header's schema and each batch as an IPC message are written and read by
// internal/arrowipc, the one place that knows how Arrow travels on the wire.

const (
	// batchMaxRows and batchTargetBytes bound one result batch, so a wide table
	// does not become one message a client cannot receive; the byte bound
	// counts the text and binary a batch holds, which is what dominates.
	batchMaxRows     = 8192
	batchTargetBytes = 1 << 20
)

// resultEncoder turns BigQuery rows into Arrow batches.
type resultEncoder struct {
	cols    []backend.Column
	header  []byte // the schema as an IPC message
	builder *array.RecordBuilder
	encoder *arrowipc.Encoder
	rows    int
	size    int
}

func newResultEncoder(cols []backend.Column) (*resultEncoder, error) {
	schema, err := arrowSchema(cols)
	if err != nil {
		return nil, err
	}
	header, err := arrowipc.SchemaMessage(schema)
	if err != nil {
		return nil, serr.Wrap(serr.Internal, "arrow", err)
	}
	fr, err := arrowipc.NewEncoder(schema)
	if err != nil {
		return nil, serr.Wrap(serr.Internal, "arrow", err)
	}
	return &resultEncoder{
		cols:    cols,
		header:  header,
		builder: array.NewRecordBuilder(memory.NewGoAllocator(), schema),
		encoder: fr,
	}, nil
}

// schemaMessage is the IPC Schema message for the result header.
func (e *resultEncoder) schemaMessage() []byte { return e.header }

// add appends one row, which must carry one value per column.
func (e *resultEncoder) add(row []bq.Value) error {
	if len(row) != len(e.cols) {
		return serr.New(serr.Internal, "Query", fmt.Sprintf("a result row has %d values for %d columns", len(row), len(e.cols)))
	}
	for i, c := range e.cols {
		if err := appendValue(e.builder.Field(i), c, row[i]); err != nil {
			return err
		}
		e.size += valueSize(row[i])
	}
	e.rows++
	return nil
}

func (e *resultEncoder) pending() bool { return e.rows > 0 }

func (e *resultEncoder) full() bool { return e.rows >= batchMaxRows || e.size >= batchTargetBytes }

// flush returns the rows added since the last flush as one batch.
func (e *resultEncoder) flush() ([]byte, error) {
	rec := e.builder.NewRecordBatch()
	defer rec.Release()
	e.rows, e.size = 0, 0
	out, err := e.encoder.Encode(rec)
	if err != nil {
		return nil, serr.Wrap(serr.Internal, "arrow", err)
	}
	return out, nil
}

func (e *resultEncoder) release() {
	e.builder.Release()
	_ = e.encoder.Close()
}

// valueSize is a cheap estimate of a value's weight, enough to bound a batch.
func valueSize(v bq.Value) int {
	switch x := v.(type) {
	case string:
		return len(x)
	case []byte:
		return len(x)
	case []bq.Value:
		n := 8
		for _, e := range x {
			n += valueSize(e)
		}
		return n
	default:
		return 8
	}
}

// mismatch is a BigQuery value of a Go type the column's type does not produce.
func mismatch(c backend.Column, v bq.Value) error {
	return serr.New(serr.Internal, "Query", fmt.Sprintf("column %q (%s) produced a value of type %T", c.Name, c.NativeType, v))
}

// appendValue adds one BigQuery value to the Arrow builder of its column.
func appendValue(b array.Builder, c backend.Column, v bq.Value) error {
	if v == nil {
		if c.Type == backend.TypeArray && !c.Nullable {
			// BigQuery has no NULL array; an absent one is an empty array.
			b.(*array.ListBuilder).Append(true)
			return nil
		}
		b.AppendNull()
		return nil
	}
	switch c.Type {
	case backend.TypeBool:
		x, ok := v.(bool)
		if !ok {
			return mismatch(c, v)
		}
		b.(*array.BooleanBuilder).Append(x)
	case backend.TypeInt64:
		x, ok := v.(int64)
		if !ok {
			return mismatch(c, v)
		}
		b.(*array.Int64Builder).Append(x)
	case backend.TypeFloat64:
		x, ok := v.(float64)
		if !ok {
			return mismatch(c, v)
		}
		b.(*array.Float64Builder).Append(x)
	case backend.TypeNumeric:
		x, ok := v.(*big.Rat)
		if !ok {
			return mismatch(c, v)
		}
		return appendDecimal(b, c, x)
	case backend.TypeString, backend.TypeJSON, backend.TypeGeography:
		x, ok := v.(string)
		if !ok {
			return mismatch(c, v)
		}
		b.(*array.StringBuilder).Append(x)
	case backend.TypeBytes:
		x, ok := v.([]byte)
		if !ok {
			return mismatch(c, v)
		}
		b.(*array.BinaryBuilder).Append(x)
	case backend.TypeDate:
		x, ok := v.(civil.Date)
		if !ok {
			return mismatch(c, v)
		}
		b.(*array.Date32Builder).Append(arrow.Date32(x.DaysSince(civil.Date{Year: 1970, Month: time.January, Day: 1})))
	case backend.TypeTime:
		x, ok := v.(civil.Time)
		if !ok {
			return mismatch(c, v)
		}
		micros := ((int64(x.Hour)*60+int64(x.Minute))*60+int64(x.Second))*1_000_000 + int64(x.Nanosecond)/1000
		b.(*array.Time64Builder).Append(arrow.Time64(micros))
	case backend.TypeTimestamp:
		x, ok := v.(civil.DateTime)
		if !ok {
			return mismatch(c, v)
		}
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(x.In(time.UTC).UnixMicro()))
	case backend.TypeTimestampTZ:
		x, ok := v.(time.Time)
		if !ok {
			return mismatch(c, v)
		}
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(x.UnixMicro()))
	case backend.TypeInterval:
		x, ok := v.(*bq.IntervalValue)
		if !ok {
			return mismatch(c, v)
		}
		b.(*array.MonthDayNanoIntervalBuilder).Append(arrow.MonthDayNanoInterval{
			Months:      x.Years*12 + x.Months,
			Days:        x.Days,
			Nanoseconds: ((int64(x.Hours)*60+int64(x.Minutes))*60+int64(x.Seconds))*1_000_000_000 + int64(x.SubSecondNanos),
		})
	case backend.TypeArray:
		x, ok := v.([]bq.Value)
		if !ok {
			return mismatch(c, v)
		}
		lb := b.(*array.ListBuilder)
		lb.Append(true)
		for _, item := range x {
			if err := appendValue(lb.ValueBuilder(), c.Fields[0], item); err != nil {
				return err
			}
		}
	case backend.TypeStruct:
		x, ok := v.([]bq.Value)
		if !ok || len(x) != len(c.Fields) {
			return mismatch(c, v)
		}
		sb := b.(*array.StructBuilder)
		sb.Append(true)
		for i, sub := range c.Fields {
			if err := appendValue(sb.FieldBuilder(i), sub, x[i]); err != nil {
				return err
			}
		}
	default:
		return serr.New(serr.Unsupported, "Query", fmt.Sprintf("column %q has no portable type", c.Name))
	}
	return nil
}

// unscaled is r as an integer count of 10^-scale, which BigQuery guarantees for a
// value of its own column. A value that is not one means the column's declared
// scale and its data disagree, so it is an error and never a rounded number.
func unscaled(r *big.Rat, scale int32) (*big.Int, error) {
	if scale < 0 {
		return nil, fmt.Errorf("negative decimal scale %d", scale)
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	scaled := new(big.Int).Mul(r.Num(), factor)
	quotient, remainder := new(big.Int).QuoRem(scaled, r.Denom(), new(big.Int))
	if remainder.Sign() != 0 {
		return nil, fmt.Errorf("%s has more than %d fractional digits", r.FloatString(40), scale)
	}
	return quotient, nil
}

func appendDecimal(b array.Builder, c backend.Column, r *big.Rat) error {
	switch t := b.Type().(type) {
	case *arrow.Decimal128Type:
		n, err := unscaled(r, t.Scale)
		if err != nil {
			return serr.Wrap(serr.Internal, "Query", fmt.Errorf("column %q: %w", c.Name, err))
		}
		if n.BitLen() > 126 {
			return serr.New(serr.Internal, "Query", fmt.Sprintf("column %q: a value does not fit %s", c.Name, t))
		}
		num := decimal128.FromBigInt(n)
		if !num.FitsInPrecision(t.Precision) {
			return serr.New(serr.Internal, "Query", fmt.Sprintf("column %q: a value does not fit %s", c.Name, t))
		}
		b.(*array.Decimal128Builder).Append(num)
	case *arrow.Decimal256Type:
		n, err := unscaled(r, t.Scale)
		if err != nil {
			return serr.Wrap(serr.Internal, "Query", fmt.Errorf("column %q: %w", c.Name, err))
		}
		if n.BitLen() > 254 {
			return serr.New(serr.Internal, "Query", fmt.Sprintf("column %q: a value does not fit %s", c.Name, t))
		}
		num := decimal256.FromBigInt(n)
		if !num.FitsInPrecision(t.Precision) {
			return serr.New(serr.Internal, "Query", fmt.Sprintf("column %q: a value does not fit %s", c.Name, t))
		}
		b.(*array.Decimal256Builder).Append(num)
	default:
		return mismatch(c, r)
	}
	return nil
}
