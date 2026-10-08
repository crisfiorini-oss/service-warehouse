package bigquery

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/big"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// This file is the Arrow side of the backend, in both directions: BigQuery
// result values into Arrow-IPC batches for Query, and Arrow-IPC batches into
// the values a streaming insert sends for InsertRows.
//
// Framing, which is the contract a client reads against: the header carries one
// encapsulated IPC Schema message, and every batch is one encapsulated IPC
// RecordBatch message without the schema. The schema followed by the batches is
// a valid Arrow IPC stream (an end-of-stream marker is optional), which is how
// BigQuery's own Storage Read API frames Arrow too.

const (
	// batchMaxRows and batchTargetBytes bound one result batch, so a wide table
	// does not become one message a client cannot receive; the byte bound
	// counts the text and binary a batch holds, which is what dominates.
	batchMaxRows     = 8192
	batchTargetBytes = 1 << 20
)

// eos is the 8-byte end-of-stream marker an IPC stream writer appends: the
// continuation indicator followed by a zero metadata length.
var eos = []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}

// framer writes the schema message once and each record batch as a message of
// its own, by running one IPC stream writer and cutting what it emits.
type framer struct {
	schema    *arrow.Schema
	schemaMsg []byte
	buf       bytes.Buffer
	writer    *ipc.Writer
	primed    bool
}

func newFramer(schema *arrow.Schema, mem memory.Allocator) (*framer, error) {
	// A stream with no batches is the schema message and the end marker.
	var only bytes.Buffer
	empty := ipc.NewWriter(&only, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := empty.Close(); err != nil {
		return nil, serr.Wrap(serr.Internal, "arrow", fmt.Errorf("encode the schema: %w", err))
	}
	encoded := only.Bytes()
	if !bytes.HasSuffix(encoded, eos) {
		return nil, serr.New(serr.Internal, "arrow", "the IPC writer's stream does not end with the end-of-stream marker")
	}
	f := &framer{schema: schema, schemaMsg: append([]byte(nil), encoded[:len(encoded)-len(eos)]...)}
	f.writer = ipc.NewWriter(&f.buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	return f, nil
}

// batch returns rec as one RecordBatch message.
func (f *framer) batch(rec arrow.RecordBatch) ([]byte, error) {
	f.buf.Reset()
	if err := f.writer.Write(rec); err != nil {
		return nil, serr.Wrap(serr.Internal, "arrow", fmt.Errorf("encode a batch: %w", err))
	}
	out := f.buf.Bytes()
	if !f.primed {
		// The first write also carries the schema message, which the header
		// already sent.
		if !bytes.HasPrefix(out, f.schemaMsg) {
			return nil, serr.New(serr.Internal, "arrow", "the IPC writer's first batch does not start with the schema message")
		}
		out = out[len(f.schemaMsg):]
		f.primed = true
	}
	return append([]byte(nil), out...), nil
}

// resultEncoder turns BigQuery rows into Arrow batches.
type resultEncoder struct {
	cols    []backend.Column
	mem     memory.Allocator
	builder *array.RecordBuilder
	framer  *framer
	rows    int
	size    int
}

func newResultEncoder(cols []backend.Column) (*resultEncoder, error) {
	schema, err := arrowSchema(cols)
	if err != nil {
		return nil, err
	}
	mem := memory.NewGoAllocator()
	fr, err := newFramer(schema, mem)
	if err != nil {
		return nil, err
	}
	return &resultEncoder{
		cols:    cols,
		mem:     mem,
		builder: array.NewRecordBuilder(mem, schema),
		framer:  fr,
	}, nil
}

// schemaMessage is the IPC Schema message for the result header.
func (e *resultEncoder) schemaMessage() []byte { return e.framer.schemaMsg }

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
	return e.framer.batch(rec)
}

func (e *resultEncoder) release() { e.builder.Release() }

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

// batchStream reads the Arrow batches of an InsertRows call. The header's schema
// message, when there is one, completes each batch; a batch that carries its own
// schema is read as the stream it is.
type batchStream struct {
	mem    memory.Allocator
	header []byte // the header's Schema message with any end marker removed; nil when absent
	src    backend.BatchReader

	schema *arrow.Schema // the schema every record must have, known after the first
	cur    *ipc.Reader
}

func newBatchStream(header []byte, src backend.BatchReader) (*batchStream, error) {
	s := &batchStream{mem: memory.NewGoAllocator(), src: src}
	if len(header) == 0 {
		return s, nil
	}
	s.header = bytes.TrimSuffix(header, eos)
	schema, err := s.readSchema(bytes.NewReader(s.header))
	if err != nil {
		return nil, err
	}
	s.schema = schema
	return s, nil
}

func invalidBatch(err error) error {
	return serr.Wrap(serr.InvalidArgument, "InsertRows", fmt.Errorf("the Arrow data is malformed: %w", err))
}

// guard turns a panic inside the Arrow reader, which is fed bytes from the
// network, into an invalid-argument error.
func guard(err *error) {
	if r := recover(); r != nil {
		*err = invalidBatch(fmt.Errorf("%v", r))
	}
}

func (s *batchStream) readSchema(r io.Reader) (schema *arrow.Schema, err error) {
	defer guard(&err)
	reader, err := ipc.NewReader(r, ipc.WithAllocator(s.mem))
	if err != nil {
		return nil, invalidBatch(err)
	}
	defer reader.Release()
	return reader.Schema(), nil
}

// carriesSchema reports whether an IPC payload begins with a Schema message.
func carriesSchema(payload []byte) bool {
	messages := ipc.NewMessageReader(bytes.NewReader(payload))
	defer messages.Release()
	message, err := messages.Message()
	if err != nil {
		return false
	}
	defer message.Release()
	return message.Type() == ipc.MessageSchema
}

// next returns the next record batch, or io.EOF when the stream is exhausted. The
// caller releases the record.
func (s *batchStream) next() (rec arrow.RecordBatch, err error) {
	defer guard(&err)
	for {
		if s.cur != nil {
			if s.cur.Next() {
				rec := s.cur.RecordBatch()
				if s.schema == nil {
					s.schema = rec.Schema()
				} else if !s.schema.Equal(rec.Schema()) {
					return nil, serr.New(serr.InvalidArgument, "InsertRows", "a batch's schema differs from the schema of the batches before it")
				}
				rec.Retain()
				return rec, nil
			}
			err := s.cur.Err()
			s.cur.Release()
			s.cur = nil
			if err != nil {
				return nil, invalidBatch(err)
			}
		}
		payload, err := s.src.Next()
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		var stream io.Reader
		switch {
		case carriesSchema(payload):
			stream = bytes.NewReader(payload)
		case s.header != nil:
			stream = io.MultiReader(bytes.NewReader(s.header), bytes.NewReader(payload))
		default:
			return nil, serr.New(serr.InvalidArgument, "InsertRows", "a batch carries no schema and the header gave none")
		}
		reader, err := ipc.NewReader(stream, ipc.WithAllocator(s.mem))
		if err != nil {
			return nil, invalidBatch(err)
		}
		s.cur = reader
	}
}

func (s *batchStream) close() {
	if s.cur != nil {
		s.cur.Release()
		s.cur = nil
	}
}
