package duckdb

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// appendValue appends v, the Go value the driver produced for a column of type
// t, to b — a builder made for t.arrowType(). A value that cannot be carried
// exactly is an error: a result is exact or it is refused.
func appendValue(b array.Builder, t *dtype, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch t.typ {
	case duckdb.TYPE_BOOLEAN:
		x, ok := v.(bool)
		if !ok {
			return mismatch(t, v)
		}
		b.(*array.BooleanBuilder).Append(x)

	case duckdb.TYPE_TINYINT, duckdb.TYPE_SMALLINT, duckdb.TYPE_INTEGER, duckdb.TYPE_BIGINT,
		duckdb.TYPE_UTINYINT, duckdb.TYPE_USMALLINT, duckdb.TYPE_UINTEGER:
		x, ok := asInt64(v)
		if !ok {
			return mismatch(t, v)
		}
		b.(*array.Int64Builder).Append(x)

	case duckdb.TYPE_UBIGINT:
		x, ok := v.(uint64)
		if !ok {
			return mismatch(t, v)
		}
		b.(*array.Decimal128Builder).Append(decimal128.FromU64(x))

	case duckdb.TYPE_FLOAT:
		x, ok := v.(float32)
		if !ok {
			return mismatch(t, v)
		}
		b.(*array.Float64Builder).Append(float64(x))

	case duckdb.TYPE_DOUBLE:
		x, ok := v.(float64)
		if !ok {
			return mismatch(t, v)
		}
		b.(*array.Float64Builder).Append(x)

	case duckdb.TYPE_DECIMAL:
		x, ok := v.(duckdb.Decimal)
		if !ok || x.Value == nil {
			return mismatch(t, v)
		}
		b.(*array.Decimal128Builder).Append(decimal128.FromBigInt(x.Value))

	case duckdb.TYPE_VARCHAR, duckdb.TYPE_ENUM, duckdb.TYPE_BIT:
		x, ok := v.(string)
		if !ok {
			return mismatch(t, v)
		}
		b.(*array.StringBuilder).Append(x)

	case duckdb.TYPE_BLOB, duckdb.TYPE_GEOMETRY:
		x, ok := v.([]byte)
		if !ok {
			return mismatch(t, v)
		}
		b.(*array.BinaryBuilder).Append(x)

	case duckdb.TYPE_UUID:
		x, ok := v.([]byte)
		if !ok || len(x) != 16 {
			return mismatch(t, v)
		}
		b.(*array.StringBuilder).Append(formatUUID(x))

	case duckdb.TYPE_HUGEINT, duckdb.TYPE_UHUGEINT:
		x, ok := v.(*big.Int)
		if !ok {
			return mismatch(t, v)
		}
		// 128 bits hold one more digit than NUMERIC(38,0) does.
		if x.CmpAbs(maxNumeric38) > 0 {
			return &valueError{"an integer of more than 38 digits does not fit NUMERIC(38,0)"}
		}
		b.(*array.Decimal128Builder).Append(decimal128.FromBigInt(x))

	case duckdb.TYPE_BIGNUM:
		x, ok := v.(*big.Int)
		if !ok {
			return mismatch(t, v)
		}
		b.(*array.StringBuilder).Append(x.String())

	case duckdb.TYPE_DATE:
		x, ok := v.(time.Time)
		if !ok {
			return mismatch(t, v)
		}
		b.(*array.Date32Builder).Append(arrow.Date32FromTime(x))

	case duckdb.TYPE_TIME:
		x, ok := v.(time.Time)
		if !ok {
			return mismatch(t, v)
		}
		b.(*array.Time64Builder).Append(arrow.Time64(timeOfDayMicros(x)))

	case duckdb.TYPE_TIME_TZ:
		x, ok := v.(time.Time)
		if !ok {
			return mismatch(t, v)
		}
		b.(*array.StringBuilder).Append(x.Format("15:04:05.999999999Z07:00"))

	case duckdb.TYPE_TIMESTAMP, duckdb.TYPE_TIMESTAMP_S, duckdb.TYPE_TIMESTAMP_MS, duckdb.TYPE_TIMESTAMP_NS,
		duckdb.TYPE_TIMESTAMP_TZ:
		x, ok := v.(time.Time)
		if !ok {
			return mismatch(t, v)
		}
		unit := t.arrowType().(*arrow.TimestampType).Unit
		ts, err := arrow.TimestampFromTime(x, unit)
		if err != nil {
			return &valueError{fmt.Sprintf("timestamp %s is outside the range Arrow carries", x.Format(time.RFC3339Nano))}
		}
		b.(*array.TimestampBuilder).Append(ts)

	case duckdb.TYPE_INTERVAL:
		x, ok := v.(duckdb.Interval)
		if !ok {
			return mismatch(t, v)
		}
		const maxMicros = int64(1<<63-1) / 1000
		if x.Micros > maxMicros || x.Micros < -maxMicros {
			return &valueError{fmt.Sprintf("interval of %d microseconds does not fit Arrow's nanosecond interval", x.Micros)}
		}
		b.(*array.MonthDayNanoIntervalBuilder).Append(arrow.MonthDayNanoInterval{
			Months: x.Months, Days: x.Days, Nanoseconds: x.Micros * 1000,
		})

	case duckdb.TYPE_LIST, duckdb.TYPE_ARRAY:
		x, ok := v.([]any)
		if !ok {
			return mismatch(t, v)
		}
		lb := b.(*array.ListBuilder)
		lb.Append(true)
		for _, e := range x {
			if err := appendValue(lb.ValueBuilder(), t.elem, e); err != nil {
				return err
			}
		}

	case duckdb.TYPE_STRUCT:
		x, ok := v.(map[string]any)
		if !ok {
			return mismatch(t, v)
		}
		sb := b.(*array.StructBuilder)
		sb.Append(true)
		for i, f := range t.fields {
			if err := appendValue(sb.FieldBuilder(i), f.dt, x[f.name]); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("no Arrow encoding for %s", t.native())
	}
	return nil
}

// maxNumeric38 is 10^38 - 1, the largest magnitude NUMERIC(38,0) holds.
var maxNumeric38 = new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))

func mismatch(t *dtype, v any) error {
	return serr.New(serr.Internal, "Query", fmt.Sprintf("the engine produced %T for a %s column", v, t.native()))
}

// valueError is a value Arrow cannot carry exactly. The result is refused as
// Unsupported instead of returning a rounded or clamped stand-in.
type valueError struct{ msg string }

func (e *valueError) Error() string { return e.msg }

// asInt64 widens any integer the driver returns for a column of at most 32
// bits, or BIGINT, to int64.
func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	}
	return 0, false
}

// timeOfDayMicros is the microseconds since midnight of a TIME the driver
// returned as a time.Time on a fixed date.
func timeOfDayMicros(t time.Time) int64 {
	return int64(t.Hour())*3_600_000_000 + int64(t.Minute())*60_000_000 +
		int64(t.Second())*1_000_000 + int64(t.Nanosecond())/1000
}

// formatUUID spells a 16-byte UUID in its canonical 8-4-4-4-12 form.
func formatUUID(b []byte) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 0, 36)
	for i, c := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out = append(out, '-')
		}
		out = append(out, hex[c>>4], hex[c&0x0f])
	}
	return string(out)
}

// rowsToRecord drains up to limit rows from next into one record batch. It
// returns the record and whether the source is exhausted; a record with no rows
// is nil.
func rowsToRecord(schema *arrow.Schema, types []*dtype, next func(dst []driver.Value) error, limit int) (arrow.RecordBatch, bool, error) {
	mem := arrowAllocator
	builders := make([]array.Builder, len(types))
	for i, t := range types {
		builders[i] = array.NewBuilder(mem, t.arrowType())
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	dst := make([]driver.Value, len(types))
	n, exhausted := 0, false
	for n < limit {
		if err := next(dst); err != nil {
			if isEOF(err) {
				exhausted = true
				break
			}
			return nil, false, err
		}
		for i, t := range types {
			if err := appendValue(builders[i], t, dst[i]); err != nil {
				var ve *valueError
				if errors.As(err, &ve) {
					return nil, false, &valueError{fmt.Sprintf("column %q: %s", schema.Field(i).Name, ve.msg)}
				}
				return nil, false, err
			}
		}
		n++
	}
	if n == 0 {
		return nil, exhausted, nil
	}
	cols := make([]arrow.Array, len(builders))
	for i, b := range builders {
		cols[i] = b.NewArray()
	}
	rec := array.NewRecordBatch(schema, cols, int64(n))
	for _, c := range cols {
		c.Release()
	}
	return rec, exhausted, nil
}
