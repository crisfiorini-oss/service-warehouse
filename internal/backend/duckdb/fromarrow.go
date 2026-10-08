package duckdb

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// This file is toarrow.go in the other direction: the Arrow values of an insert
// become the Go values the driver's appender writes into a column. A column's
// type decides what it accepts, so checkArrow runs once per insert and refuses
// a mismatched column for the whole request; arrowValue then runs per value and
// can refuse a single row (a value out of range, text that is not JSON).

// checkArrow reports whether an Arrow column of type at can be written into a
// column of type dt.
func checkArrow(name string, dt *dtype, at arrow.DataType) error {
	mismatchErr := func() error {
		return serr.New(serr.InvalidArgument, "InsertRows",
			fmt.Sprintf("column %q is %s; an Arrow %s cannot be written to it", name, dt.native(), at))
	}
	switch dt.typ {
	case duckdb.TYPE_BOOLEAN:
		if at.ID() != arrow.BOOL {
			return mismatchErr()
		}
	case duckdb.TYPE_TINYINT, duckdb.TYPE_SMALLINT, duckdb.TYPE_INTEGER, duckdb.TYPE_BIGINT,
		duckdb.TYPE_UTINYINT, duckdb.TYPE_USMALLINT, duckdb.TYPE_UINTEGER, duckdb.TYPE_UBIGINT:
		if !isArrowInteger(at.ID()) {
			return mismatchErr()
		}
	case duckdb.TYPE_FLOAT, duckdb.TYPE_DOUBLE:
		if at.ID() != arrow.FLOAT32 && at.ID() != arrow.FLOAT64 {
			return mismatchErr()
		}
	case duckdb.TYPE_DECIMAL:
		if at.ID() != arrow.DECIMAL128 && !isArrowInteger(at.ID()) {
			return mismatchErr()
		}
	case duckdb.TYPE_VARCHAR, duckdb.TYPE_ENUM:
		if at.ID() != arrow.STRING && at.ID() != arrow.LARGE_STRING {
			return mismatchErr()
		}
	case duckdb.TYPE_BLOB:
		if at.ID() != arrow.BINARY && at.ID() != arrow.LARGE_BINARY && at.ID() != arrow.FIXED_SIZE_BINARY {
			return mismatchErr()
		}
	case duckdb.TYPE_DATE:
		if at.ID() != arrow.DATE32 && at.ID() != arrow.DATE64 {
			return mismatchErr()
		}
	case duckdb.TYPE_TIME:
		if at.ID() != arrow.TIME32 && at.ID() != arrow.TIME64 {
			return mismatchErr()
		}
	case duckdb.TYPE_TIMESTAMP, duckdb.TYPE_TIMESTAMP_S, duckdb.TYPE_TIMESTAMP_MS, duckdb.TYPE_TIMESTAMP_NS,
		duckdb.TYPE_TIMESTAMP_TZ:
		if at.ID() != arrow.TIMESTAMP {
			return mismatchErr()
		}
	case duckdb.TYPE_INTERVAL:
		if at.ID() != arrow.INTERVAL_MONTH_DAY_NANO {
			return mismatchErr()
		}
	case duckdb.TYPE_LIST, duckdb.TYPE_ARRAY:
		var elem arrow.DataType
		switch v := at.(type) {
		case *arrow.ListType:
			elem = v.Elem()
		case *arrow.LargeListType:
			elem = v.Elem()
		case *arrow.FixedSizeListType:
			elem = v.Elem()
		default:
			return mismatchErr()
		}
		return checkArrow(name, dt.elem, elem)
	case duckdb.TYPE_STRUCT:
		st, ok := at.(*arrow.StructType)
		if !ok {
			return mismatchErr()
		}
		for _, f := range st.Fields() {
			target := dt.field(f.Name)
			if target == nil {
				return serr.New(serr.InvalidArgument, "InsertRows",
					fmt.Sprintf("column %q has no struct field %q", name, f.Name))
			}
			if err := checkArrow(name+"."+f.Name, target, f.Type); err != nil {
				return err
			}
		}
	default:
		return serr.New(serr.Unsupported, "InsertRows",
			fmt.Sprintf("column %q is %s, which the duckdb backend cannot insert into", name, dt.native()))
	}
	return nil
}

func isArrowInteger(id arrow.Type) bool {
	switch id {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return true
	}
	return false
}

// field finds a struct field by name; DuckDB names fold case.
func (t *dtype) field(name string) *dtype {
	for _, f := range t.fields {
		if strings.EqualFold(f.name, name) {
			return f.dt
		}
	}
	return nil
}

// fieldName is the stored name of the struct field that name addresses.
func (t *dtype) fieldName(name string) string {
	for _, f := range t.fields {
		if strings.EqualFold(f.name, name) {
			return f.name
		}
	}
	return name
}

// intBounds is each DuckDB integer type's range.
var intBounds = map[duckdb.Type]struct {
	min int64
	max uint64
}{
	duckdb.TYPE_TINYINT:   {math.MinInt8, math.MaxInt8},
	duckdb.TYPE_SMALLINT:  {math.MinInt16, math.MaxInt16},
	duckdb.TYPE_INTEGER:   {math.MinInt32, math.MaxInt32},
	duckdb.TYPE_BIGINT:    {math.MinInt64, math.MaxInt64},
	duckdb.TYPE_UTINYINT:  {0, math.MaxUint8},
	duckdb.TYPE_USMALLINT: {0, math.MaxUint16},
	duckdb.TYPE_UINTEGER:  {0, math.MaxUint32},
	duckdb.TYPE_UBIGINT:   {0, math.MaxUint64},
}

// integerAt reads an Arrow integer as a sign and magnitude, which carries every
// int64 and uint64 without overflow.
func integerAt(arr arrow.Array, i int) (negative bool, magnitude uint64, ok bool) {
	signed := func(v int64) (bool, uint64, bool) {
		if v < 0 {
			return true, uint64(-(v + 1)) + 1, true
		}
		return false, uint64(v), true
	}
	switch a := arr.(type) {
	case *array.Int8:
		return signed(int64(a.Value(i)))
	case *array.Int16:
		return signed(int64(a.Value(i)))
	case *array.Int32:
		return signed(int64(a.Value(i)))
	case *array.Int64:
		return signed(a.Value(i))
	case *array.Uint8:
		return false, uint64(a.Value(i)), true
	case *array.Uint16:
		return false, uint64(a.Value(i)), true
	case *array.Uint32:
		return false, uint64(a.Value(i)), true
	case *array.Uint64:
		return false, a.Value(i), true
	}
	return false, 0, false
}

// arrowValue reads row i of arr as the Go value the appender writes into a
// column of type dt. A nil value is NULL.
func arrowValue(dt *dtype, arr arrow.Array, i int) (driver.Value, error) {
	if arr.IsNull(i) {
		return nil, nil
	}
	switch dt.typ {
	case duckdb.TYPE_BOOLEAN:
		return arr.(*array.Boolean).Value(i), nil

	case duckdb.TYPE_TINYINT, duckdb.TYPE_SMALLINT, duckdb.TYPE_INTEGER, duckdb.TYPE_BIGINT,
		duckdb.TYPE_UTINYINT, duckdb.TYPE_USMALLINT, duckdb.TYPE_UINTEGER, duckdb.TYPE_UBIGINT:
		neg, mag, _ := integerAt(arr, i)
		bounds := intBounds[dt.typ]
		inRange := false
		switch {
		case neg:
			inRange = bounds.min < 0 && mag <= uint64(-(bounds.min+1))+1
		default:
			inRange = mag <= bounds.max
		}
		if !inRange {
			return nil, rowError("a value is out of range for " + dt.native())
		}
		if neg {
			return -int64(mag-1) - 1, nil
		}
		if dt.typ == duckdb.TYPE_UBIGINT {
			return mag, nil
		}
		return int64(mag), nil

	case duckdb.TYPE_FLOAT, duckdb.TYPE_DOUBLE:
		var f float64
		switch a := arr.(type) {
		case *array.Float32:
			f = float64(a.Value(i))
		case *array.Float64:
			f = a.Value(i)
		}
		if dt.typ == duckdb.TYPE_FLOAT && !math.IsInf(f, 0) && !math.IsNaN(f) && math.Abs(f) > math.MaxFloat32 {
			return nil, rowError("a value is out of range for FLOAT")
		}
		return f, nil

	case duckdb.TYPE_DECIMAL:
		return decimalValue(dt, arr, i)

	case duckdb.TYPE_VARCHAR, duckdb.TYPE_ENUM:
		var s string
		switch a := arr.(type) {
		case *array.String:
			s = a.Value(i)
		case *array.LargeString:
			s = a.Value(i)
		}
		if !utf8.ValidString(s) || strings.ContainsRune(s, 0) {
			return nil, rowError("a string is not valid UTF-8 text without NUL bytes")
		}
		if dt.typ == duckdb.TYPE_ENUM && !slices.Contains(dt.enum, s) {
			return nil, rowError("a value is not one of the column's ENUM values")
		}
		if dt.isJSON() {
			// The raw text goes in unchanged. Passing the string itself would
			// have the appender encode it as a JSON string, and decoding it
			// first would round its numbers.
			if !json.Valid([]byte(s)) {
				return nil, rowError("a value is not valid JSON")
			}
			return json.RawMessage(s), nil
		}
		return s, nil

	case duckdb.TYPE_BLOB:
		switch a := arr.(type) {
		case *array.Binary:
			return append([]byte(nil), a.Value(i)...), nil
		case *array.LargeBinary:
			return append([]byte(nil), a.Value(i)...), nil
		case *array.FixedSizeBinary:
			return append([]byte(nil), a.Value(i)...), nil
		}

	case duckdb.TYPE_DATE:
		switch a := arr.(type) {
		case *array.Date32:
			return a.Value(i).ToTime(), nil
		case *array.Date64:
			return a.Value(i).ToTime(), nil
		}

	case duckdb.TYPE_TIME:
		var micros int64
		switch a := arr.(type) {
		case *array.Time32:
			micros = timeOfDay(int64(a.Value(i)), a.DataType().(*arrow.Time32Type).Unit)
		case *array.Time64:
			micros = timeOfDay(int64(a.Value(i)), a.DataType().(*arrow.Time64Type).Unit)
		}
		if micros < 0 || micros >= 24*3_600_000_000 {
			return nil, rowError("a time is outside 00:00:00 to 23:59:59.999999")
		}
		return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(micros) * time.Microsecond), nil

	case duckdb.TYPE_TIMESTAMP, duckdb.TYPE_TIMESTAMP_S, duckdb.TYPE_TIMESTAMP_MS, duckdb.TYPE_TIMESTAMP_NS,
		duckdb.TYPE_TIMESTAMP_TZ:
		a := arr.(*array.Timestamp)
		t := a.Value(i).ToTime(a.DataType().(*arrow.TimestampType).Unit).UTC()
		// The engine stores microseconds; a finer value would be truncated
		// without a word, so it is refused unless the column is TIMESTAMP_NS.
		if dt.typ != duckdb.TYPE_TIMESTAMP_NS && t.Nanosecond()%1000 != 0 {
			return nil, rowError("a timestamp has sub-microsecond precision, which " + dt.native() + " cannot hold")
		}
		if dt.typ == duckdb.TYPE_TIMESTAMP_S && t.Nanosecond() != 0 {
			return nil, rowError("a timestamp has sub-second precision, which TIMESTAMP_S cannot hold")
		}
		if dt.typ == duckdb.TYPE_TIMESTAMP_MS && t.Nanosecond()%1_000_000 != 0 {
			return nil, rowError("a timestamp has sub-millisecond precision, which TIMESTAMP_MS cannot hold")
		}
		return t, nil

	case duckdb.TYPE_INTERVAL:
		v := arr.(*array.MonthDayNanoInterval).Value(i)
		if v.Nanoseconds%1000 != 0 {
			return nil, rowError("an interval has sub-microsecond precision, which INTERVAL cannot hold")
		}
		return duckdb.Interval{Months: v.Months, Days: v.Days, Micros: v.Nanoseconds / 1000}, nil

	case duckdb.TYPE_LIST, duckdb.TYPE_ARRAY:
		return listValue(dt, arr, i)

	case duckdb.TYPE_STRUCT:
		a := arr.(*array.Struct)
		st := a.DataType().(*arrow.StructType)
		out := make(map[string]any, len(dt.fields))
		for _, f := range dt.fields {
			out[f.name] = nil
		}
		for fi, f := range st.Fields() {
			target := dt.field(f.Name)
			v, err := arrowValue(target, a.Field(fi), i)
			if err != nil {
				return nil, err
			}
			out[dt.fieldName(f.Name)] = v
		}
		return out, nil
	}
	return nil, serr.New(serr.Internal, "InsertRows", "no conversion for "+dt.native())
}

// rowError is a refusal of one value, which becomes a per-row error rather than
// a failure of the whole insert.
func rowError(msg string) error { return &valueError{msg} }

func timeOfDay(v int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return v * 1_000_000
	case arrow.Millisecond:
		return v * 1_000
	case arrow.Microsecond:
		return v
	}
	return v / 1000
}

func listValue(dt *dtype, arr arrow.Array, i int) (driver.Value, error) {
	var (
		child      arrow.Array
		start, end int
	)
	switch a := arr.(type) {
	case *array.List:
		s, e := a.ValueOffsets(i)
		child, start, end = a.ListValues(), int(s), int(e)
	case *array.LargeList:
		s, e := a.ValueOffsets(i)
		child, start, end = a.ListValues(), int(s), int(e)
	case *array.FixedSizeList:
		s, e := a.ValueOffsets(i)
		child, start, end = a.ListValues(), int(s), int(e)
	default:
		return nil, serr.New(serr.Internal, "InsertRows", "no conversion for "+dt.native())
	}
	if dt.typ == duckdb.TYPE_ARRAY && uint64(end-start) != dt.size {
		return nil, rowError(fmt.Sprintf("a list has %d elements, and the column holds exactly %d", end-start, dt.size))
	}
	out := make([]any, 0, end-start)
	for j := start; j < end; j++ {
		v, err := arrowValue(dt.elem, child, j)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// decimalValue rescales an Arrow decimal (or integer) to the column's scale. A
// value that would lose digits, or no longer fits the column's precision, is
// refused rather than rounded.
func decimalValue(dt *dtype, arr arrow.Array, i int) (driver.Value, error) {
	var (
		unscaled *big.Int
		scale    int32
	)
	switch a := arr.(type) {
	case *array.Decimal128:
		unscaled = a.Value(i).BigInt()
		scale = a.DataType().(*arrow.Decimal128Type).Scale
	default:
		neg, mag, _ := integerAt(arr, i)
		unscaled = new(big.Int).SetUint64(mag)
		if neg {
			unscaled.Neg(unscaled)
		}
	}
	diff := int32(dt.scale) - scale
	switch {
	case diff > 0:
		unscaled = new(big.Int).Mul(unscaled, pow10(diff))
	case diff < 0:
		q, r := new(big.Int).QuoRem(unscaled, pow10(-diff), new(big.Int))
		if r.Sign() != 0 {
			return nil, rowError("a decimal has more fractional digits than " + dt.native() + " holds")
		}
		unscaled = q
	}
	if len(new(big.Int).Abs(unscaled).String()) > int(dt.width) {
		return nil, rowError("a decimal has more digits than " + dt.native() + " holds")
	}
	return duckdb.Decimal{Width: dt.width, Scale: dt.scale, Value: unscaled}, nil
}

func pow10(n int32) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}
