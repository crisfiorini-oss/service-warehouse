package bigquery

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	bq "cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// This file turns one cell of an Arrow batch into the JSON value a BigQuery
// streaming insert sends for it. Every value comes out as a JSON-native Go value
// (string, int64, float64, bool, nil, a slice or a map), so what the insert sends
// does not depend on how the client library would have encoded a Go type.
//
// A cell that cannot be sent is a valueError: the row is refused as invalid. A
// column whose Arrow type has no BigQuery equivalent is not a cell's fault and is
// caught for the whole stream by checkArrowSchema before any row is read.

// valueError is a cell BigQuery cannot be given, with the column it is in.
type valueError struct {
	column string
	reason string
}

func (e *valueError) Error() string { return fmt.Sprintf("column %q: %s", e.column, e.reason) }

// Timestamp layouts BigQuery accepts in a JSON row, at its microsecond precision.
const (
	timestampLayoutTZ   = "2006-01-02T15:04:05.000000Z"
	timestampLayoutNoTZ = "2006-01-02T15:04:05.000000"
	dateLayout          = "2006-01-02"
	microsPerSecond     = int64(1_000_000)
	nanosPerMicro       = int64(1_000)
	microsPerDay        = 24 * 3600 * microsPerSecond
)

// checkArrowSchema refuses, before anything is read, an Arrow schema with a type
// that has no BigQuery equivalent. That is a property of the request, not of a
// row, so it fails the call.
func checkArrowSchema(schema *arrow.Schema) error {
	for _, f := range schema.Fields() {
		if err := checkArrowType(f.Name, f.Type); err != nil {
			return err
		}
	}
	return nil
}

func checkArrowType(name string, t arrow.DataType) error {
	switch t := t.(type) {
	case *arrow.BooleanType, *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type,
		*arrow.Float32Type, *arrow.Float64Type,
		*arrow.StringType, *arrow.LargeStringType,
		*arrow.BinaryType, *arrow.LargeBinaryType, *arrow.FixedSizeBinaryType,
		*arrow.Date32Type, *arrow.Date64Type, *arrow.Time32Type, *arrow.Time64Type,
		*arrow.TimestampType, *arrow.Decimal128Type, *arrow.Decimal256Type,
		*arrow.MonthDayNanoIntervalType:
		return nil
	case *arrow.ListType:
		return checkArrowType(name, t.Elem())
	case *arrow.LargeListType:
		return checkArrowType(name, t.Elem())
	case *arrow.StructType:
		for _, f := range t.Fields() {
			if err := checkArrowType(name+"."+f.Name, f.Type); err != nil {
				return err
			}
		}
		return nil
	default:
		return &arrowTypeError{column: name, typeName: t.String()}
	}
}

// arrowTypeError is an Arrow type BigQuery has no equivalent of.
type arrowTypeError struct{ column, typeName string }

func (e *arrowTypeError) Error() string {
	return fmt.Sprintf("column %q has the Arrow type %s, which BigQuery has no equivalent of", e.column, e.typeName)
}

// jsonValue is the JSON value of arr[i] for column name.
func jsonValue(name string, arr arrow.Array, i int) (any, error) {
	if arr.IsNull(i) {
		return nil, nil
	}
	bad := func(format string, args ...any) (any, error) {
		return nil, &valueError{column: name, reason: fmt.Sprintf(format, args...)}
	}
	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(i), nil
	case *array.Int8:
		return int64(a.Value(i)), nil
	case *array.Int16:
		return int64(a.Value(i)), nil
	case *array.Int32:
		return int64(a.Value(i)), nil
	case *array.Int64:
		return a.Value(i), nil
	case *array.Uint8:
		return int64(a.Value(i)), nil
	case *array.Uint16:
		return int64(a.Value(i)), nil
	case *array.Uint32:
		return int64(a.Value(i)), nil
	case *array.Uint64:
		if a.Value(i) > uint64(math.MaxInt64) {
			return bad("the integer %d does not fit a 64-bit signed integer", a.Value(i))
		}
		return int64(a.Value(i)), nil
	case *array.Float32:
		return floatJSON(float64(a.Value(i))), nil
	case *array.Float64:
		return floatJSON(a.Value(i)), nil
	case *array.String:
		return textJSON(name, a.Value(i))
	case *array.LargeString:
		return textJSON(name, a.Value(i))
	case *array.Binary:
		return base64.StdEncoding.EncodeToString(a.Value(i)), nil
	case *array.LargeBinary:
		return base64.StdEncoding.EncodeToString(a.Value(i)), nil
	case *array.FixedSizeBinary:
		return base64.StdEncoding.EncodeToString(a.Value(i)), nil
	case *array.Date32:
		return a.Value(i).ToTime().Format(dateLayout), nil
	case *array.Date64:
		return a.Value(i).ToTime().Format(dateLayout), nil
	case *array.Time32:
		unit := a.DataType().(*arrow.Time32Type).Unit
		return timeOfDay(name, int64(a.Value(i)), unit)
	case *array.Time64:
		unit := a.DataType().(*arrow.Time64Type).Unit
		return timeOfDay(name, int64(a.Value(i)), unit)
	case *array.Timestamp:
		t := a.DataType().(*arrow.TimestampType)
		if t.Unit == arrow.Nanosecond && int64(a.Value(i))%nanosPerMicro != 0 {
			return bad("the timestamp has sub-microsecond precision, which BigQuery cannot store")
		}
		instant := a.Value(i).ToTime(t.Unit)
		if t.TimeZone != "" {
			return instant.UTC().Format(timestampLayoutTZ), nil
		}
		return instant.UTC().Format(timestampLayoutNoTZ), nil
	case *array.Decimal128:
		return a.Value(i).ToString(a.DataType().(*arrow.Decimal128Type).Scale), nil
	case *array.Decimal256:
		return a.Value(i).ToString(a.DataType().(*arrow.Decimal256Type).Scale), nil
	case *array.MonthDayNanoInterval:
		v := a.Value(i)
		const nanosPerHour, nanosPerMinute, nanosPerSecond = int64(3_600_000_000_000), int64(60_000_000_000), int64(1_000_000_000)
		n := v.Nanoseconds
		interval := bq.IntervalValue{
			Years:          v.Months / 12,
			Months:         v.Months % 12,
			Days:           v.Days,
			Hours:          int32(n / nanosPerHour),
			Minutes:        int32(n % nanosPerHour / nanosPerMinute),
			Seconds:        int32(n % nanosPerMinute / nanosPerSecond),
			SubSecondNanos: int32(n % nanosPerSecond),
		}
		return interval.String(), nil
	case *array.List:
		start, end := a.ValueOffsets(i)
		return listJSON(name, a.ListValues(), int(start), int(end))
	case *array.LargeList:
		start, end := a.ValueOffsets(i)
		return listJSON(name, a.ListValues(), int(start), int(end))
	case *array.Struct:
		st := a.DataType().(*arrow.StructType)
		out := make(map[string]any, st.NumFields())
		for k := 0; k < st.NumFields(); k++ {
			v, err := jsonValue(name+"."+st.Field(k).Name, a.Field(k), i)
			if err != nil {
				return nil, err
			}
			out[st.Field(k).Name] = v
		}
		return out, nil
	default:
		return nil, &arrowTypeError{column: name, typeName: arr.DataType().String()}
	}
}

func listJSON(name string, values arrow.Array, start, end int) (any, error) {
	out := make([]any, 0, end-start)
	for k := start; k < end; k++ {
		v, err := jsonValue(name, values, k)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// floatJSON keeps a float a number, except the three values JSON has no number
// for, which BigQuery reads as these strings.
func floatJSON(v float64) any {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "Infinity"
	case math.IsInf(v, -1):
		return "-Infinity"
	default:
		return v
	}
}

// textJSON refuses text BigQuery would refuse: a STRING must be valid UTF-8.
func textJSON(name, s string) (any, error) {
	if !utf8.ValidString(s) {
		return nil, &valueError{column: name, reason: "the text is not valid UTF-8"}
	}
	return s, nil
}

// timeOfDay formats a time of day as BigQuery's TIME text, at microseconds.
func timeOfDay(name string, v int64, unit arrow.TimeUnit) (any, error) {
	var micros int64
	switch unit {
	case arrow.Second:
		micros = v * microsPerSecond
	case arrow.Millisecond:
		micros = v * 1000
	case arrow.Microsecond:
		micros = v
	default:
		if v%nanosPerMicro != 0 {
			return nil, &valueError{column: name, reason: "the time has sub-microsecond precision, which BigQuery cannot store"}
		}
		micros = v / nanosPerMicro
	}
	if micros < 0 || micros >= microsPerDay {
		return nil, &valueError{column: name, reason: "the time of day is outside 00:00:00 to 23:59:59.999999"}
	}
	seconds := micros / microsPerSecond
	return fmt.Sprintf("%02d:%02d:%02d.%06d", seconds/3600, seconds%3600/60, seconds%60, micros%microsPerSecond), nil
}

// asValueError reports whether err is a cell's fault.
func asValueError(err error) (*valueError, bool) {
	var ve *valueError
	return ve, errors.As(err, &ve)
}
