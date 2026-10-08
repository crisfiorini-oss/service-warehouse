package bigquery

import (
	"bytes"
	"io"
	"math"
	"math/big"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/serr"
)

func rat(s string) *big.Rat {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		panic("bad rat " + s)
	}
	return r
}

// everyType is a BigQuery schema with one column of each type this backend maps,
// and the values a result of it holds: a full row, and a row of NULLs.
func everyType() (bq.Schema, [][]bq.Value, [][]any) {
	schema := bq.Schema{
		{Name: "b", Type: bq.BooleanFieldType},
		{Name: "i", Type: bq.IntegerFieldType},
		{Name: "f", Type: bq.FloatFieldType},
		{Name: "n", Type: bq.NumericFieldType},
		{Name: "bn", Type: bq.BigNumericFieldType},
		{Name: "s", Type: bq.StringFieldType},
		{Name: "y", Type: bq.BytesFieldType},
		{Name: "d", Type: bq.DateFieldType},
		{Name: "tm", Type: bq.TimeFieldType},
		{Name: "dt", Type: bq.DateTimeFieldType},
		{Name: "ts", Type: bq.TimestampFieldType},
		{Name: "iv", Type: bq.IntervalFieldType},
		{Name: "j", Type: bq.JSONFieldType},
		{Name: "g", Type: bq.GeographyFieldType},
		{Name: "arr", Type: bq.IntegerFieldType, Repeated: true},
		{Name: "st", Type: bq.RecordFieldType, Schema: bq.Schema{
			{Name: "a", Type: bq.IntegerFieldType}, {Name: "z", Type: bq.StringFieldType},
		}},
		{Name: "sa", Type: bq.RecordFieldType, Repeated: true, Schema: bq.Schema{{Name: "x", Type: bq.IntegerFieldType}}},
	}
	values := [][]bq.Value{
		{
			true, int64(-42), 1.5, rat("12.34"), rat("-98765432109876543210.12345678901234567890"),
			"héllo", []byte{0, 1, 2, 255},
			civil.Date{Year: 2024, Month: time.February, Day: 29},
			civil.Time{Hour: 13, Minute: 4, Second: 5, Nanosecond: 123456000},
			civil.DateTime{Date: civil.Date{Year: 2024, Month: time.February, Day: 29}, Time: civil.Time{Hour: 13, Minute: 4, Second: 5, Nanosecond: 123456000}},
			time.Date(2024, time.February, 29, 13, 4, 5, 123456000, time.UTC),
			&bq.IntervalValue{Years: 1, Months: 2, Days: 3, Hours: 4, Minutes: 5, Seconds: 6, SubSecondNanos: 789000000},
			`{"k":[1,2]}`, "POINT(1 2)",
			[]bq.Value{int64(1), int64(2), int64(3)},
			[]bq.Value{int64(7), "seven"},
			[]bq.Value{[]bq.Value{int64(1)}, []bq.Value{int64(2)}},
		},
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, []bq.Value(nil), nil, []bq.Value(nil)},
	}
	// What a client reading the batches back sees, as the JSON a streaming insert
	// would send for the same cell: the reverse direction of the same mapping.
	wantJSON := [][]any{
		{
			true, int64(-42), 1.5, "12.340000000", "-98765432109876543210.12345678901234567890000000000000000000",
			"héllo", "AAEC/w==", "2024-02-29", "13:04:05.123456",
			"2024-02-29T13:04:05.123456", "2024-02-29T13:04:05.123456Z",
			"1-2 3 4:5:6.789", `{"k":[1,2]}`, "POINT(1 2)",
			[]any{int64(1), int64(2), int64(3)},
			map[string]any{"a": int64(7), "z": "seven"},
			[]any{map[string]any{"x": int64(1)}, map[string]any{"x": int64(2)}},
		},
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, []any{}, nil, []any{}},
	}
	return schema, values, wantJSON
}

func readBack(t *testing.T, schemaMsg []byte, batch []byte) arrow.RecordBatch {
	t.Helper()
	reader, err := ipc.NewReader(io.MultiReader(bytes.NewReader(schemaMsg), bytes.NewReader(batch)))
	require.NoError(t, err)
	t.Cleanup(reader.Release)
	require.True(t, reader.Next(), "the batch holds a record: %v", reader.Err())
	rec := reader.RecordBatch()
	rec.Retain()
	t.Cleanup(rec.Release)
	return rec
}

func TestResultEncoderRoundTripsEveryType(t *testing.T) {
	schema, rows, want := everyType()
	cols := columnsFromSchema(schema)
	enc, err := newResultEncoder(cols)
	require.NoError(t, err)
	defer enc.release()
	for _, row := range rows {
		require.NoError(t, enc.add(row))
	}
	batch, err := enc.flush()
	require.NoError(t, err)
	rec := readBack(t, enc.schemaMessage(), batch)

	require.EqualValues(t, len(rows), rec.NumRows())
	for c, col := range cols {
		for r := range rows {
			got, err := jsonValue(col.Name, rec.Column(c), r)
			require.NoError(t, err, "column %s row %d", col.Name, r)
			require.Equal(t, want[r][c], got, "column %s row %d", col.Name, r)
		}
	}
}

func TestResultEncoderArrowTypes(t *testing.T) {
	schema, _, _ := everyType()
	cols := columnsFromSchema(schema)
	got, err := arrowSchema(cols)
	require.NoError(t, err)
	want := map[string]arrow.DataType{
		"b": arrow.FixedWidthTypes.Boolean, "i": arrow.PrimitiveTypes.Int64, "f": arrow.PrimitiveTypes.Float64,
		"n":  &arrow.Decimal128Type{Precision: 38, Scale: 9},
		"bn": &arrow.Decimal256Type{Precision: 76, Scale: 38},
		"s":  arrow.BinaryTypes.String, "y": arrow.BinaryTypes.Binary,
		"d": arrow.FixedWidthTypes.Date32, "tm": arrow.FixedWidthTypes.Time64us,
		"dt": &arrow.TimestampType{Unit: arrow.Microsecond},
		"ts": &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
		"iv": arrow.FixedWidthTypes.MonthDayNanoInterval,
		"j":  arrow.BinaryTypes.String, "g": arrow.BinaryTypes.String,
	}
	for name, dt := range want {
		idx := got.FieldIndices(name)
		require.Len(t, idx, 1, name)
		require.True(t, arrow.TypeEqual(dt, got.Field(idx[0]).Type), "%s: got %s want %s", name, got.Field(idx[0]).Type, dt)
	}
	arr := got.Field(got.FieldIndices("arr")[0])
	require.Equal(t, "list<item: int64>", arr.Type.String(), "an ARRAY is a list of elements that are never NULL")
	require.False(t, arr.Nullable, "BigQuery has no NULL array")
	st := got.Field(got.FieldIndices("st")[0]).Type.(*arrow.StructType)
	require.Equal(t, []string{"a", "z"}, []string{st.Field(0).Name, st.Field(1).Name})
}

func TestNumericIsCarriedWithItsDeclaredPrecisionAndScale(t *testing.T) {
	cols := columnsFromSchema(bq.Schema{{Name: "p", Type: bq.NumericFieldType, Precision: 10, Scale: 2}})
	schema, err := arrowSchema(cols)
	require.NoError(t, err)
	require.True(t, arrow.TypeEqual(&arrow.Decimal128Type{Precision: 10, Scale: 2}, schema.Field(0).Type))

	enc, err := newResultEncoder(cols)
	require.NoError(t, err)
	defer enc.release()
	require.NoError(t, enc.add([]bq.Value{rat("-123.45")}))
	batch, err := enc.flush()
	require.NoError(t, err)
	rec := readBack(t, enc.schemaMessage(), batch)
	require.Equal(t, decimal128.FromI64(-12345), rec.Column(0).(*array.Decimal128).Value(0))
}

func TestADecimalThatDoesNotFitItsColumnIsAnErrorNotARoundedNumber(t *testing.T) {
	cols := columnsFromSchema(bq.Schema{{Name: "p", Type: bq.NumericFieldType, Precision: 10, Scale: 2}})
	enc, err := newResultEncoder(cols)
	require.NoError(t, err)
	defer enc.release()
	require.Error(t, enc.add([]bq.Value{rat("1.005")}), "a third fractional digit would be rounded away")
	require.Error(t, enc.add([]bq.Value{rat("123456789.01")}), "eleven digits do not fit precision 10")
}

func TestResultEncoderRefusesAValueOfTheWrongType(t *testing.T) {
	enc, err := newResultEncoder(columnsFromSchema(bq.Schema{{Name: "i", Type: bq.IntegerFieldType}}))
	require.NoError(t, err)
	defer enc.release()
	err = enc.add([]bq.Value{"not an int"})
	require.True(t, serr.Is(err, serr.Internal))
	require.Error(t, enc.add([]bq.Value{}), "a row with too few values")
}

func TestJSONValuesOfTheCellsAStreamingInsertSends(t *testing.T) {
	mem := memory.NewGoAllocator()
	build := func(dt arrow.DataType, fill func(b array.Builder)) arrow.Array {
		b := array.NewBuilder(mem, dt)
		defer b.Release()
		fill(b)
		return b.NewArray()
	}
	cell := func(arr arrow.Array) (any, error) {
		defer arr.Release()
		return jsonValue("c", arr, 0)
	}

	for name, tc := range map[string]struct {
		arr  arrow.Array
		want any
	}{
		"NaN is the string BigQuery reads as NaN": {build(arrow.PrimitiveTypes.Float64, func(b array.Builder) { b.(*array.Float64Builder).Append(math.NaN()) }), "NaN"},
		"+Inf":             {build(arrow.PrimitiveTypes.Float64, func(b array.Builder) { b.(*array.Float64Builder).Append(math.Inf(1)) }), "Infinity"},
		"-Inf":             {build(arrow.PrimitiveTypes.Float64, func(b array.Builder) { b.(*array.Float64Builder).Append(math.Inf(-1)) }), "-Infinity"},
		"float32":          {build(arrow.PrimitiveTypes.Float32, func(b array.Builder) { b.(*array.Float32Builder).Append(0.5) }), 0.5},
		"small uint":       {build(arrow.PrimitiveTypes.Uint32, func(b array.Builder) { b.(*array.Uint32Builder).Append(4000000000) }), int64(4000000000)},
		"uint64 in range":  {build(arrow.PrimitiveTypes.Uint64, func(b array.Builder) { b.(*array.Uint64Builder).Append(math.MaxInt64) }), int64(math.MaxInt64)},
		"time in seconds":  {build(&arrow.Time32Type{Unit: arrow.Second}, func(b array.Builder) { b.(*array.Time32Builder).Append(arrow.Time32(3661)) }), "01:01:01.000000"},
		"naive timestamp":  {build(&arrow.TimestampType{Unit: arrow.Millisecond}, func(b array.Builder) { b.(*array.TimestampBuilder).Append(arrow.Timestamp(1700000000123)) }), "2023-11-14T22:13:20.123000"},
		"timestamp in ns":  {build(&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, func(b array.Builder) { b.(*array.TimestampBuilder).Append(arrow.Timestamp(1700000000123456000)) }), "2023-11-14T22:13:20.123456Z"},
		"date64":           {build(arrow.FixedWidthTypes.Date64, func(b array.Builder) { b.(*array.Date64Builder).Append(arrow.Date64(86400000 * 19000)) }), "2022-01-08"},
		"fixed size bytes": {build(&arrow.FixedSizeBinaryType{ByteWidth: 2}, func(b array.Builder) { b.(*array.FixedSizeBinaryBuilder).Append([]byte{1, 2}) }), "AQI="},
		"negative decimal": {build(&arrow.Decimal128Type{Precision: 10, Scale: 2}, func(b array.Builder) { b.(*array.Decimal128Builder).Append(decimal128.FromI64(-5)) }), "-0.05"},
		"interval": {build(arrow.FixedWidthTypes.MonthDayNanoInterval, func(b array.Builder) {
			b.(*array.MonthDayNanoIntervalBuilder).Append(arrow.MonthDayNanoInterval{Months: 14, Days: 1, Nanoseconds: 3_661_500_000_000})
		}), "1-2 1 1:1:1.5"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := cell(tc.arr)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	for name, arr := range map[string]arrow.Array{
		"uint64 beyond int64":    build(arrow.PrimitiveTypes.Uint64, func(b array.Builder) { b.(*array.Uint64Builder).Append(math.MaxUint64) }),
		"text that is not UTF-8": build(arrow.BinaryTypes.String, func(b array.Builder) { b.(*array.StringBuilder).Append("a\xffb") }),
		"sub-microsecond":        build(&arrow.TimestampType{Unit: arrow.Nanosecond}, func(b array.Builder) { b.(*array.TimestampBuilder).Append(arrow.Timestamp(1_700_000_000_000_000_001)) }),
		"time past a day":        build(arrow.FixedWidthTypes.Time64us, func(b array.Builder) { b.(*array.Time64Builder).Append(arrow.Time64(25 * 3600 * 1_000_000)) }),
		"negative time":          build(arrow.FixedWidthTypes.Time64us, func(b array.Builder) { b.(*array.Time64Builder).Append(arrow.Time64(-1)) }),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := cell(arr)
			ve, ok := asValueError(err)
			require.True(t, ok, "a cell BigQuery cannot be given is a value error, which refuses its row: %v", err)
			require.Equal(t, "c", ve.column)
		})
	}
}

func TestAnArrowTypeWithNoBigQueryEquivalentFailsTheWholeStream(t *testing.T) {
	for name, dt := range map[string]arrow.DataType{
		"map":        arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64),
		"duration":   &arrow.DurationType{Unit: arrow.Second},
		"dictionary": &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String},
		"nested":     arrow.StructOf(arrow.Field{Name: "d", Type: &arrow.DurationType{Unit: arrow.Second}}),
		"in a list":  arrow.ListOf(&arrow.DurationType{Unit: arrow.Second}),
	} {
		t.Run(name, func(t *testing.T) {
			err := checkArrowSchema(arrow.NewSchema([]arrow.Field{{Name: "c", Type: dt}}, nil))
			var typeErr *arrowTypeError
			require.ErrorAs(t, err, &typeErr)
			require.Equal(t, "c", typeErr.column[:1])
		})
	}
	require.NoError(t, checkArrowSchema(eventSchema))
}
