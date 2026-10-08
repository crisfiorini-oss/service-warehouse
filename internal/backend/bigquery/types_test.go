package bigquery

import (
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

func TestPortableTypeOfEveryBigQueryType(t *testing.T) {
	for in, want := range map[bq.FieldType]backend.ColumnType{
		bq.BooleanFieldType: backend.TypeBool, "BOOL": backend.TypeBool,
		bq.IntegerFieldType: backend.TypeInt64, "INT64": backend.TypeInt64,
		bq.FloatFieldType: backend.TypeFloat64, "FLOAT64": backend.TypeFloat64,
		bq.NumericFieldType: backend.TypeNumeric, bq.BigNumericFieldType: backend.TypeNumeric,
		bq.StringFieldType: backend.TypeString, bq.BytesFieldType: backend.TypeBytes,
		bq.DateFieldType: backend.TypeDate, bq.TimeFieldType: backend.TypeTime,
		// The portable TIMESTAMP is wall-clock; the zoned instant is TIMESTAMPTZ.
		bq.DateTimeFieldType: backend.TypeTimestamp, bq.TimestampFieldType: backend.TypeTimestampTZ,
		bq.IntervalFieldType: backend.TypeInterval, bq.JSONFieldType: backend.TypeJSON,
		bq.RecordFieldType: backend.TypeStruct, "STRUCT": backend.TypeStruct,
		bq.GeographyFieldType: backend.TypeGeography,
		bq.RangeFieldType:     backend.TypeUnknown, "SOMETHING_NEW": backend.TypeUnknown,
	} {
		require.Equal(t, want, portableType(in), string(in))
	}
}

func TestNativeTypeKeepsBigQuerysSpelling(t *testing.T) {
	for name, tc := range map[string]struct {
		field *bq.FieldSchema
		want  string
	}{
		"integer":             {&bq.FieldSchema{Type: bq.IntegerFieldType}, "INT64"},
		"float":               {&bq.FieldSchema{Type: bq.FloatFieldType}, "FLOAT64"},
		"boolean":             {&bq.FieldSchema{Type: bq.BooleanFieldType}, "BOOL"},
		"numeric":             {&bq.FieldSchema{Type: bq.NumericFieldType}, "NUMERIC"},
		"numeric with p,s":    {&bq.FieldSchema{Type: bq.NumericFieldType, Precision: 10, Scale: 2}, "NUMERIC(10, 2)"},
		"numeric with p":      {&bq.FieldSchema{Type: bq.NumericFieldType, Precision: 10}, "NUMERIC(10)"},
		"bignumeric":          {&bq.FieldSchema{Type: bq.BigNumericFieldType, Precision: 40, Scale: 10}, "BIGNUMERIC(40, 10)"},
		"sized string":        {&bq.FieldSchema{Type: bq.StringFieldType, MaxLength: 32}, "STRING(32)"},
		"sized bytes":         {&bq.FieldSchema{Type: bq.BytesFieldType, MaxLength: 8}, "BYTES(8)"},
		"array":               {&bq.FieldSchema{Type: bq.DateFieldType, Repeated: true}, "ARRAY<DATE>"},
		"range":               {&bq.FieldSchema{Type: bq.RangeFieldType, RangeElementType: &bq.RangeElementType{Type: bq.DateTimeFieldType}}, "RANGE<DATETIME>"},
		"struct":              {&bq.FieldSchema{Type: bq.RecordFieldType, Schema: bq.Schema{{Name: "a", Type: bq.IntegerFieldType}, {Name: "b", Type: bq.StringFieldType, Repeated: true}}}, "STRUCT<a INT64, b ARRAY<STRING>>"},
		"array of structs":    {&bq.FieldSchema{Type: bq.RecordFieldType, Repeated: true, Schema: bq.Schema{{Name: "a", Type: bq.IntegerFieldType}}}, "ARRAY<STRUCT<a INT64>>"},
		"a type not yet seen": {&bq.FieldSchema{Type: "NEW_TYPE"}, "NEW_TYPE"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, nativeType(tc.field))
		})
	}
}

func TestColumnsRoundTripThroughBigQueryFields(t *testing.T) {
	cols := []backend.Column{
		{Name: "b", Type: backend.TypeBool},
		{Name: "i", Type: backend.TypeInt64, Nullable: true, Description: "an int"},
		{Name: "f", Type: backend.TypeFloat64, Nullable: true},
		{Name: "n", Type: backend.TypeNumeric, Nullable: true, Precision: 10, Scale: 2},
		{Name: "big", Type: backend.TypeNumeric, Nullable: true, Precision: 76, Scale: 38, NativeType: "BIGNUMERIC"},
		{Name: "s", Type: backend.TypeString, Nullable: true},
		{Name: "y", Type: backend.TypeBytes, Nullable: true},
		{Name: "d", Type: backend.TypeDate, Nullable: true},
		{Name: "t", Type: backend.TypeTime, Nullable: true},
		{Name: "wall", Type: backend.TypeTimestamp, Nullable: true},
		{Name: "at", Type: backend.TypeTimestampTZ, Nullable: true},
		{Name: "iv", Type: backend.TypeInterval, Nullable: true},
		{Name: "j", Type: backend.TypeJSON, Nullable: true},
		{Name: "g", Type: backend.TypeGeography, Nullable: true},
		{Name: "tags", Type: backend.TypeArray, Fields: []backend.Column{{Name: "item", Type: backend.TypeString}}},
		{Name: "owner", Type: backend.TypeStruct, Nullable: true, Fields: []backend.Column{
			{Name: "name", Type: backend.TypeString, Nullable: true},
			{Name: "ids", Type: backend.TypeArray, Fields: []backend.Column{{Name: "item", Type: backend.TypeInt64}}},
		}},
		{Name: "lines", Type: backend.TypeArray, Fields: []backend.Column{{Name: "item", Type: backend.TypeStruct, Fields: []backend.Column{
			{Name: "sku", Type: backend.TypeString, Nullable: true},
		}}}},
	}
	schema, err := schemaFromColumns(cols)
	require.NoError(t, err)

	// Back to portable columns: the types, nullability and shape survive. The
	// native spelling is what BigQuery reports, so it is not part of the round
	// trip's claim.
	back := columnsFromSchema(schema)
	require.Len(t, back, len(cols))
	for i := range cols {
		assertSameShape(t, cols[i], back[i])
	}
}

func assertSameShape(t *testing.T, want, got backend.Column) {
	t.Helper()
	require.Equal(t, want.Name, got.Name)
	require.Equal(t, want.Type, got.Type, want.Name)
	require.Equal(t, want.Nullable, got.Nullable, want.Name)
	require.Equal(t, want.Description, got.Description, want.Name)
	if want.Precision != 0 {
		require.Equal(t, want.Precision, got.Precision, want.Name)
		require.Equal(t, want.Scale, got.Scale, want.Name)
	}
	require.Len(t, got.Fields, len(want.Fields), want.Name)
	for i := range want.Fields {
		assertSameShape(t, want.Fields[i], got.Fields[i])
	}
}

func TestNumericChoosesTheWiderTypeOnlyWhenTheNarrowOneCannotHoldIt(t *testing.T) {
	for name, tc := range map[string]struct {
		col       backend.Column
		wantType  bq.FieldType
		wantP, wS int64
	}{
		"unparameterized":    {backend.Column{Name: "n", Type: backend.TypeNumeric}, bq.NumericFieldType, 0, 0},
		"its own bounds":     {backend.Column{Name: "n", Type: backend.TypeNumeric, Precision: 38, Scale: 9}, bq.NumericFieldType, 0, 0},
		"parameterized":      {backend.Column{Name: "n", Type: backend.TypeNumeric, Precision: 10, Scale: 2}, bq.NumericFieldType, 10, 2},
		"scale past 9":       {backend.Column{Name: "n", Type: backend.TypeNumeric, Precision: 30, Scale: 12}, bq.BigNumericFieldType, 30, 12},
		"precision past 38":  {backend.Column{Name: "n", Type: backend.TypeNumeric, Precision: 50, Scale: 5}, bq.BigNumericFieldType, 50, 5},
		"its own spelling":   {backend.Column{Name: "n", Type: backend.TypeNumeric, NativeType: "BIGNUMERIC"}, bq.BigNumericFieldType, 0, 0},
		"bignumeric default": {backend.Column{Name: "n", Type: backend.TypeNumeric, Precision: 76, Scale: 38, NativeType: "BIGNUMERIC"}, bq.BigNumericFieldType, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			f, err := fieldFromColumn(tc.col)
			require.NoError(t, err)
			require.Equal(t, tc.wantType, f.Type)
			require.Equal(t, tc.wantP, f.Precision)
			require.Equal(t, tc.wS, f.Scale)
		})
	}
}

func TestColumnsBigQueryCannotHoldAreRefusedAndNeverBecomeSomethingElse(t *testing.T) {
	for name, tc := range map[string]struct {
		col  backend.Column
		want serr.Code
	}{
		"unknown type":      {backend.Column{Name: "x", Type: backend.TypeUnknown, NativeType: "RANGE<DATE>"}, serr.Unsupported},
		"array of arrays":   {backend.Column{Name: "x", Type: backend.TypeArray, Fields: []backend.Column{{Name: "item", Type: backend.TypeArray, Fields: []backend.Column{{Type: backend.TypeInt64}}}}}, serr.Unsupported},
		"array w/o element": {backend.Column{Name: "x", Type: backend.TypeArray}, serr.InvalidArgument},
		"struct w/o fields": {backend.Column{Name: "x", Type: backend.TypeStruct}, serr.InvalidArgument},
		"no name":           {backend.Column{Type: backend.TypeInt64}, serr.InvalidArgument},
		"nested unknown":    {backend.Column{Name: "x", Type: backend.TypeStruct, Fields: []backend.Column{{Name: "y", Type: backend.TypeUnknown}}}, serr.Unsupported},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := fieldFromColumn(tc.col)
			require.True(t, serr.Is(err, tc.want), "got %v", err)
		})
	}
}

func TestAResultColumnWithNoPortableTypeCannotBecomeArrow(t *testing.T) {
	_, err := arrowSchema([]backend.Column{{Name: "span", Type: backend.TypeUnknown, NativeType: "RANGE<DATE>"}})
	require.True(t, serr.Is(err, serr.Unsupported))
	require.Contains(t, err.Error(), "span")
	require.Contains(t, err.Error(), "RANGE<DATE>")
}
