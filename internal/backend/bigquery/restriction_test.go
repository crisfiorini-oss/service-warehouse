package bigquery

import (
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// tableSchema has a column of every type a filter may compare, and the ones it
// may not.
var tableSchema = bq.Schema{
	{Name: "id", Type: bq.IntegerFieldType},
	{Name: "score", Type: bq.FloatFieldType},
	{Name: "price", Type: bq.NumericFieldType},
	{Name: "ok", Type: bq.BooleanFieldType},
	{Name: "Name", Type: bq.StringFieldType},
	{Name: "blob", Type: bq.BytesFieldType},
	{Name: "day", Type: bq.DateFieldType},
	{Name: "clock", Type: bq.TimeFieldType},
	{Name: "wall", Type: bq.DateTimeFieldType},
	{Name: "at", Type: bq.TimestampFieldType},
	{Name: "we`ird\\col", Type: bq.StringFieldType},
	{Name: "tags", Type: bq.StringFieldType, Repeated: true},
	{Name: "rec", Type: bq.RecordFieldType, Schema: bq.Schema{{Name: "a", Type: bq.StringFieldType}}},
	{Name: "doc", Type: bq.JSONFieldType},
}

func p(field string, op backend.PredicateOp, typ backend.ColumnType, values ...string) backend.Predicate {
	return backend.Predicate{Field: field, Op: op, Type: typ, Values: values}
}

func TestRestrictionRendersEveryOperatorAndType(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []backend.Predicate
		want string
	}{
		{"no filter", nil, ""},
		{"EQ int", []backend.Predicate{p("id", backend.OpEq, backend.TypeInt64, "42")}, "`id` = 42"},
		{"EQ negative int", []backend.Predicate{p("id", backend.OpEq, backend.TypeInt64, "-9223372036854775808")}, "`id` = -9223372036854775808"},
		{"LT", []backend.Predicate{p("id", backend.OpLt, backend.TypeInt64, "7")}, "`id` < 7"},
		{"LE", []backend.Predicate{p("id", backend.OpLe, backend.TypeInt64, "7")}, "`id` <= 7"},
		{"GT", []backend.Predicate{p("id", backend.OpGt, backend.TypeInt64, "7")}, "`id` > 7"},
		{"GE", []backend.Predicate{p("id", backend.OpGe, backend.TypeInt64, "7")}, "`id` >= 7"},
		{"IN", []backend.Predicate{p("id", backend.OpIn, backend.TypeInt64, "1", "2", "3")}, "`id` IN (1, 2, 3)"},
		{"IS NULL", []backend.Predicate{p("id", backend.OpIsNull, backend.TypeUnknown)}, "`id` IS NULL"},
		{"IS NOT NULL", []backend.Predicate{p("id", backend.OpIsNotNull, backend.TypeUnknown)}, "`id` IS NOT NULL"},
		{"bool", []backend.Predicate{p("ok", backend.OpEq, backend.TypeBool, "TRUE")}, "`ok` = true"},
		{"float", []backend.Predicate{p("score", backend.OpGt, backend.TypeFloat64, "2.5")}, "`score` > 2.5"},
		{"whole float stays a float", []backend.Predicate{p("score", backend.OpGt, backend.TypeFloat64, "3")}, "`score` > 3.0"},
		{"float exponent", []backend.Predicate{p("score", backend.OpLt, backend.TypeFloat64, "1e21")}, "`score` < 1e+21"},
		{"float NaN", []backend.Predicate{p("score", backend.OpEq, backend.TypeFloat64, "NaN")}, "`score` = CAST(\"NaN\" AS FLOAT64)"},
		{"int column, float value", []backend.Predicate{p("id", backend.OpGt, backend.TypeFloat64, "1.5")}, "`id` > 1.5"},
		{"numeric", []backend.Predicate{p("price", backend.OpEq, backend.TypeNumeric, "12.50")}, "`price` = CAST(\"12.50\" AS NUMERIC)"},
		{"numeric too wide for NUMERIC", []backend.Predicate{p("price", backend.OpEq, backend.TypeNumeric, "1."+"1234567890123456789012345678901234567890"[:35])}, "`price` = CAST(\"1.12345678901234567890123456789012345\" AS BIGNUMERIC)"},
		{"date", []backend.Predicate{p("day", backend.OpGe, backend.TypeDate, "2024-02-29")}, "`day` >= CAST(\"2024-02-29\" AS DATE)"},
		{"time", []backend.Predicate{p("clock", backend.OpLt, backend.TypeTime, "13:04:05.25")}, "`clock` < CAST(\"13:04:05.250000\" AS TIME)"},
		{"wall-clock datetime", []backend.Predicate{p("wall", backend.OpEq, backend.TypeTimestamp, "2024-01-02T03:04:05.123456")}, "`wall` = CAST(\"2024-01-02 03:04:05.123456\" AS DATETIME)"},
		{"instant in UTC", []backend.Predicate{p("at", backend.OpGe, backend.TypeTimestampTZ, "2024-01-02T03:04:05Z")}, "`at` >= CAST(\"2024-01-02 03:04:05+00:00\" AS TIMESTAMP)"},
		{"instant in another zone is written in UTC", []backend.Predicate{p("at", backend.OpLt, backend.TypeTimestampTZ, "2024-01-02T03:04:05.5+05:30")}, "`at` < CAST(\"2024-01-01 21:34:05.5+00:00\" AS TIMESTAMP)"},
		{"bytes", []backend.Predicate{p("blob", backend.OpEq, backend.TypeBytes, "AAEC/w==")}, "`blob` = b\"\\x00\\x01\\x02\\xFF\""},
		{"empty bytes", []backend.Predicate{p("blob", backend.OpEq, backend.TypeBytes, "")}, "`blob` = b\"\""},
		{"conjunction", []backend.Predicate{
			p("id", backend.OpGe, backend.TypeInt64, "1"),
			p("Name", backend.OpEq, backend.TypeString, "x"),
			p("at", backend.OpIsNotNull, backend.TypeUnknown),
		}, "`id` >= 1 AND `Name` = \"x\" AND `at` IS NOT NULL"},
		{"a column in another case is written as the table spells it", []backend.Predicate{p("NAME", backend.OpEq, backend.TypeString, "x")}, "`Name` = \"x\""},
		{"a column name with a backtick and a backslash", []backend.Predicate{p("we`ird\\col", backend.OpIsNull, backend.TypeUnknown)}, "`we\\`ird\\\\col` IS NULL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := restriction(tableSchema, tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// A string value is escaped so that no value can end its literal: a quote and a
// backslash are escaped and every other non-printable character is a \U escape.
func TestRestrictionEscapesStringValues(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  string
	}{
		{"plain", `"plain"`},
		{"", `""`},
		{`say "hi"`, `"say \"hi\""`},
		{`it's`, `"it's"`},
		{`back\slash`, `"back\\slash"`},
		{`\"`, `"\\\""`},
		{`" OR 1=1 --`, `"\" OR 1=1 --"`},
		{`"; DROP TABLE t; --`, `"\"; DROP TABLE t; --"`},
		{"line\nbreak", `"line\U0000000Abreak"`},
		{"tab\tand\rcr", `"tab\U00000009and\U0000000Dcr"`},
		{"\x7f", `"\U0000007F"`},
		{"é", `"\U000000E9"`},
		{"日本", `"\U000065E5\U0000672C"`},
		{"😀", `"\U0001F600"`},
		{" ", `"\U00002028"`},
	} {
		got, err := restriction(tableSchema, []backend.Predicate{p("Name", backend.OpEq, backend.TypeString, tc.value)})
		require.NoError(t, err, "%q", tc.value)
		require.Equal(t, "`Name` = "+tc.want, got, "%q", tc.value)
	}

	got, err := restriction(tableSchema, []backend.Predicate{p("Name", backend.OpIn, backend.TypeString, `a"b`, `c\d`)})
	require.NoError(t, err)
	require.Equal(t, "`Name` IN (\"a\\\"b\", \"c\\\\d\")", got)
}

func TestRestrictionRefusesWhatItCannotSayExactly(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   backend.Predicate
		want serr.Code
	}{
		{"unknown column", p("nope", backend.OpEq, backend.TypeInt64, "1"), serr.InvalidArgument},
		{"unknown column with IS NULL", p("nope", backend.OpIsNull, backend.TypeUnknown), serr.InvalidArgument},
		{"a nested name is not a column", p("rec.a", backend.OpEq, backend.TypeString, "x"), serr.InvalidArgument},
		{"a repeated column", p("tags", backend.OpEq, backend.TypeString, "x"), serr.Unsupported},
		{"a repeated column with IS NULL", p("tags", backend.OpIsNull, backend.TypeUnknown), serr.Unsupported},
		{"a record column", p("rec", backend.OpIsNull, backend.TypeUnknown), serr.Unsupported},
		{"a JSON column", p("doc", backend.OpIsNull, backend.TypeUnknown), serr.Unsupported},
		{"a string compared with an integer", p("Name", backend.OpEq, backend.TypeInt64, "1"), serr.InvalidArgument},
		{"an integer compared with a string", p("id", backend.OpEq, backend.TypeString, "1"), serr.InvalidArgument},
		{"a TIMESTAMP compared with a DATETIME value", p("at", backend.OpEq, backend.TypeTimestamp, "2024-01-01T00:00:00"), serr.InvalidArgument},
		{"a DATETIME compared with an instant", p("wall", backend.OpEq, backend.TypeTimestampTZ, "2024-01-01T00:00:00Z"), serr.InvalidArgument},
		{"a DATE compared with a string", p("day", backend.OpEq, backend.TypeString, "2024-01-01"), serr.InvalidArgument},
		{"a malformed integer", p("id", backend.OpEq, backend.TypeInt64, "1 OR 1=1"), serr.InvalidArgument},
		{"a malformed date", p("day", backend.OpEq, backend.TypeDate, "2024-13-01"), serr.InvalidArgument},
		{"a wall-clock value with a zone", p("wall", backend.OpEq, backend.TypeTimestamp, "2024-01-01T00:00:00Z"), serr.InvalidArgument},
		{"an instant with no zone", p("at", backend.OpEq, backend.TypeTimestampTZ, "2024-01-01T00:00:00"), serr.InvalidArgument},
		{"a sub-microsecond instant", p("at", backend.OpEq, backend.TypeTimestampTZ, "2024-01-01T00:00:00.0000001Z"), serr.InvalidArgument},
		{"a decimal that would be rounded", p("price", backend.OpEq, backend.TypeNumeric, "0."+"1234567890123456789012345678901234567890123456789"), serr.InvalidArgument},
		{"bytes that are not base64", p("blob", backend.OpEq, backend.TypeBytes, "not base64!"), serr.InvalidArgument},
		{"text that is not UTF-8", p("Name", backend.OpEq, backend.TypeString, "a\xffb"), serr.InvalidArgument},
		{"a NUL character", p("Name", backend.OpEq, backend.TypeString, "a\x00b"), serr.InvalidArgument},
		{"an operator that is none", backend.Predicate{Field: "id", Type: backend.TypeInt64, Values: []string{"1"}}, serr.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := restriction(tableSchema, []backend.Predicate{tc.in})
			require.Error(t, err)
			require.Equal(t, tc.want, serr.CodeOf(err), "%v", err)
		})
	}
}
