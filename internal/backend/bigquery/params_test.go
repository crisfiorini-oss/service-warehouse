package bigquery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

func TestParameterValuesAreCheckedAndNormalized(t *testing.T) {
	for name, tc := range map[string]struct {
		param backend.QueryParam
		kind  string
		text  string
	}{
		"bool":                {backend.QueryParam{Type: backend.TypeBool, Value: "TRUE"}, "BOOL", "true"},
		"bool short":          {backend.QueryParam{Type: backend.TypeBool, Value: "f"}, "BOOL", "false"},
		"int":                 {backend.QueryParam{Type: backend.TypeInt64, Value: "-9223372036854775808"}, "INT64", "-9223372036854775808"},
		"float":               {backend.QueryParam{Type: backend.TypeFloat64, Value: "1.50"}, "FLOAT64", "1.5"},
		"float exponent":      {backend.QueryParam{Type: backend.TypeFloat64, Value: "1e3"}, "FLOAT64", "1000"},
		"float nan":           {backend.QueryParam{Type: backend.TypeFloat64, Value: "NaN"}, "FLOAT64", "NaN"},
		"float +inf":          {backend.QueryParam{Type: backend.TypeFloat64, Value: "+Inf"}, "FLOAT64", "Infinity"},
		"float -inf":          {backend.QueryParam{Type: backend.TypeFloat64, Value: "-Inf"}, "FLOAT64", "-Infinity"},
		"numeric":             {backend.QueryParam{Type: backend.TypeNumeric, Value: "+12.345"}, "NUMERIC", "12.345"},
		"numeric at the edge": {backend.QueryParam{Type: backend.TypeNumeric, Value: strings.Repeat("9", 29) + "." + strings.Repeat("9", 9)}, "NUMERIC", strings.Repeat("9", 29) + "." + strings.Repeat("9", 9)},
		"numeric leading 0s":  {backend.QueryParam{Type: backend.TypeNumeric, Value: "000" + strings.Repeat("9", 29)}, "NUMERIC", "000" + strings.Repeat("9", 29)},
		"numeric needing big": {backend.QueryParam{Type: backend.TypeNumeric, Value: "1." + strings.Repeat("1", 20)}, "BIGNUMERIC", "1." + strings.Repeat("1", 20)},
		"numeric wide":        {backend.QueryParam{Type: backend.TypeNumeric, Value: strings.Repeat("9", 38)}, "BIGNUMERIC", strings.Repeat("9", 38)},
		"string":              {backend.QueryParam{Type: backend.TypeString, Value: "it's a ' \" -- ; DROP"}, "STRING", "it's a ' \" -- ; DROP"},
		"empty string":        {backend.QueryParam{Type: backend.TypeString, Value: ""}, "STRING", ""},
		"bytes":               {backend.QueryParam{Type: backend.TypeBytes, Value: "AAEC/w=="}, "BYTES", "AAEC/w=="},
		"date":                {backend.QueryParam{Type: backend.TypeDate, Value: "2024-02-29"}, "DATE", "2024-02-29"},
		"time":                {backend.QueryParam{Type: backend.TypeTime, Value: "13:04:05.5"}, "TIME", "13:04:05.500000"},
		"wall clock":          {backend.QueryParam{Type: backend.TypeTimestamp, Value: "2024-02-29 13:04:05"}, "DATETIME", "2024-02-29 13:04:05"},
		"wall clock T":        {backend.QueryParam{Type: backend.TypeTimestamp, Value: "2024-02-29T13:04:05.25"}, "DATETIME", "2024-02-29 13:04:05.250000"},
		"instant":             {backend.QueryParam{Type: backend.TypeTimestampTZ, Value: "2024-02-29T13:04:05.5+02:00"}, "TIMESTAMP", "2024-02-29 11:04:05.5+00:00"},
		"instant spaced":      {backend.QueryParam{Type: backend.TypeTimestampTZ, Value: "2024-02-29 13:04:05Z"}, "TIMESTAMP", "2024-02-29 13:04:05+00:00"},
		"interval":            {backend.QueryParam{Type: backend.TypeInterval, Value: "1-2 3 4:5:6"}, "INTERVAL", "1-2 3 4:5:6"},
		"json":                {backend.QueryParam{Type: backend.TypeJSON, Value: `{"a":[1,2]}`}, "JSON", `{"a":[1,2]}`},
		"geography":           {backend.QueryParam{Type: backend.TypeGeography, Value: "POINT(1 2)"}, "GEOGRAPHY", "POINT(1 2)"},
		"null keeps its type": {backend.QueryParam{Type: backend.TypeDate, IsNull: true, Value: "ignored"}, "DATE", ""},
	} {
		t.Run(name, func(t *testing.T) {
			kind, text, err := paramValue(tc.param)
			require.NoError(t, err)
			require.Equal(t, tc.kind, kind)
			require.Equal(t, tc.text, text)
		})
	}
}

func TestMalformedParameterValuesAreInvalidArguments(t *testing.T) {
	for name, p := range map[string]backend.QueryParam{
		"bool":             {Type: backend.TypeBool, Value: "yes"},
		"int":              {Type: backend.TypeInt64, Value: "1.5"},
		"int overflow":     {Type: backend.TypeInt64, Value: "9223372036854775808"},
		"float":            {Type: backend.TypeFloat64, Value: "one"},
		"numeric":          {Type: backend.TypeNumeric, Value: "1e5"},
		"numeric rounding": {Type: backend.TypeNumeric, Value: "1." + strings.Repeat("1", 39)},
		"numeric too big":  {Type: backend.TypeNumeric, Value: strings.Repeat("9", 40)},
		"string not UTF-8": {Type: backend.TypeString, Value: "a\xffb"},
		"bytes":            {Type: backend.TypeBytes, Value: "not base64!"},
		"date":             {Type: backend.TypeDate, Value: "29/02/2024"},
		"time":             {Type: backend.TypeTime, Value: "25:00:00"},
		"time nanoseconds": {Type: backend.TypeTime, Value: "13:04:05.123456789"},
		"wall clock":       {Type: backend.TypeTimestamp, Value: "yesterday"},
		"instant no zone":  {Type: backend.TypeTimestampTZ, Value: "2024-02-29T13:04:05"},
		"instant ns":       {Type: backend.TypeTimestampTZ, Value: "2024-02-29T13:04:05.123456789Z"},
		"interval":         {Type: backend.TypeInterval, Value: "a while"},
		"json":             {Type: backend.TypeJSON, Value: "{not json"},
		"geography":        {Type: backend.TypeGeography, Value: "  "},
		"no type":          {Value: "1"},
	} {
		t.Run(name, func(t *testing.T) {
			p.Name = "p"
			_, err := bindParams([]backend.QueryParam{p})
			require.True(t, serr.Is(err, serr.InvalidArgument), "got %v", err)
			require.Contains(t, err.Error(), `"p"`, "the error names the parameter")
		})
	}
}

func TestArrayAndStructParametersAreUnsupportedNotMisbound(t *testing.T) {
	for _, typ := range []backend.ColumnType{backend.TypeArray, backend.TypeStruct} {
		_, err := bindParams([]backend.QueryParam{{Name: "p", Type: typ, Value: "[1,2]"}})
		require.True(t, serr.Is(err, serr.Unsupported), "got %v", err)
	}
}

func TestParameterNames(t *testing.T) {
	got, err := bindParams([]backend.QueryParam{
		{Name: "plain", Type: backend.TypeInt64, Value: "1"},
		{Name: "@at", Type: backend.TypeInt64, Value: "2"},
		{Name: ":colon", Type: backend.TypeInt64, Value: "3"},
		{Name: "$dollar", Type: backend.TypeInt64, Value: "4"},
		{Name: "_u1", Type: backend.TypeInt64, Value: "5"},
	})
	require.NoError(t, err)
	names := make([]string, len(got))
	for i, p := range got {
		names[i] = p.Name
	}
	require.Equal(t, []string{"plain", "at", "colon", "dollar", "_u1"}, names)

	for _, bad := range []string{"", "@", "1a", "a b", "a-b", "a;b", "@@x", "x'", "é"} {
		_, err := bindParams([]backend.QueryParam{{Name: bad, Type: backend.TypeInt64, Value: "1"}})
		require.True(t, serr.Is(err, serr.InvalidArgument), "%q: %v", bad, err)
	}
	_, err = bindParams([]backend.QueryParam{
		{Name: "Dup", Type: backend.TypeInt64, Value: "1"}, {Name: "dup", Type: backend.TypeInt64, Value: "2"},
	})
	require.True(t, serr.Is(err, serr.InvalidArgument), "BigQuery matches names case-insensitively: %v", err)
}

func TestNoParametersIsNoParameters(t *testing.T) {
	got, err := bindParams(nil)
	require.NoError(t, err)
	require.Empty(t, got)
}
