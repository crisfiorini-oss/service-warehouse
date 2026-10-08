package bigquery

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// Parameters arrive as text plus a portable type, and are sent to BigQuery as
// that text with an explicit parameter type, so BigQuery parses and binds the
// value and nothing is ever spliced into the SQL. The text is checked here first,
// for what is cheap to check, so a malformed value is an invalid argument naming
// the parameter instead of a failed job.
//
// The SQL itself uses BigQuery's named-parameter syntax, @name. A parameter's
// name is given without the sigil; one leading @, : or $ is accepted and dropped.

// errNotBindable marks a parameter type that has no scalar text form.
var errNotBindable = errors.New("not bindable from text")

var paramName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// decimalText is a plain decimal number: a sign, digits, and optional fraction.
var decimalText = regexp.MustCompile(`^([+-]?)([0-9]+)(?:\.([0-9]+))?$`)

// Layouts for text that is parsed here and then sent in BigQuery's own form.
const (
	timestampParamLayout = "2006-01-02 15:04:05.999999-07:00"
	maxNumericInt        = numericPrecision - numericScale
	maxBigNumericInt     = bigNumericPrecision - bigNumericScale
)

// bindParams builds the query parameters of a request.
func bindParams(params []backend.QueryParam) ([]bq.QueryParameter, error) {
	out := make([]bq.QueryParameter, 0, len(params))
	seen := make(map[string]bool, len(params))
	for _, p := range params {
		name := p.Name
		if name != "" && strings.ContainsAny(name[:1], "@:$") {
			name = name[1:]
		}
		if !paramName.MatchString(name) {
			return nil, serr.New(serr.InvalidArgument, "Query",
				fmt.Sprintf("%q is not a valid parameter name (letters, digits and underscores, not starting with a digit)", p.Name))
		}
		// BigQuery matches names case-insensitively.
		key := strings.ToLower(name)
		if seen[key] {
			return nil, serr.New(serr.InvalidArgument, "Query", fmt.Sprintf("parameter %q is given twice", name))
		}
		seen[key] = true
		kind, text, err := paramValue(p)
		if errors.Is(err, errNotBindable) {
			return nil, serr.New(serr.Unsupported, "Query",
				fmt.Sprintf("parameter %q: an array or struct cannot be bound from a text value", name))
		}
		if err != nil {
			return nil, serr.New(serr.InvalidArgument, "Query", fmt.Sprintf("parameter %q: %v", name, err))
		}
		value := &bq.QueryParameterValue{Type: bq.StandardSQLDataType{TypeKind: kind}}
		if p.IsNull {
			// A typed NULL: the type is declared, the value is absent.
			value.Value = bq.NullString{}
		} else {
			value.Value = text
		}
		out = append(out, bq.QueryParameter{Name: name, Value: value})
	}
	return out, nil
}

// paramValue returns BigQuery's type name for the parameter and its text,
// normalized. A null parameter returns only the type.
func paramValue(p backend.QueryParam) (kind, text string, err error) {
	switch p.Type {
	case backend.TypeBool:
		kind = "BOOL"
	case backend.TypeInt64:
		kind = "INT64"
	case backend.TypeFloat64:
		kind = "FLOAT64"
	case backend.TypeNumeric:
		kind = "NUMERIC"
	case backend.TypeString:
		kind = "STRING"
	case backend.TypeBytes:
		kind = "BYTES"
	case backend.TypeDate:
		kind = "DATE"
	case backend.TypeTime:
		kind = "TIME"
	case backend.TypeTimestamp:
		kind = "DATETIME"
	case backend.TypeTimestampTZ:
		kind = "TIMESTAMP"
	case backend.TypeInterval:
		kind = "INTERVAL"
	case backend.TypeJSON:
		kind = "JSON"
	case backend.TypeGeography:
		kind = "GEOGRAPHY"
	case backend.TypeArray, backend.TypeStruct:
		return "", "", errNotBindable
	default:
		return "", "", fmt.Errorf("has no type; give it one of the scalar types")
	}
	if p.IsNull {
		return kind, "", nil
	}
	v := p.Value
	switch p.Type {
	case backend.TypeBool:
		b, err := strconv.ParseBool(v)
		if err != nil {
			return "", "", fmt.Errorf("%q is not a boolean", v)
		}
		return kind, strconv.FormatBool(b), nil
	case backend.TypeInt64:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", "", fmt.Errorf("%q is not a 64-bit integer", v)
		}
		return kind, strconv.FormatInt(n, 10), nil
	case backend.TypeFloat64:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return "", "", fmt.Errorf("%q is not a floating-point number", v)
		}
		switch {
		case math.IsNaN(f):
			return kind, "NaN", nil
		case math.IsInf(f, 1):
			return kind, "Infinity", nil
		case math.IsInf(f, -1):
			return kind, "-Infinity", nil
		}
		return kind, strconv.FormatFloat(f, 'g', -1, 64), nil
	case backend.TypeNumeric:
		return decimalParam(v)
	case backend.TypeString:
		if !utf8.ValidString(v) {
			return "", "", fmt.Errorf("the text is not valid UTF-8")
		}
		return kind, v, nil
	case backend.TypeBytes:
		raw, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return "", "", fmt.Errorf("bytes are given as standard base64 text")
		}
		return kind, base64.StdEncoding.EncodeToString(raw), nil
	case backend.TypeDate:
		d, err := civil.ParseDate(v)
		if err != nil {
			return "", "", fmt.Errorf("%q is not a date (YYYY-MM-DD)", v)
		}
		return kind, d.String(), nil
	case backend.TypeTime:
		t, err := civil.ParseTime(v)
		if err != nil {
			return "", "", fmt.Errorf("%q is not a time (HH:MM:SS[.ffffff])", v)
		}
		if t.Nanosecond%1000 != 0 {
			return "", "", fmt.Errorf("BigQuery times have microsecond precision")
		}
		return kind, bq.CivilTimeString(t), nil
	case backend.TypeTimestamp:
		// civil reads only the T separator; BigQuery's own text uses a space.
		dt, err := civil.ParseDateTime(strings.Replace(v, " ", "T", 1))
		if err != nil {
			return "", "", fmt.Errorf("%q is not a wall-clock timestamp (YYYY-MM-DDTHH:MM:SS[.ffffff])", v)
		}
		if dt.Time.Nanosecond%1000 != 0 {
			return "", "", fmt.Errorf("BigQuery timestamps have microsecond precision")
		}
		return kind, bq.CivilDateTimeString(dt), nil
	case backend.TypeTimestampTZ:
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t, err = time.Parse("2006-01-02 15:04:05.999999999Z07:00", v)
		}
		if err != nil {
			return "", "", fmt.Errorf("%q is not an instant (RFC 3339, with a zone)", v)
		}
		if t.Nanosecond()%1000 != 0 {
			return "", "", fmt.Errorf("BigQuery timestamps have microsecond precision")
		}
		return kind, t.UTC().Format(timestampParamLayout), nil
	case backend.TypeInterval:
		iv, err := bq.ParseInterval(v)
		if err != nil {
			return "", "", fmt.Errorf("%q is not an interval (Y-M D H:M:S[.F])", v)
		}
		return kind, iv.String(), nil
	case backend.TypeJSON:
		if !json.Valid([]byte(v)) {
			return "", "", fmt.Errorf("the text is not valid JSON")
		}
		return kind, v, nil
	case backend.TypeGeography:
		if strings.TrimSpace(v) == "" {
			return "", "", fmt.Errorf("the geography is empty")
		}
		return kind, v, nil
	}
	return "", "", fmt.Errorf("unreachable parameter type")
}

// decimalParam picks NUMERIC when the value fits it and BIGNUMERIC when only that
// fits, and refuses a value that fits neither: BigQuery would round it, and a
// bound value is never rounded.
func decimalParam(v string) (string, string, error) {
	m := decimalText.FindStringSubmatch(v)
	if m == nil {
		return "", "", fmt.Errorf("%q is not a decimal number", v)
	}
	whole := strings.TrimLeft(m[2], "0")
	fraction := m[3]
	switch {
	case len(whole) <= maxNumericInt && len(fraction) <= numericScale:
		return "NUMERIC", strings.TrimPrefix(v, "+"), nil
	case len(whole) <= maxBigNumericInt && len(fraction) <= bigNumericScale:
		return "BIGNUMERIC", strings.TrimPrefix(v, "+"), nil
	}
	return "", "", fmt.Errorf("%q does not fit NUMERIC or BIGNUMERIC without rounding", v)
}
