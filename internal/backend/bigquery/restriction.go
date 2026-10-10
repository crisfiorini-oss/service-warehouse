package bigquery

import (
	"encoding/base64"
	"fmt"
	"strings"
	"unicode/utf8"

	bq "cloud.google.com/go/bigquery"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// A Scan's filter becomes the row restriction of a Storage Read API session: a
// GoogleSQL expression BigQuery evaluates while it plans and reads the streams,
// so it prunes partitions and blocks before any row is sent. It is built from
// parts this package spells itself, never from request text: a column is named
// by the table's own spelling of it, quoted, and a value is parsed as the type
// the predicate declares and then written as a literal of that type, so nothing
// a value contains can end it or add to the expression.
//
// The rendering was modelled on the audit store's reader in codefly-dev/
// module-saas-starter (auditstore/bigquerystore/restriction.go), which sends the
// same kind of restriction to the same API.

// scalarColumn is whether a filter may name a column of this type: the scalar
// types the portable contract has a literal for.
func scalarColumn(f *bq.FieldSchema) bool {
	if f.Repeated {
		return false
	}
	switch fieldType(f.Type) {
	case bq.BooleanFieldType, bq.IntegerFieldType, bq.FloatFieldType, bq.NumericFieldType, bq.BigNumericFieldType,
		bq.StringFieldType, bq.BytesFieldType, bq.DateFieldType, bq.TimeFieldType, bq.DateTimeFieldType, bq.TimestampFieldType:
		return true
	}
	return false
}

// comparable reports whether a value of the declared type can be compared with a
// column of this BigQuery type. The numbers compare with each other; every other
// type compares only with itself, as BigQuery's own operators do (a DATETIME is
// not a TIMESTAMP, and a string is not a number).
func comparable(f *bq.FieldSchema, declared backend.ColumnType) bool {
	column := portableType(f.Type)
	if numeric(column) && numeric(declared) {
		return true
	}
	return column == declared
}

func numeric(t backend.ColumnType) bool {
	return t == backend.TypeInt64 || t == backend.TypeFloat64 || t == backend.TypeNumeric
}

// restriction renders the filter over the table's schema. A predicate on a column
// the table lacks is InvalidArgument; one on a column no literal can be compared
// with, or with a value of an incomparable type, is refused too. The result is ""
// for an empty filter.
func restriction(schema bq.Schema, filter []backend.Predicate) (string, error) {
	const op = "Scan"
	terms := make([]string, 0, len(filter))
	for _, p := range filter {
		field := findField(schema, p.Field)
		if field == nil {
			return "", serr.New(serr.InvalidArgument, op, fmt.Sprintf("the table has no column %q", p.Field))
		}
		if !scalarColumn(field) {
			return "", serr.New(serr.Unsupported, op, fmt.Sprintf(
				"column %q is %s, and a filter can name only a column of a scalar type (BOOL, INT64, FLOAT64, NUMERIC, STRING, BYTES, DATE, TIME, DATETIME, TIMESTAMP)",
				field.Name, nativeType(field)))
		}
		name, err := quoteColumn(field.Name)
		if err != nil {
			return "", err
		}
		switch p.Op {
		case backend.OpIsNull:
			terms = append(terms, name+" IS NULL")
			continue
		case backend.OpIsNotNull:
			terms = append(terms, name+" IS NOT NULL")
			continue
		}
		if !comparable(field, p.Type) {
			return "", serr.New(serr.InvalidArgument, op, fmt.Sprintf(
				"column %q is %s, so a %s value cannot be compared with it", field.Name, nativeType(field), backend.TypeName(p.Type)))
		}
		literals := make([]string, len(p.Values))
		for i, v := range p.Values {
			if literals[i], err = literal(field.Name, p.Type, v); err != nil {
				return "", err
			}
		}
		switch p.Op {
		case backend.OpIn:
			terms = append(terms, name+" IN ("+strings.Join(literals, ", ")+")")
		case backend.OpEq:
			terms = append(terms, name+" = "+literals[0])
		case backend.OpLt:
			terms = append(terms, name+" < "+literals[0])
		case backend.OpLe:
			terms = append(terms, name+" <= "+literals[0])
		case backend.OpGt:
			terms = append(terms, name+" > "+literals[0])
		case backend.OpGe:
			terms = append(terms, name+" >= "+literals[0])
		default:
			return "", serr.New(serr.InvalidArgument, op, "a predicate has no operator")
		}
	}
	return strings.Join(terms, " AND "), nil
}

// findField is the top-level column a name addresses, matched without regard to
// case as BigQuery matches column names, or nil.
func findField(schema bq.Schema, name string) *bq.FieldSchema {
	for _, f := range schema {
		if strings.EqualFold(f.Name, name) {
			return f
		}
	}
	return nil
}

// quoteColumn spells a column name as a GoogleSQL quoted identifier. The name is
// the table's own, so it is what BigQuery accepted for a column; what quoting
// cannot carry intact (a control character, invalid UTF-8) is refused.
func quoteColumn(name string) (string, error) {
	if name == "" || !utf8.ValidString(name) {
		return "", serr.New(serr.InvalidArgument, "Scan", "a column name cannot be written as an identifier")
	}
	var b strings.Builder
	b.WriteByte('`')
	for _, r := range name {
		switch {
		case r == '`' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r < 0x20 || r == 0x7f:
			return "", serr.New(serr.InvalidArgument, "Scan", "a column name cannot be written as an identifier")
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('`')
	return b.String(), nil
}

// literal writes value as a GoogleSQL literal of the declared type. The value is
// parsed first, by the same rules as a query parameter of that type, so a
// malformed one is InvalidArgument naming the column and nothing reaches the
// expression unparsed. Dates, times and timestamps are written as casts of their
// normalized text, and an instant carries an explicit +00:00, so none is read in
// a time zone the caller did not name.
func literal(column string, t backend.ColumnType, value string) (string, error) {
	kind, text, err := paramValue(backend.QueryParam{Type: t, Value: value})
	if err != nil {
		return "", serr.New(serr.InvalidArgument, "Scan", fmt.Sprintf("column %q: %v", column, err))
	}
	switch t {
	case backend.TypeBool, backend.TypeInt64:
		return text, nil
	case backend.TypeFloat64:
		switch text {
		case "NaN", "Infinity", "-Infinity":
			return `CAST("` + text + `" AS FLOAT64)`, nil
		}
		if !strings.ContainsAny(text, ".eE") {
			text += ".0"
		}
		return text, nil
	case backend.TypeString:
		return stringLiteral(column, text)
	case backend.TypeBytes:
		raw, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return "", serr.New(serr.InvalidArgument, "Scan", fmt.Sprintf("column %q: bytes are given as standard base64 text", column))
		}
		return bytesLiteral(raw), nil
	}
	// NUMERIC or BIGNUMERIC (whichever holds the value without rounding), DATE,
	// TIME, DATETIME, TIMESTAMP: the text is already normalized and plain.
	return "CAST(\"" + text + "\" AS " + kind + ")", nil
}

// stringLiteral quotes value as a GoogleSQL string literal: printable ASCII as
// itself, a quote and a backslash escaped, every other code point as a \U escape,
// so no value can end the literal or carry a raw control character. A NUL is
// refused: no rendering of it is worth trusting in an expression.
func stringLiteral(column, value string) (string, error) {
	if !utf8.ValidString(value) {
		return "", serr.New(serr.InvalidArgument, "Scan", fmt.Sprintf("column %q: the value is not valid UTF-8", column))
	}
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range value {
		switch {
		case r == 0:
			return "", serr.New(serr.InvalidArgument, "Scan", fmt.Sprintf("column %q: the value holds a NUL character", column))
		case r == '"' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r >= 0x20 && r < 0x7f:
			b.WriteRune(r)
		default:
			fmt.Fprintf(&b, `\U%08X`, r)
		}
	}
	b.WriteByte('"')
	return b.String(), nil
}

// bytesLiteral writes raw as a GoogleSQL bytes literal, every byte escaped.
func bytesLiteral(raw []byte) string {
	var b strings.Builder
	b.WriteString(`b"`)
	for _, c := range raw {
		fmt.Fprintf(&b, `\x%02X`, c)
	}
	b.WriteByte('"')
	return b.String()
}
