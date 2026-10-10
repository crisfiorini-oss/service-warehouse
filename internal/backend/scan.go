package backend

import (
	"fmt"
	"strings"

	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// PredicateOp is the comparison of a Predicate.
type PredicateOp int

// The zero value is deliberately not an operator, so a predicate nobody filled
// in is refused instead of read as equality.
const (
	OpInvalid PredicateOp = iota
	OpEq
	OpIn
	OpLt
	OpLe
	OpGt
	OpGe
	OpIsNull
	OpIsNotNull
)

// takesValues reports whether the operator compares against values.
func (o PredicateOp) takesValues() bool {
	switch o {
	case OpEq, OpIn, OpLt, OpLe, OpGt, OpGe:
		return true
	}
	return false
}

// Predicate is one comparison of a column with typed values. Values are the
// text form QueryParam.Value takes for Type. It is data a backend renders or
// binds itself, never SQL.
type Predicate struct {
	Field  string
	Op     PredicateOp
	Type   ColumnType
	Values []string
}

// ScanOptions is what a Scan reads: the columns, and a conjunction of
// predicates that every returned row satisfies.
type ScanOptions struct {
	// Selected are the columns to return, in this order; empty is every column in
	// the table's order.
	Selected []string
	// Filter is ANDed; empty reads every row.
	Filter []Predicate
}

// ValidateScan checks what no backend may accept: a predicate with no field or
// operator, the wrong number of values for its operator, a type missing where
// values are given (or present where there are none), an unbindable type, and a
// column selected twice. Whether a column exists is the backend's to say, as
// only it can read the table. Every failure is InvalidArgument, except a type
// that no text value can carry, which is Unsupported.
func ValidateScan(op string, opts ScanOptions) error {
	seen := make(map[string]bool, len(opts.Selected))
	for _, name := range opts.Selected {
		if name == "" {
			return serr.New(serr.InvalidArgument, op, "a selected field is empty")
		}
		key := strings.ToLower(name)
		if seen[key] {
			return serr.New(serr.InvalidArgument, op, fmt.Sprintf("field %q is selected twice", name))
		}
		seen[key] = true
	}
	for i, p := range opts.Filter {
		if err := p.validate(op, i); err != nil {
			return err
		}
	}
	return nil
}

func (p Predicate) validate(op string, i int) error {
	bad := func(format string, args ...any) error {
		return serr.New(serr.InvalidArgument, op, fmt.Sprintf("predicate %d: ", i)+fmt.Sprintf(format, args...))
	}
	if p.Field == "" {
		return bad("it names no field")
	}
	if p.Op == OpInvalid || p.Op > OpIsNotNull {
		return bad("it has no operator")
	}
	if !p.Op.takesValues() {
		if len(p.Values) != 0 {
			return bad("%s on %q takes no values", p.Op, p.Field)
		}
		if p.Type != TypeUnknown {
			return bad("%s on %q takes no type", p.Op, p.Field)
		}
		return nil
	}
	switch {
	case len(p.Values) == 0:
		return bad("%s on %q needs a value", p.Op, p.Field)
	case p.Op != OpIn && len(p.Values) != 1:
		return bad("%s on %q takes exactly one value, got %d", p.Op, p.Field, len(p.Values))
	}
	switch p.Type {
	case TypeBool, TypeInt64, TypeFloat64, TypeNumeric, TypeString, TypeBytes,
		TypeDate, TypeTime, TypeTimestamp, TypeTimestampTZ:
		return nil
	case TypeArray, TypeStruct:
		return serr.New(serr.Unsupported, op, fmt.Sprintf("predicate %d: a filter cannot compare an ARRAY or STRUCT column", i))
	case TypeInterval, TypeJSON, TypeGeography:
		return serr.New(serr.Unsupported, op, fmt.Sprintf("predicate %d: a filter cannot compare %s values", i, TypeName(p.Type)))
	}
	return bad("%s on %q needs a type for its values", p.Op, p.Field)
}

// TypeName is the portable name of a type, as the contract spells it.
func TypeName(t ColumnType) string {
	switch t {
	case TypeBool:
		return "BOOL"
	case TypeInt64:
		return "INT64"
	case TypeFloat64:
		return "FLOAT64"
	case TypeNumeric:
		return "NUMERIC"
	case TypeString:
		return "STRING"
	case TypeBytes:
		return "BYTES"
	case TypeDate:
		return "DATE"
	case TypeTime:
		return "TIME"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeTimestampTZ:
		return "TIMESTAMPTZ"
	case TypeInterval:
		return "INTERVAL"
	case TypeJSON:
		return "JSON"
	case TypeArray:
		return "ARRAY"
	case TypeStruct:
		return "STRUCT"
	case TypeGeography:
		return "GEOGRAPHY"
	}
	return "UNKNOWN"
}

func (o PredicateOp) String() string {
	switch o {
	case OpEq:
		return "EQ"
	case OpIn:
		return "IN"
	case OpLt:
		return "LT"
	case OpLe:
		return "LE"
	case OpGt:
		return "GT"
	case OpGe:
		return "GE"
	case OpIsNull:
		return "IS_NULL"
	case OpIsNotNull:
		return "IS_NOT_NULL"
	}
	return "INVALID"
}
