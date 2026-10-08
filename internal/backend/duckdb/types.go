package duckdb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// dtype is a DuckDB column type with its structure resolved: the internal type
// plus whatever the type carries (decimal width and scale, list element, struct
// fields). It is the single description every other file works from — the
// portable column, the Arrow type, the SQL spelling and the value converters
// are all functions of it, so the four can never disagree about one type.
type dtype struct {
	typ   duckdb.Type
	alias string // "JSON" for a JSON column, which DuckDB stores as VARCHAR

	width, scale uint8  // DECIMAL
	size         uint64 // ARRAY length

	elem   *dtype   // LIST / ARRAY element, MAP value
	key    *dtype   // MAP key
	fields []dfield // STRUCT fields, UNION members
	enum   []string // ENUM values, for the native spelling only
}

type dfield struct {
	name string
	dt   *dtype
}

func (t *dtype) isJSON() bool { return t.typ == duckdb.TYPE_VARCHAR && t.alias == "JSON" }

// fromTypeInfo resolves the structured type DuckDB reports for a prepared
// statement column. A type this backend cannot return is an error naming the
// column, so a query over it fails whole instead of returning a lie.
func fromTypeInfo(ti duckdb.TypeInfo) (*dtype, error) {
	t := &dtype{typ: ti.InternalType(), alias: ti.Alias()}
	switch d := ti.Details().(type) {
	case *duckdb.DecimalDetails:
		t.width, t.scale = d.Width, d.Scale
	case *duckdb.ListDetails:
		elem, err := fromTypeInfo(d.Child)
		if err != nil {
			return nil, err
		}
		t.elem = elem
	case *duckdb.ArrayDetails:
		elem, err := fromTypeInfo(d.Child)
		if err != nil {
			return nil, err
		}
		t.elem, t.size = elem, d.Size
	case *duckdb.StructDetails:
		for _, e := range d.Entries {
			ft, err := fromTypeInfo(e.Info())
			if err != nil {
				return nil, err
			}
			t.fields = append(t.fields, dfield{name: e.Name(), dt: ft})
		}
	case *duckdb.MapDetails:
		var err error
		if t.key, err = fromTypeInfo(d.Key); err != nil {
			return nil, err
		}
		if t.elem, err = fromTypeInfo(d.Value); err != nil {
			return nil, err
		}
	case *duckdb.UnionDetails:
		for _, m := range d.Members {
			mt, err := fromTypeInfo(m.Type)
			if err != nil {
				return nil, err
			}
			t.fields = append(t.fields, dfield{name: m.Name, dt: mt})
		}
	case *duckdb.EnumDetails:
		t.enum = d.Values
	}
	return t, nil
}

// scalarNames spells the scalar types a driver reports by name, for the one
// case where the structured type is unavailable: a result column whose type
// depends on a parameter that was not bound when the statement was prepared.
var scalarNames = map[string]duckdb.Type{
	"BOOLEAN": duckdb.TYPE_BOOLEAN, "TINYINT": duckdb.TYPE_TINYINT, "SMALLINT": duckdb.TYPE_SMALLINT,
	"INTEGER": duckdb.TYPE_INTEGER, "BIGINT": duckdb.TYPE_BIGINT, "UTINYINT": duckdb.TYPE_UTINYINT,
	"USMALLINT": duckdb.TYPE_USMALLINT, "UINTEGER": duckdb.TYPE_UINTEGER, "UBIGINT": duckdb.TYPE_UBIGINT,
	"HUGEINT": duckdb.TYPE_HUGEINT, "UHUGEINT": duckdb.TYPE_UHUGEINT, "FLOAT": duckdb.TYPE_FLOAT,
	"DOUBLE": duckdb.TYPE_DOUBLE, "VARCHAR": duckdb.TYPE_VARCHAR, "BLOB": duckdb.TYPE_BLOB,
	"DATE": duckdb.TYPE_DATE, "TIME": duckdb.TYPE_TIME, "TIMETZ": duckdb.TYPE_TIME_TZ,
	"TIMESTAMP": duckdb.TYPE_TIMESTAMP, "TIMESTAMP_S": duckdb.TYPE_TIMESTAMP_S,
	"TIMESTAMP_MS": duckdb.TYPE_TIMESTAMP_MS, "TIMESTAMP_NS": duckdb.TYPE_TIMESTAMP_NS,
	"TIMESTAMPTZ": duckdb.TYPE_TIMESTAMP_TZ, "INTERVAL": duckdb.TYPE_INTERVAL,
	"UUID": duckdb.TYPE_UUID, "BIT": duckdb.TYPE_BIT,
}

// fromScalarName resolves a scalar by the name the result reports.
func fromScalarName(name string) (*dtype, bool) {
	if name == "JSON" {
		return &dtype{typ: duckdb.TYPE_VARCHAR, alias: "JSON"}, true
	}
	t, ok := scalarNames[name]
	if !ok {
		return nil, false
	}
	return &dtype{typ: t}, true
}

// portable is the portable bucket for a DuckDB type. A type with no honest
// bucket is TypeUnknown, and the exact DuckDB spelling travels in NativeType.
//
//   - every integer width up to 64 bits is INT64, and every float is FLOAT64,
//     so a client reads the same Arrow width here as on any other backend;
//   - UBIGINT does not fit INT64, so it is NUMERIC(20,0);
//   - HUGEINT and UHUGEINT are NUMERIC(38,0), which is what SUM over an integer
//     column returns here. They hold one more digit than NUMERIC(38) can, so a
//     value past 38 digits is refused when it is read (see appendValue) instead
//     of being rounded or rendered as something else;
//   - DuckDB GEOMETRY is planar, not the spheroidal GEOGRAPHY the portable type
//     names, so it is UNKNOWN: DuckDB has no portable GEOGRAPHY at all.
func (t *dtype) portable() backend.ColumnType {
	switch t.typ {
	case duckdb.TYPE_BOOLEAN:
		return backend.TypeBool
	case duckdb.TYPE_TINYINT, duckdb.TYPE_SMALLINT, duckdb.TYPE_INTEGER, duckdb.TYPE_BIGINT,
		duckdb.TYPE_UTINYINT, duckdb.TYPE_USMALLINT, duckdb.TYPE_UINTEGER:
		return backend.TypeInt64
	case duckdb.TYPE_UBIGINT, duckdb.TYPE_HUGEINT, duckdb.TYPE_UHUGEINT, duckdb.TYPE_DECIMAL:
		return backend.TypeNumeric
	case duckdb.TYPE_FLOAT, duckdb.TYPE_DOUBLE:
		return backend.TypeFloat64
	case duckdb.TYPE_VARCHAR:
		if t.isJSON() {
			return backend.TypeJSON
		}
		return backend.TypeString
	case duckdb.TYPE_ENUM:
		return backend.TypeString
	case duckdb.TYPE_BLOB:
		return backend.TypeBytes
	case duckdb.TYPE_DATE:
		return backend.TypeDate
	case duckdb.TYPE_TIME:
		return backend.TypeTime
	case duckdb.TYPE_TIMESTAMP, duckdb.TYPE_TIMESTAMP_S, duckdb.TYPE_TIMESTAMP_MS, duckdb.TYPE_TIMESTAMP_NS:
		return backend.TypeTimestamp
	case duckdb.TYPE_TIMESTAMP_TZ:
		return backend.TypeTimestampTZ
	case duckdb.TYPE_INTERVAL:
		return backend.TypeInterval
	case duckdb.TYPE_LIST, duckdb.TYPE_ARRAY:
		return backend.TypeArray
	case duckdb.TYPE_STRUCT:
		return backend.TypeStruct
	default:
		return backend.TypeUnknown
	}
}

// errJSONRead is why a JSON value cannot be returned.
//
// The driver decodes a JSON column with encoding/json into Go values before this
// package sees it, which loses what the column held: an integer above 2^53 comes
// back rounded, a JSON null reads as a SQL NULL, and the text is re-spelled. A
// result must be exact or refused, so a JSON column is refused until the driver
// can hand over the stored text; casting the column to VARCHAR in the query
// returns that text unchanged.
var errJSONRead = fmt.Errorf("JSON values cannot be returned exactly; cast the column to VARCHAR")

// readable reports whether results of this type can be returned, and if not,
// why.
func (t *dtype) readable() error {
	if t.isJSON() {
		return errJSONRead
	}
	switch t.typ {
	case duckdb.TYPE_BOOLEAN, duckdb.TYPE_TINYINT, duckdb.TYPE_SMALLINT, duckdb.TYPE_INTEGER, duckdb.TYPE_BIGINT,
		duckdb.TYPE_UTINYINT, duckdb.TYPE_USMALLINT, duckdb.TYPE_UINTEGER, duckdb.TYPE_UBIGINT,
		duckdb.TYPE_HUGEINT, duckdb.TYPE_UHUGEINT, duckdb.TYPE_BIGNUM,
		duckdb.TYPE_FLOAT, duckdb.TYPE_DOUBLE, duckdb.TYPE_DECIMAL,
		duckdb.TYPE_VARCHAR, duckdb.TYPE_ENUM, duckdb.TYPE_BLOB, duckdb.TYPE_BIT, duckdb.TYPE_UUID, duckdb.TYPE_GEOMETRY,
		duckdb.TYPE_DATE, duckdb.TYPE_TIME, duckdb.TYPE_TIME_TZ,
		duckdb.TYPE_TIMESTAMP, duckdb.TYPE_TIMESTAMP_S, duckdb.TYPE_TIMESTAMP_MS, duckdb.TYPE_TIMESTAMP_NS,
		duckdb.TYPE_TIMESTAMP_TZ, duckdb.TYPE_INTERVAL:
		return nil
	case duckdb.TYPE_LIST, duckdb.TYPE_ARRAY:
		if t.elem == nil {
			return fmt.Errorf("list without an element type")
		}
		return t.elem.readable()
	case duckdb.TYPE_STRUCT:
		for _, f := range t.fields {
			if err := f.dt.readable(); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("the duckdb backend cannot return type %s", t.native())
}

// nativeScalars is DuckDB's own spelling of each scalar type.
var nativeScalars = map[duckdb.Type]string{
	duckdb.TYPE_BOOLEAN: "BOOLEAN", duckdb.TYPE_TINYINT: "TINYINT", duckdb.TYPE_SMALLINT: "SMALLINT",
	duckdb.TYPE_INTEGER: "INTEGER", duckdb.TYPE_BIGINT: "BIGINT", duckdb.TYPE_UTINYINT: "UTINYINT",
	duckdb.TYPE_USMALLINT: "USMALLINT", duckdb.TYPE_UINTEGER: "UINTEGER", duckdb.TYPE_UBIGINT: "UBIGINT",
	duckdb.TYPE_HUGEINT: "HUGEINT", duckdb.TYPE_UHUGEINT: "UHUGEINT", duckdb.TYPE_BIGNUM: "BIGNUM",
	duckdb.TYPE_FLOAT: "FLOAT", duckdb.TYPE_DOUBLE: "DOUBLE", duckdb.TYPE_VARCHAR: "VARCHAR",
	duckdb.TYPE_BLOB: "BLOB", duckdb.TYPE_BIT: "BIT", duckdb.TYPE_UUID: "UUID", duckdb.TYPE_GEOMETRY: "GEOMETRY",
	duckdb.TYPE_DATE: "DATE", duckdb.TYPE_TIME: "TIME", duckdb.TYPE_TIME_TZ: "TIMETZ",
	duckdb.TYPE_TIMESTAMP: "TIMESTAMP", duckdb.TYPE_TIMESTAMP_S: "TIMESTAMP_S",
	duckdb.TYPE_TIMESTAMP_MS: "TIMESTAMP_MS", duckdb.TYPE_TIMESTAMP_NS: "TIMESTAMP_NS",
	duckdb.TYPE_TIMESTAMP_TZ: "TIMESTAMPTZ", duckdb.TYPE_INTERVAL: "INTERVAL",
	duckdb.TYPE_MAP: "MAP", duckdb.TYPE_UNION: "UNION", duckdb.TYPE_VARIANT: "VARIANT",
	duckdb.TYPE_STRUCT: "STRUCT",
}

// native is the exact DuckDB spelling, as it would appear in a CREATE TABLE.
func (t *dtype) native() string {
	switch t.typ {
	case duckdb.TYPE_VARCHAR:
		if t.isJSON() {
			return "JSON"
		}
		return "VARCHAR"
	case duckdb.TYPE_DECIMAL:
		return "DECIMAL(" + strconv.Itoa(int(t.width)) + "," + strconv.Itoa(int(t.scale)) + ")"
	case duckdb.TYPE_ENUM:
		quoted := make([]string, len(t.enum))
		for i, v := range t.enum {
			quoted[i] = quoteLiteral(v)
		}
		return "ENUM(" + strings.Join(quoted, ", ") + ")"
	case duckdb.TYPE_LIST:
		return t.elem.native() + "[]"
	case duckdb.TYPE_ARRAY:
		return t.elem.native() + "[" + strconv.FormatUint(t.size, 10) + "]"
	case duckdb.TYPE_STRUCT, duckdb.TYPE_UNION:
		parts := make([]string, len(t.fields))
		for i, f := range t.fields {
			parts[i] = quoteIdent(f.name) + " " + f.dt.native()
		}
		return nativeScalars[t.typ] + "(" + strings.Join(parts, ", ") + ")"
	case duckdb.TYPE_MAP:
		return "MAP(" + t.key.native() + ", " + t.elem.native() + ")"
	}
	if s, ok := nativeScalars[t.typ]; ok {
		return s
	}
	return "UNKNOWN"
}

// column is the portable description of a result or table column of type t.
func (t *dtype) column(name string, nullable bool) backend.Column {
	c := backend.Column{
		Name:       name,
		Type:       t.portable(),
		Nullable:   nullable,
		NativeType: t.native(),
	}
	switch t.typ {
	case duckdb.TYPE_DECIMAL:
		c.Precision, c.Scale = int32(t.width), int32(t.scale)
	case duckdb.TYPE_UBIGINT:
		c.Precision = 20
	case duckdb.TYPE_HUGEINT, duckdb.TYPE_UHUGEINT:
		c.Precision = 38
	case duckdb.TYPE_LIST, duckdb.TYPE_ARRAY:
		c.Fields = []backend.Column{t.elem.column("element", true)}
	case duckdb.TYPE_STRUCT:
		for _, f := range t.fields {
			c.Fields = append(c.Fields, f.dt.column(f.name, true))
		}
	}
	return c
}

// arrowType is the Arrow type of t's values. It is a function of the portable
// type, not of DuckDB's width: a client reads int64 and float64 on every
// backend, so code written against this engine does not break on another.
func (t *dtype) arrowType() arrow.DataType {
	switch t.typ {
	case duckdb.TYPE_BOOLEAN:
		return arrow.FixedWidthTypes.Boolean
	case duckdb.TYPE_TINYINT, duckdb.TYPE_SMALLINT, duckdb.TYPE_INTEGER, duckdb.TYPE_BIGINT,
		duckdb.TYPE_UTINYINT, duckdb.TYPE_USMALLINT, duckdb.TYPE_UINTEGER:
		return arrow.PrimitiveTypes.Int64
	case duckdb.TYPE_UBIGINT:
		return &arrow.Decimal128Type{Precision: 20, Scale: 0}
	case duckdb.TYPE_HUGEINT, duckdb.TYPE_UHUGEINT:
		return &arrow.Decimal128Type{Precision: 38, Scale: 0}
	case duckdb.TYPE_FLOAT, duckdb.TYPE_DOUBLE:
		return arrow.PrimitiveTypes.Float64
	case duckdb.TYPE_DECIMAL:
		return &arrow.Decimal128Type{Precision: int32(t.width), Scale: int32(t.scale)}
	case duckdb.TYPE_BLOB, duckdb.TYPE_GEOMETRY:
		return arrow.BinaryTypes.Binary
	case duckdb.TYPE_DATE:
		return arrow.FixedWidthTypes.Date32
	case duckdb.TYPE_TIME:
		return arrow.FixedWidthTypes.Time64us
	// arrow's own Timestamp_us / Timestamp_ns constants carry a UTC zone, which
	// would turn a wall-clock TIMESTAMP into an instant.
	case duckdb.TYPE_TIMESTAMP, duckdb.TYPE_TIMESTAMP_S, duckdb.TYPE_TIMESTAMP_MS:
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case duckdb.TYPE_TIMESTAMP_NS:
		return &arrow.TimestampType{Unit: arrow.Nanosecond}
	case duckdb.TYPE_TIMESTAMP_TZ:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case duckdb.TYPE_INTERVAL:
		return arrow.FixedWidthTypes.MonthDayNanoInterval
	case duckdb.TYPE_LIST, duckdb.TYPE_ARRAY:
		return arrow.ListOfField(arrow.Field{Name: "element", Type: t.elem.arrowType(), Nullable: true})
	case duckdb.TYPE_STRUCT:
		fields := make([]arrow.Field, len(t.fields))
		for i, f := range t.fields {
			fields[i] = arrow.Field{Name: f.name, Type: f.dt.arrowType(), Nullable: true}
		}
		return arrow.StructOf(fields...)
	default:
		// Strings, enums, JSON, and the types with no bucket (UUID, TIMETZ, BIT,
		// BIGNUM) which travel as their text.
		return arrow.BinaryTypes.String
	}
}

// arrowSchema builds the Arrow schema for a set of result columns.
func arrowSchema(names []string, types []*dtype) *arrow.Schema {
	fields := make([]arrow.Field, len(names))
	for i := range names {
		fields[i] = arrow.Field{Name: names[i], Type: types[i].arrowType(), Nullable: true}
	}
	return arrow.NewSchema(fields, nil)
}

// ddlType spells a portable column as the DuckDB type that stores it. A column
// DuckDB cannot store is Unsupported, never a nearby type: storing GEOGRAPHY as
// a planar GEOMETRY would answer geography queries wrongly.
func ddlType(c backend.Column) (string, error) {
	switch c.Type {
	case backend.TypeBool:
		return "BOOLEAN", nil
	case backend.TypeInt64:
		return "BIGINT", nil
	case backend.TypeFloat64:
		return "DOUBLE", nil
	case backend.TypeNumeric:
		p, s := c.Precision, c.Scale
		if p == 0 && s == 0 {
			p, s = 38, 9 // the portable NUMERIC default
		}
		if p < 1 || p > 38 || s < 0 || s > p {
			return "", serr.New(serr.InvalidArgument, "CreateTable",
				fmt.Sprintf("column %q: NUMERIC(%d,%d) is outside DECIMAL(1..38, 0..precision)", c.Name, p, s))
		}
		return fmt.Sprintf("DECIMAL(%d,%d)", p, s), nil
	case backend.TypeString:
		return "VARCHAR", nil
	case backend.TypeBytes:
		return "BLOB", nil
	case backend.TypeDate:
		return "DATE", nil
	case backend.TypeTime:
		return "TIME", nil
	case backend.TypeTimestamp:
		return "TIMESTAMP", nil
	case backend.TypeTimestampTZ:
		return "TIMESTAMPTZ", nil
	case backend.TypeInterval:
		return "INTERVAL", nil
	case backend.TypeJSON:
		return "JSON", nil
	case backend.TypeArray:
		if len(c.Fields) != 1 {
			return "", serr.New(serr.InvalidArgument, "CreateTable",
				fmt.Sprintf("column %q: an ARRAY needs exactly one element type in fields", c.Name))
		}
		elem, err := ddlType(c.Fields[0])
		if err != nil {
			return "", err
		}
		return elem + "[]", nil
	case backend.TypeStruct:
		if len(c.Fields) == 0 {
			return "", serr.New(serr.InvalidArgument, "CreateTable",
				fmt.Sprintf("column %q: a STRUCT needs at least one field", c.Name))
		}
		parts := make([]string, len(c.Fields))
		for i, f := range c.Fields {
			if err := checkName(f.Name); err != nil {
				return "", serr.New(serr.InvalidArgument, "CreateTable", fmt.Sprintf("column %q: struct field: %v", c.Name, err))
			}
			ft, err := ddlType(f)
			if err != nil {
				return "", err
			}
			parts[i] = quoteIdent(f.Name) + " " + ft
		}
		return "STRUCT(" + strings.Join(parts, ", ") + ")", nil
	case backend.TypeGeography:
		return "", serr.New(serr.Unsupported, "CreateTable",
			fmt.Sprintf("column %q: duckdb has no GEOGRAPHY type (its GEOMETRY is planar, not spheroidal)", c.Name))
	default:
		return "", serr.New(serr.Unsupported, "CreateTable",
			fmt.Sprintf("column %q: UNKNOWN has no duckdb spelling", c.Name))
	}
}
