package bigquery

import (
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// This file is the type mapping: BigQuery's field schema to the portable
// Column, and the portable Column to an Arrow type. Both directions are total
// over what is mapped and refuse the rest with serr.Unsupported; nothing falls
// into a type that merely looks right.

// Defaults of the two decimal types when a column states no precision.
const (
	numericPrecision, numericScale       = 38, 9
	bigNumericPrecision, bigNumericScale = 76, 38
)

// itemName names the single element column of an ARRAY, which is Arrow's own
// name for it.
const itemName = "item"

// fieldType normalizes the standard-SQL spellings BigQuery also accepts to the
// ones its schemas report.
func fieldType(t bq.FieldType) bq.FieldType {
	switch strings.ToUpper(string(t)) {
	case "INT64":
		return bq.IntegerFieldType
	case "FLOAT64":
		return bq.FloatFieldType
	case "BOOL":
		return bq.BooleanFieldType
	case "STRUCT":
		return bq.RecordFieldType
	default:
		return bq.FieldType(strings.ToUpper(string(t)))
	}
}

// portableType is the portable bucket of a BigQuery type. A type with no bucket
// is TypeUnknown and keeps its spelling in Column.NativeType.
func portableType(t bq.FieldType) backend.ColumnType {
	switch fieldType(t) {
	case bq.BooleanFieldType:
		return backend.TypeBool
	case bq.IntegerFieldType:
		return backend.TypeInt64
	case bq.FloatFieldType:
		return backend.TypeFloat64
	case bq.NumericFieldType, bq.BigNumericFieldType:
		return backend.TypeNumeric
	case bq.StringFieldType:
		return backend.TypeString
	case bq.BytesFieldType:
		return backend.TypeBytes
	case bq.DateFieldType:
		return backend.TypeDate
	case bq.TimeFieldType:
		return backend.TypeTime
	case bq.DateTimeFieldType:
		return backend.TypeTimestamp
	case bq.TimestampFieldType:
		return backend.TypeTimestampTZ
	case bq.IntervalFieldType:
		return backend.TypeInterval
	case bq.JSONFieldType:
		return backend.TypeJSON
	case bq.RecordFieldType:
		return backend.TypeStruct
	case bq.GeographyFieldType:
		return backend.TypeGeography
	default:
		return backend.TypeUnknown
	}
}

// nativeType is BigQuery's own standard-SQL spelling of a field's type, with its
// parameters: NUMERIC(10, 2), STRING(32), ARRAY<STRUCT<a INT64>>.
func nativeType(f *bq.FieldSchema) string {
	base := baseNativeType(f)
	if f.Repeated {
		return "ARRAY<" + base + ">"
	}
	return base
}

func baseNativeType(f *bq.FieldSchema) string {
	switch t := fieldType(f.Type); t {
	case bq.IntegerFieldType:
		return "INT64"
	case bq.FloatFieldType:
		return "FLOAT64"
	case bq.BooleanFieldType:
		return "BOOL"
	case bq.RecordFieldType:
		parts := make([]string, len(f.Schema))
		for i, sub := range f.Schema {
			parts[i] = sub.Name + " " + nativeType(sub)
		}
		return "STRUCT<" + strings.Join(parts, ", ") + ">"
	case bq.NumericFieldType, bq.BigNumericFieldType:
		switch {
		case f.Precision > 0 && f.Scale > 0:
			return fmt.Sprintf("%s(%d, %d)", t, f.Precision, f.Scale)
		case f.Precision > 0:
			return fmt.Sprintf("%s(%d)", t, f.Precision)
		}
		return string(t)
	case bq.StringFieldType, bq.BytesFieldType:
		if f.MaxLength > 0 {
			return fmt.Sprintf("%s(%d)", t, f.MaxLength)
		}
		return string(t)
	case bq.RangeFieldType:
		if f.RangeElementType != nil {
			return "RANGE<" + string(fieldType(f.RangeElementType.Type)) + ">"
		}
		return string(t)
	default:
		return string(t)
	}
}

// columnFromField is the portable description of a BigQuery field. A REPEATED
// field is an ARRAY whose single sub-column is the element; a RECORD is a STRUCT
// whose sub-columns are its fields.
func columnFromField(f *bq.FieldSchema) backend.Column {
	element := backend.Column{
		Name:        f.Name,
		Type:        portableType(f.Type),
		Nullable:    !f.Required,
		NativeType:  baseNativeType(f),
		Description: f.Description,
	}
	switch fieldType(f.Type) {
	case bq.NumericFieldType:
		element.Precision, element.Scale = decimalParameters(f, numericPrecision, numericScale)
	case bq.BigNumericFieldType:
		element.Precision, element.Scale = decimalParameters(f, bigNumericPrecision, bigNumericScale)
	case bq.RecordFieldType:
		element.Fields = columnsFromSchema(f.Schema)
	}
	if !f.Repeated {
		return element
	}
	// BigQuery has no NULL array and no NULL element: an absent array is empty.
	element.Name, element.Nullable, element.Description = itemName, false, ""
	return backend.Column{
		Name:        f.Name,
		Type:        backend.TypeArray,
		Nullable:    false,
		NativeType:  nativeType(f),
		Description: f.Description,
		Fields:      []backend.Column{element},
	}
}

func decimalParameters(f *bq.FieldSchema, defaultPrecision, defaultScale int32) (int32, int32) {
	if f.Precision > 0 {
		return int32(f.Precision), int32(f.Scale)
	}
	return defaultPrecision, defaultScale
}

func columnsFromSchema(s bq.Schema) []backend.Column {
	if s == nil {
		return nil
	}
	out := make([]backend.Column, len(s))
	for i, f := range s {
		out[i] = columnFromField(f)
	}
	return out
}

// fieldFromColumn is the BigQuery field a portable column creates. A column of
// a type BigQuery has no equivalent for is Unsupported, never a lookalike.
func fieldFromColumn(c backend.Column) (*bq.FieldSchema, error) {
	if strings.TrimSpace(c.Name) == "" {
		return nil, serr.New(serr.InvalidArgument, "CreateTable", "a column has no name")
	}
	if c.Type == backend.TypeArray {
		if len(c.Fields) != 1 {
			return nil, serr.New(serr.InvalidArgument, "CreateTable",
				fmt.Sprintf("column %q is an ARRAY and must carry exactly one element column", c.Name))
		}
		element := c.Fields[0]
		if element.Type == backend.TypeArray {
			return nil, serr.New(serr.Unsupported, "CreateTable",
				fmt.Sprintf("column %q: BigQuery does not allow an array of arrays", c.Name))
		}
		element.Name = c.Name
		f, err := fieldFromColumn(element)
		if err != nil {
			return nil, err
		}
		f.Repeated, f.Required = true, false
		f.Description = c.Description
		return f, nil
	}

	f := &bq.FieldSchema{Name: c.Name, Description: c.Description, Required: !c.Nullable}
	switch c.Type {
	case backend.TypeBool:
		f.Type = bq.BooleanFieldType
	case backend.TypeInt64:
		f.Type = bq.IntegerFieldType
	case backend.TypeFloat64:
		f.Type = bq.FloatFieldType
	case backend.TypeNumeric:
		f.Type = bq.NumericFieldType
		if c.Precision > numericPrecision || c.Scale > numericScale ||
			strings.HasPrefix(strings.ToUpper(c.NativeType), "BIGNUMERIC") {
			f.Type = bq.BigNumericFieldType
		}
		// A precision and scale equal to the type's own bounds is the unparameterized
		// type, which is how BigQuery reports it back.
		defaultPrecision, defaultScale := int32(numericPrecision), int32(numericScale)
		if f.Type == bq.BigNumericFieldType {
			defaultPrecision, defaultScale = bigNumericPrecision, bigNumericScale
		}
		if c.Precision > 0 && !(c.Precision == defaultPrecision && c.Scale == defaultScale) {
			f.Precision, f.Scale = int64(c.Precision), int64(c.Scale)
		}
	case backend.TypeString:
		f.Type = bq.StringFieldType
	case backend.TypeBytes:
		f.Type = bq.BytesFieldType
	case backend.TypeDate:
		f.Type = bq.DateFieldType
	case backend.TypeTime:
		f.Type = bq.TimeFieldType
	case backend.TypeTimestamp:
		f.Type = bq.DateTimeFieldType
	case backend.TypeTimestampTZ:
		f.Type = bq.TimestampFieldType
	case backend.TypeInterval:
		f.Type = bq.IntervalFieldType
	case backend.TypeJSON:
		f.Type = bq.JSONFieldType
	case backend.TypeGeography:
		f.Type = bq.GeographyFieldType
	case backend.TypeStruct:
		if len(c.Fields) == 0 {
			return nil, serr.New(serr.InvalidArgument, "CreateTable",
				fmt.Sprintf("column %q is a STRUCT and must carry its fields", c.Name))
		}
		f.Type = bq.RecordFieldType
		for _, sub := range c.Fields {
			subField, err := fieldFromColumn(sub)
			if err != nil {
				return nil, err
			}
			f.Schema = append(f.Schema, subField)
		}
	default:
		return nil, serr.New(serr.Unsupported, "CreateTable",
			fmt.Sprintf("column %q has a type with no BigQuery equivalent (native type %q)", c.Name, c.NativeType))
	}
	return f, nil
}

func schemaFromColumns(cols []backend.Column) (bq.Schema, error) {
	out := make(bq.Schema, 0, len(cols))
	for _, c := range cols {
		f, err := fieldFromColumn(c)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}

// arrowField is the Arrow field a portable column becomes in a result.
func arrowField(c backend.Column) (arrow.Field, error) {
	t, err := arrowType(c)
	if err != nil {
		return arrow.Field{}, err
	}
	return arrow.Field{Name: c.Name, Type: t, Nullable: c.Nullable}, nil
}

// arrowType is the Arrow type of a portable column. A column whose type has no
// portable bucket (a BigQuery RANGE, say) is Unsupported: a result is never
// sent with a column the client could mistake for another type.
func arrowType(c backend.Column) (arrow.DataType, error) {
	switch c.Type {
	case backend.TypeBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case backend.TypeInt64:
		return arrow.PrimitiveTypes.Int64, nil
	case backend.TypeFloat64:
		return arrow.PrimitiveTypes.Float64, nil
	case backend.TypeNumeric:
		precision, scale := c.Precision, c.Scale
		if precision == 0 {
			precision, scale = numericPrecision, numericScale
			if strings.HasPrefix(strings.ToUpper(c.NativeType), "BIGNUMERIC") {
				precision, scale = bigNumericPrecision, bigNumericScale
			}
		}
		if precision > numericPrecision {
			return &arrow.Decimal256Type{Precision: precision, Scale: scale}, nil
		}
		return &arrow.Decimal128Type{Precision: precision, Scale: scale}, nil
	case backend.TypeString, backend.TypeJSON, backend.TypeGeography:
		return arrow.BinaryTypes.String, nil
	case backend.TypeBytes:
		return arrow.BinaryTypes.Binary, nil
	case backend.TypeDate:
		return arrow.FixedWidthTypes.Date32, nil
	case backend.TypeTime:
		return arrow.FixedWidthTypes.Time64us, nil
	case backend.TypeTimestamp:
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case backend.TypeTimestampTZ:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case backend.TypeInterval:
		return arrow.FixedWidthTypes.MonthDayNanoInterval, nil
	case backend.TypeArray:
		if len(c.Fields) != 1 {
			return nil, serr.New(serr.Internal, "arrow", fmt.Sprintf("array column %q has no element type", c.Name))
		}
		element, err := arrowField(c.Fields[0])
		if err != nil {
			return nil, err
		}
		return arrow.ListOfField(element), nil
	case backend.TypeStruct:
		fields := make([]arrow.Field, len(c.Fields))
		for i, sub := range c.Fields {
			f, err := arrowField(sub)
			if err != nil {
				return nil, err
			}
			fields[i] = f
		}
		return arrow.StructOf(fields...), nil
	default:
		return nil, serr.New(serr.Unsupported, "arrow",
			fmt.Sprintf("column %q (native type %q) has no portable type and cannot be sent as Arrow", c.Name, c.NativeType))
	}
}

func arrowSchema(cols []backend.Column) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		f, err := arrowField(c)
		if err != nil {
			return nil, err
		}
		fields[i] = f
	}
	return arrow.NewSchema(fields, nil), nil
}
