package duckdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

func rowCount(t *testing.T, be backend.Backend, table string) int64 {
	t.Helper()
	return run(t, be, `SELECT count(*) FROM `+table).column(0)[0].(int64)
}

// A row of every portable type goes in as Arrow and comes back through a query,
// and the two agree.
func TestInsertRowsRoundTripsEveryPortableType(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	_, err := be.CreateTable(ctx, backend.TableRef{Dataset: "main", Table: "all_types"}, backend.CreateTableOptions{Columns: []backend.Column{
		{Name: "id", Type: backend.TypeInt64},
		{Name: "b", Type: backend.TypeBool, Nullable: true},
		{Name: "f", Type: backend.TypeFloat64, Nullable: true},
		{Name: "n", Type: backend.TypeNumeric, Precision: 12, Scale: 3, Nullable: true},
		{Name: "s", Type: backend.TypeString, Nullable: true},
		{Name: "by", Type: backend.TypeBytes, Nullable: true},
		{Name: "d", Type: backend.TypeDate, Nullable: true},
		{Name: "t", Type: backend.TypeTime, Nullable: true},
		{Name: "ts", Type: backend.TypeTimestamp, Nullable: true},
		{Name: "tz", Type: backend.TypeTimestampTZ, Nullable: true},
		{Name: "iv", Type: backend.TypeInterval, Nullable: true},
		{Name: "j", Type: backend.TypeJSON, Nullable: true},
		{Name: "arr", Type: backend.TypeArray, Fields: []backend.Column{{Type: backend.TypeInt64}}, Nullable: true},
		{Name: "st", Type: backend.TypeStruct, Nullable: true, Fields: []backend.Column{
			{Name: "a", Type: backend.TypeString},
			{Name: "tags", Type: backend.TypeArray, Fields: []backend.Column{{Type: backend.TypeString}}},
		}},
	}})
	require.NoError(t, err)

	schema := arrow.NewSchema([]arrow.Field{
		field("id", arrow.PrimitiveTypes.Int64),
		field("b", arrow.FixedWidthTypes.Boolean),
		field("f", arrow.PrimitiveTypes.Float64),
		field("n", &arrow.Decimal128Type{Precision: 12, Scale: 3}),
		field("s", arrow.BinaryTypes.String),
		field("by", arrow.BinaryTypes.Binary),
		field("d", arrow.FixedWidthTypes.Date32),
		field("t", arrow.FixedWidthTypes.Time64us),
		field("ts", &arrow.TimestampType{Unit: arrow.Microsecond}),
		field("tz", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}),
		field("iv", arrow.FixedWidthTypes.MonthDayNanoInterval),
		field("j", arrow.BinaryTypes.String),
		field("arr", arrow.ListOf(arrow.PrimitiveTypes.Int64)),
		field("st", arrow.StructOf(field("a", arrow.BinaryTypes.String), field("tags", arrow.ListOf(arrow.BinaryTypes.String)))),
	}, nil)
	num, err := decimal128.FromString("12345.678", 12, 3)
	require.NoError(t, err)
	stamp := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)

	rec := rowsOf(schema,
		[]any{int64(1), true, 2.5, num, "héllo", []byte{0, 1, 2}, stamp, int64(10*3600+11*60+12) * 1_000_000, stamp, stamp,
			arrow.MonthDayNanoInterval{Months: 14, Days: 3, Nanoseconds: 4_000_000_000},
			`{"id": 9007199254740993, "k": [1, null]}`, []any{int64(1), nil, int64(3)},
			map[string]any{"a": "x", "tags": []any{"p", "q"}}},
		[]any{int64(2), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
	)
	defer rec.Release()
	res, err := insert(t, be, "all_types", rec)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.RowsInserted)
	require.Empty(t, res.Errors)

	r := run(t, be, `SELECT b, f, n, s, "by", d, t, ts, tz, iv, CAST(j AS VARCHAR) AS j, arr, st FROM all_types ORDER BY id`)
	want := [][]string{
		{"true", "NULL"},
		{"2.5", "NULL"},
		{"12345.678", "NULL"},
		{"héllo", "NULL"},
		{"[0 1 2]", "NULL"},
		{"2024-01-02", "NULL"},
		{"10:11:12", "NULL"},
		{"2024-01-02T03:04:05.123456", "NULL"},
		{"2024-01-02T03:04:05.123456Z", "NULL"},
		{"{14 3 4000000000}", "NULL"},
		// The text is stored compact, with its numbers exactly as given.
		{`{"id":9007199254740993,"k":[1,null]}`, "NULL"},
		{"[1,null,3]", "NULL"},
		{`{"a":"x","tags":["p","q"]}`, "NULL"},
	}
	for i, w := range want {
		col := r.column(i)
		require.Len(t, col, 2, r.res.Schema[i].Name)
		require.Equal(t, w[0], render(col[0]), r.res.Schema[i].Name)
		require.Equal(t, w[1], render(col[1]), r.res.Schema[i].Name)
	}
}

// Arrow's other spellings of a type are accepted where they mean the same thing.
func TestInsertRowsAcceptsEquivalentArrowTypes(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE t (a BIGINT, b SMALLINT, c DOUBLE, d DECIMAL(10,2), e VARCHAR, f DATE, g TIMESTAMP, h TIMESTAMPTZ, i UBIGINT)`)
	schema := arrow.NewSchema([]arrow.Field{
		field("a", arrow.PrimitiveTypes.Int32),
		field("b", arrow.PrimitiveTypes.Int8),
		field("c", arrow.PrimitiveTypes.Float32),
		field("d", &arrow.Decimal128Type{Precision: 12, Scale: 4}),
		field("e", arrow.BinaryTypes.LargeString),
		field("f", arrow.FixedWidthTypes.Date64),
		field("g", &arrow.TimestampType{Unit: arrow.Nanosecond}),
		field("h", &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Europe/Paris"}),
		field("i", arrow.PrimitiveTypes.Uint64),
	}, nil)
	num, err := decimal128.FromString("1.2300", 12, 4)
	require.NoError(t, err)
	stamp := time.Date(2024, 5, 6, 7, 8, 9, 123000000, time.UTC)
	rec := rowsOf(schema, []any{int32(-5), int8(7), float32(1.5), num, "x", stamp, stamp, stamp, uint64(18446744073709551615)})
	defer rec.Release()
	res, err := insert(t, be, "t", rec)
	require.NoError(t, err)
	require.Empty(t, res.Errors)
	require.Equal(t, int64(1), res.RowsInserted)

	r := run(t, be, `SELECT * FROM t`)
	got := make([]string, len(r.res.Schema))
	for i := range got {
		got[i] = render(r.column(i)[0])
	}
	require.Equal(t, []string{"-5", "7", "1.5", "1.23", "x", "2024-05-06", "2024-05-06T07:08:09.123", "2024-05-06T07:08:09.123Z", "18446744073709551615"}, got)
}

// The engine refuses part of a stream and keeps the rest, and each refusal is
// reported by its offset in the stream, across batches.
func TestInsertRowsReportsRefusedRowsByOffset(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE people (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL)`)
	mustExec(t, be, `INSERT INTO people VALUES (1, 'ada')`)

	schema := arrow.NewSchema([]arrow.Field{field("id", arrow.PrimitiveTypes.Int64), field("name", arrow.BinaryTypes.String)}, nil)
	first := rowsOf(schema, []any{int64(2), "b"}, []any{int64(1), "duplicate"}, []any{int64(3), "c"})
	second := rowsOf(schema, []any{int64(4), nil}, []any{int64(5), "e"})
	defer first.Release()
	defer second.Release()

	res, err := insert(t, be, "people", first, second)
	require.NoError(t, err)
	require.Equal(t, int64(3), res.RowsInserted)
	// A duplicate key has no portable reason of its own; a null in a required
	// column is a value the column cannot hold.
	require.Equal(t, []backend.RowError{
		{RowIndex: 1, Reason: backend.RefusalUnspecified, Error: "a constraint was violated"},
		{RowIndex: 3, Reason: backend.RefusalInvalidValue, Error: "a required column has no value"},
	}, res.Errors)
	require.Equal(t, []any{int64(1), int64(2), int64(3), int64(5)}, run(t, be, `SELECT id FROM people ORDER BY id`).column(0))
}

// A value the column cannot hold is refused as that row, not coerced.
func TestInsertRowsRefusesValuesTheColumnCannotHold(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE t (id BIGINT, tiny TINYINT, price DECIMAL(5,2), doc JSON, stamp TIMESTAMP, name VARCHAR)`)
	schema := arrow.NewSchema([]arrow.Field{
		field("id", arrow.PrimitiveTypes.Int64),
		field("tiny", arrow.PrimitiveTypes.Int64),
		field("price", &arrow.Decimal128Type{Precision: 12, Scale: 4}),
		field("doc", arrow.BinaryTypes.String),
		field("stamp", &arrow.TimestampType{Unit: arrow.Nanosecond}),
		field("name", arrow.BinaryTypes.String),
	}, nil)
	dec := func(s string) decimal128.Num {
		n, err := decimal128.FromString(s, 12, 4)
		require.NoError(t, err)
		return n
	}
	ok := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rec := rowsOf(schema,
		[]any{int64(0), int64(127), dec("123.4500"), `{"a":1}`, ok, "fine"},
		[]any{int64(1), int64(128), dec("1.0000"), `{}`, ok, "tinyint overflow"},
		[]any{int64(2), int64(-129), dec("1.0000"), `{}`, ok, "tinyint underflow"},
		[]any{int64(3), int64(1), dec("1.2345"), `{}`, ok, "too many fractional digits"},
		[]any{int64(4), int64(1), dec("1234.5600"), `{}`, ok, "too many digits"},
		[]any{int64(5), int64(1), dec("1.0000"), `{not json`, ok, "invalid json"},
		[]any{int64(6), int64(1), dec("1.0000"), `{}`, ok.Add(7 * time.Nanosecond), "sub-microsecond timestamp"},
		[]any{int64(7), int64(1), dec("1.0000"), `{}`, ok, "bad\xffutf8"},
		[]any{int64(8), int64(-128), dec("-999.9900"), `[]`, ok, "fine again"},
	)
	defer rec.Release()
	res, err := insert(t, be, "t", rec)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.RowsInserted)
	var refused []int64
	for _, e := range res.Errors {
		refused = append(refused, e.RowIndex)
		require.NotEmpty(t, e.Error)
		require.NotContains(t, e.Error, "Error:")
		require.Equal(t, backend.RefusalInvalidValue, e.Reason, "row %d: %s", e.RowIndex, e.Error)
	}
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7}, refused)
	require.Equal(t, []any{int64(0), int64(8)}, run(t, be, `SELECT id FROM t ORDER BY id`).column(0))
}

func TestInsertRowsRefusesTheWholeRequestWhenTheShapeIsWrong(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	mustExec(t, be, `CREATE TABLE t (id BIGINT, name VARCHAR, u UUID, h HUGEINT)`)
	mustExec(t, be, `CREATE VIEW v AS SELECT id FROM t`)
	idName := arrow.NewSchema([]arrow.Field{field("id", arrow.PrimitiveTypes.Int64), field("name", arrow.BinaryTypes.String)}, nil)

	for _, tc := range []struct {
		name  string
		table string
		rec   arrow.RecordBatch
		want  serr.Code
	}{
		{"unknown column", "t", rowsOf(arrow.NewSchema([]arrow.Field{field("nope", arrow.PrimitiveTypes.Int64)}, nil), []any{int64(1)}), serr.InvalidArgument},
		{"wrong type", "t", rowsOf(arrow.NewSchema([]arrow.Field{field("id", arrow.BinaryTypes.String)}, nil), []any{"1"}), serr.InvalidArgument},
		{"duplicate column", "t", rowsOf(arrow.NewSchema([]arrow.Field{field("id", arrow.PrimitiveTypes.Int64), field("ID", arrow.PrimitiveTypes.Int64)}, nil), []any{int64(1), int64(1)}), serr.InvalidArgument},
		{"uuid column", "t", rowsOf(arrow.NewSchema([]arrow.Field{field("u", arrow.BinaryTypes.String)}, nil), []any{"x"}), serr.Unsupported},
		{"hugeint column", "t", rowsOf(arrow.NewSchema([]arrow.Field{field("h", arrow.PrimitiveTypes.Int64)}, nil), []any{int64(1)}), serr.Unsupported},
		{"missing table", "absent", rowsOf(idName, []any{int64(1), "x"}), serr.NotFound},
		{"a view", "v", rowsOf(arrow.NewSchema([]arrow.Field{field("id", arrow.PrimitiveTypes.Int64)}, nil), []any{int64(1)}), serr.PreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.rec.Release()
			_, err := insert(t, be, tc.table, tc.rec)
			require.Equal(t, tc.want, serr.CodeOf(err), "%v", err)
		})
	}

	_, err := be.InsertRows(ctx, backend.TableRef{Dataset: "absent", Table: "t"}, nil, &sliceReader{})
	require.Equal(t, serr.NotFound, serr.CodeOf(err))
	_, err = be.InsertRows(ctx, backend.TableRef{Dataset: "main", Table: ""}, nil, &sliceReader{})
	require.Equal(t, serr.InvalidArgument, serr.CodeOf(err))
	_, err = be.InsertRows(ctx, backend.TableRef{Dataset: "main", Table: "t"}, []byte("not arrow"), &sliceReader{})
	require.Equal(t, serr.InvalidArgument, serr.CodeOf(err))
	_, err = be.InsertRows(ctx, backend.TableRef{Dataset: "main", Table: "t"}, nil, &sliceReader{})
	require.Equal(t, serr.InvalidArgument, serr.CodeOf(err), "a stream with no schema anywhere")

	require.Zero(t, rowCount(t, be, "t"), "a refused request writes nothing")
}

// SWH_MAX_ARROW_MESSAGE_BYTES reaches the reader: a message whose body is over it
// is the client's invalid argument, and nothing is written.
func TestInsertRowsRefusesAMessageOverTheConfiguredBound(t *testing.T) {
	be := open(t, func(c *backend.Config) { c.MaxArrowMessageBytes = 16 })
	mustExec(t, be, `CREATE TABLE t (id BIGINT)`)
	schema := arrow.NewSchema([]arrow.Field{field("id", arrow.PrimitiveTypes.Int64)}, nil)
	rec := rowsOf(schema, []any{int64(1)}, []any{int64(2)}, []any{int64(3)})
	defer rec.Release()

	_, err := insert(t, be, "t", rec)
	require.Equal(t, serr.InvalidArgument, serr.CodeOf(err), "%v", err)
	require.Zero(t, rowCount(t, be, "t"))
}

// A failure of the stream itself is the stream's, not a complaint about Arrow.
func TestInsertRowsReturnsTheStreamsOwnError(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE t (id BIGINT)`)
	schema := arrow.NewSchema([]arrow.Field{field("id", arrow.PrimitiveTypes.Int64)}, nil)
	rec := rowsOf(schema, []any{int64(1)}, []any{int64(2)})
	defer rec.Release()
	header, batches := encode(t, rec)
	boom := errors.New("client went away")
	batches.err = boom

	res, err := be.InsertRows(context.Background(), backend.TableRef{Dataset: "main", Table: "t"}, header, batches)
	require.ErrorIs(t, err, boom)
	// What arrived before the failure was written; the insert is not atomic.
	require.Equal(t, int64(2), res.RowsInserted)
	require.Equal(t, int64(2), rowCount(t, be, "t"))
}

func TestInsertRowsFillsOmittedColumnsFromTheirDefaults(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE t (id BIGINT, status VARCHAR DEFAULT 'new', note VARCHAR)`)
	schema := arrow.NewSchema([]arrow.Field{field("id", arrow.PrimitiveTypes.Int64)}, nil)
	rec := rowsOf(schema, []any{int64(1)})
	defer rec.Release()
	_, err := insert(t, be, "t", rec)
	require.NoError(t, err)
	r := run(t, be, `SELECT status, note FROM t`)
	require.Equal(t, []any{"new"}, r.column(0))
	require.Equal(t, []any{nil}, r.column(1))
}

func TestInsertRowsMatchesColumnsByNameNotPosition(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE People (id BIGINT, name VARCHAR)`)
	schema := arrow.NewSchema([]arrow.Field{field("NAME", arrow.BinaryTypes.String), field("Id", arrow.PrimitiveTypes.Int64)}, nil)
	rec := rowsOf(schema, []any{"ada", int64(1)})
	defer rec.Release()
	_, err := insert(t, be, "people", rec)
	require.NoError(t, err)
	r := run(t, be, `SELECT id, name FROM people`)
	require.Equal(t, []any{int64(1)}, r.column(0))
	require.Equal(t, []any{"ada"}, r.column(1))
}

// A client that sends no header schema puts it in the first message.
func TestInsertRowsAcceptsASelfDescribingStream(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE t (id BIGINT)`)
	schema := arrow.NewSchema([]arrow.Field{field("id", arrow.PrimitiveTypes.Int64)}, nil)
	rec := rowsOf(schema, []any{int64(7)})
	defer rec.Release()
	header, batches := encode(t, rec)
	batches.msgs[0] = append(append([]byte(nil), header...), batches.msgs[0]...)

	res, err := be.InsertRows(context.Background(), backend.TableRef{Dataset: "main", Table: "t"}, nil, batches)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.RowsInserted)
}

func TestInsertRowsWithNoBatchesInsertsNothing(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE t (id BIGINT)`)
	header, err := arrowipc.SchemaMessage(arrow.NewSchema([]arrow.Field{field("id", arrow.PrimitiveTypes.Int64)}, nil))
	require.NoError(t, err)
	res, err := be.InsertRows(context.Background(), backend.TableRef{Dataset: "main", Table: "t"}, header, &sliceReader{})
	require.NoError(t, err)
	require.Zero(t, res.RowsInserted)
}

func TestInsertRowsHandlesManyBatches(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE t (id BIGINT, name VARCHAR)`)
	schema := arrow.NewSchema([]arrow.Field{field("id", arrow.PrimitiveTypes.Int64), field("name", arrow.BinaryTypes.String)}, nil)
	var recs []arrow.RecordBatch
	for b := 0; b < 4; b++ {
		var rows [][]any
		for i := 0; i < 5000; i++ {
			rows = append(rows, []any{int64(b*5000 + i), "row"})
		}
		rec := rowsOf(schema, rows...)
		defer rec.Release()
		recs = append(recs, rec)
	}
	res, err := insert(t, be, "t", recs...)
	require.NoError(t, err)
	require.Equal(t, int64(20000), res.RowsInserted)
	require.Equal(t, []any{"199990000"}, []any{render(run(t, be, `SELECT sum(id) FROM t`).column(0)[0])})

	tables, err := be.ListTables(context.Background(), backend.ListTablesOptions{Dataset: "main"})
	require.NoError(t, err)
	require.Equal(t, int64(20000), tables.Tables[0].NumRows, "the row count a listing reports follows the inserts")
}

// Values the engine's types constrain beyond what Arrow says are refused as
// their own row, and the rest of the batch is kept.
func TestInsertRowsRefusesEnumMembersArrayLengthsAndUnstorableValues(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE t (id BIGINT, mood ENUM('happy', 'sad'), triple INTEGER[3], born TIMESTAMP)`)
	schema := arrow.NewSchema([]arrow.Field{
		field("id", arrow.PrimitiveTypes.Int64),
		field("mood", arrow.BinaryTypes.String),
		field("triple", arrow.ListOf(arrow.PrimitiveTypes.Int32)),
		field("born", &arrow.TimestampType{Unit: arrow.Second}),
	}, nil)
	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	rec := rowsOf(schema,
		[]any{int64(0), "happy", []any{int32(1), int32(2), int32(3)}, day},
		[]any{int64(1), "furious", []any{int32(1), int32(2), int32(3)}, day},
		[]any{int64(2), "sad", []any{int32(1), int32(2)}, day},
		[]any{int64(3), "sad", []any{int32(1), int32(2), int32(3)}, time.Date(400000, 1, 1, 0, 0, 0, 0, time.UTC)},
		[]any{int64(4), "sad", []any{int32(4), int32(5), int32(6)}, day},
	)
	defer rec.Release()
	res, err := insert(t, be, "t", rec)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.RowsInserted)
	var refused []int64
	for _, e := range res.Errors {
		refused = append(refused, e.RowIndex)
		require.Equal(t, backend.RefusalInvalidValue, e.Reason, "row %d: %s", e.RowIndex, e.Error)
	}
	require.Equal(t, []int64{1, 2, 3}, refused)
	require.Equal(t, []any{int64(0), int64(4)}, run(t, be, `SELECT id FROM t ORDER BY id`).column(0))
}
