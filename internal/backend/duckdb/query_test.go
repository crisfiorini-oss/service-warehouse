package duckdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// render prints a decoded Arrow value the way the assertions below spell it.
func render(v any) string {
	switch v.(type) {
	case nil:
		return "NULL"
	case map[string]any, []any, json.RawMessage:
		b, _ := json.Marshal(v)
		return string(b)
	}
	return fmt.Sprint(v)
}

// arrowName spells an Arrow type without the nullability of its children, which
// every nested type here carries and the assertions need not repeat.
func arrowName(t arrow.DataType) string {
	return strings.NewReplacer(", nullable", "", " nullable", "").Replace(t.String())
}

// Every DuckDB type a result can carry, with the portable bucket and the Arrow
// type a client receives, and the value. The Arrow types are the contract: a
// client written against this engine reads the same widths on any backend.
func TestResultTypes(t *testing.T) {
	be := open(t)
	r := run(t, be, `SELECT true AS b,
	  1::TINYINT AS ti, 2::SMALLINT AS si, 3::INTEGER AS i, 4::BIGINT AS bi,
	  5::UTINYINT AS u1, 6::USMALLINT AS u2, 7::UINTEGER AS u4,
	  18446744073709551615::UBIGINT AS u8,
	  1.5::FLOAT AS f, 2.25::DOUBLE AS d,
	  12.345::DECIMAL(18,3) AS dc, -0.5::DECIMAL(4,4) AS neg, 12345678901234567890123456789.123456789::DECIMAL(38,9) AS wide,
	  'abc' AS s, 'cat'::ENUM('cat','dog') AS e, '\xAA\x01'::BLOB AS bl,
	  DATE '2024-01-02' AS dt, TIME '10:11:12.123456' AS tm,
	  TIMESTAMP '2024-01-02 03:04:05.123456' AS ts, TIMESTAMPTZ '2024-01-02 03:04:05+02' AS tstz,
	  TIMESTAMP_NS '2024-01-02 03:04:05.123456789' AS tsns, TIMESTAMP_S '2024-01-02 03:04:05' AS tss,
	  INTERVAL 1 YEAR + INTERVAL 2 DAY + INTERVAL 3 SECOND AS iv,
	  [1,2,3] AS l, [1,2,3]::INTEGER[3] AS arr, {'a': 1, 'b': 'x'} AS st, [{'k': 'v'}] AS nested,
	  99999999999999999999999999999999999999::HUGEINT AS h,
	  '6b3a1f0e-1111-4222-8333-444455556666'::UUID AS u, TIMETZ '10:11:12+02' AS ttz,
	  '101'::BIT AS bt, 'POINT(1 2)'::GEOMETRY AS g, NULL::BIGINT AS n`)
	require.Equal(t, 1, r.rows())

	type want struct {
		portable   backend.ColumnType
		native     string
		prec, scal int32
		arrow      string
		value      string
	}
	wants := map[string]want{
		"b":      {backend.TypeBool, "BOOLEAN", 0, 0, "bool", "true"},
		"ti":     {backend.TypeInt64, "TINYINT", 0, 0, "int64", "1"},
		"si":     {backend.TypeInt64, "SMALLINT", 0, 0, "int64", "2"},
		"i":      {backend.TypeInt64, "INTEGER", 0, 0, "int64", "3"},
		"bi":     {backend.TypeInt64, "BIGINT", 0, 0, "int64", "4"},
		"u1":     {backend.TypeInt64, "UTINYINT", 0, 0, "int64", "5"},
		"u2":     {backend.TypeInt64, "USMALLINT", 0, 0, "int64", "6"},
		"u4":     {backend.TypeInt64, "UINTEGER", 0, 0, "int64", "7"},
		"u8":     {backend.TypeNumeric, "UBIGINT", 20, 0, "decimal(20, 0)", "18446744073709551615"},
		"f":      {backend.TypeFloat64, "FLOAT", 0, 0, "float64", "1.5"},
		"d":      {backend.TypeFloat64, "DOUBLE", 0, 0, "float64", "2.25"},
		"dc":     {backend.TypeNumeric, "DECIMAL(18,3)", 18, 3, "decimal(18, 3)", "12.345"},
		"neg":    {backend.TypeNumeric, "DECIMAL(4,4)", 4, 4, "decimal(4, 4)", "-0.5"},
		"wide":   {backend.TypeNumeric, "DECIMAL(38,9)", 38, 9, "decimal(38, 9)", "12345678901234567890123456789.123456789"},
		"s":      {backend.TypeString, "VARCHAR", 0, 0, "utf8", "abc"},
		"e":      {backend.TypeString, "ENUM('cat', 'dog')", 0, 0, "utf8", "cat"},
		"bl":     {backend.TypeBytes, "BLOB", 0, 0, "binary", "[170 1]"},
		"dt":     {backend.TypeDate, "DATE", 0, 0, "date32", "2024-01-02"},
		"tm":     {backend.TypeTime, "TIME", 0, 0, "time64[us]", "10:11:12.123456"},
		"ts":     {backend.TypeTimestamp, "TIMESTAMP", 0, 0, "timestamp[us]", "2024-01-02T03:04:05.123456"},
		"tstz":   {backend.TypeTimestampTZ, "TIMESTAMPTZ", 0, 0, "timestamp[us, tz=UTC]", "2024-01-02T01:04:05Z"},
		"tsns":   {backend.TypeTimestamp, "TIMESTAMP_NS", 0, 0, "timestamp[ns]", "2024-01-02T03:04:05.123456789"},
		"tss":    {backend.TypeTimestamp, "TIMESTAMP_S", 0, 0, "timestamp[us]", "2024-01-02T03:04:05"},
		"iv":     {backend.TypeInterval, "INTERVAL", 0, 0, "month_day_nano_interval", "{12 2 3000000000}"},
		"l":      {backend.TypeArray, "INTEGER[]", 0, 0, "list<element: int64>", "[1,2,3]"},
		"arr":    {backend.TypeArray, "INTEGER[3]", 0, 0, "list<element: int64>", "[1,2,3]"},
		"st":     {backend.TypeStruct, `STRUCT("a" INTEGER, "b" VARCHAR)`, 0, 0, "struct<a: int64, b: utf8>", `{"a":1,"b":"x"}`},
		"nested": {backend.TypeArray, `STRUCT("k" VARCHAR)[]`, 0, 0, "list<element: struct<k: utf8>>", `[{"k":"v"}]`},
		// Types with no honest bucket are UNKNOWN, spelled exactly, and travel as text or bytes.
		"h":   {backend.TypeNumeric, "HUGEINT", 38, 0, "decimal(38, 0)", "99999999999999999999999999999999999999"},
		"u":   {backend.TypeUnknown, "UUID", 0, 0, "utf8", "6b3a1f0e-1111-4222-8333-444455556666"},
		"ttz": {backend.TypeUnknown, "TIMETZ", 0, 0, "utf8", "10:11:12+02:00"},
		"bt":  {backend.TypeUnknown, "BIT", 0, 0, "utf8", "101"},
		// DuckDB's GEOMETRY is planar. The portable GEOGRAPHY is spheroidal, so it is not claimed.
		"g": {backend.TypeUnknown, "GEOMETRY", 0, 0, "binary", "[1 1 0 0 0 0 0 0 0 0 0 240 63 0 0 0 0 0 0 0 64]"},
		"n": {backend.TypeInt64, "BIGINT", 0, 0, "int64", "NULL"},
	}
	require.Len(t, r.res.Schema, len(wants))
	for i, col := range r.res.Schema {
		w, ok := wants[col.Name]
		require.True(t, ok, "unexpected column %s", col.Name)
		require.Equal(t, w.portable, col.Type, col.Name)
		require.Equal(t, w.native, col.NativeType, col.Name)
		require.Equal(t, w.prec, col.Precision, col.Name)
		require.Equal(t, w.scal, col.Scale, col.Name)
		require.Equal(t, w.arrow, arrowName(r.recs[0].Schema().Field(i).Type), col.Name)
		require.Equal(t, w.value, render(r.column(i)[0]), col.Name)
	}

	// The portable schema nests: a list names its element, a struct its fields.
	byName := map[string]backend.Column{}
	for _, c := range r.res.Schema {
		byName[c.Name] = c
	}
	require.Len(t, byName["l"].Fields, 1)
	require.Equal(t, backend.TypeInt64, byName["l"].Fields[0].Type)
	require.Len(t, byName["st"].Fields, 2)
	require.Equal(t, "a", byName["st"].Fields[0].Name)
	require.Equal(t, backend.TypeString, byName["st"].Fields[1].Type)
	require.Equal(t, backend.TypeStruct, byName["nested"].Fields[0].Type)
}

// The header's Arrow schema and the batches must be one stream, and the portable
// schema must say the same thing as the Arrow one.
func TestHeaderSchemaMatchesBatches(t *testing.T) {
	be := open(t)
	r := run(t, be, `SELECT 1 AS a, 'x' AS b, 2.5::DECIMAL(10,2) AS c`)
	require.Len(t, r.res.Schema, 3)
	schema := r.recs[0].Schema()
	for i, c := range r.res.Schema {
		require.Equal(t, c.Name, schema.Field(i).Name)
	}
	require.Equal(t, int64(0), r.res.Job.Stats.RowsAffected)
	require.Equal(t, backend.JobDone, r.res.Job.State)
}

func TestEmptyResultStillHasASchema(t *testing.T) {
	be := open(t)
	r := run(t, be, `SELECT 1 AS a WHERE false`)
	require.Zero(t, r.rows())
	require.Len(t, r.res.Schema, 1)
	require.NotEmpty(t, r.res.ArrowSchema)
}

func TestLargeResultsComeInBatches(t *testing.T) {
	be := open(t)
	r := run(t, be, `SELECT range AS n FROM range(20000)`)
	require.Equal(t, 20000, r.rows())
	require.Greater(t, len(r.recs), 1)
	var sum int64
	for _, v := range r.column(0) {
		sum += int64(v.(int64))
	}
	require.Equal(t, int64(20000*19999/2), sum)
	// Order is preserved across batches.
	require.Equal(t, int64(0), r.column(0)[0])
	require.Equal(t, int64(19999), r.column(0)[19999])
}

// SUM over an integer column is a HUGEINT in DuckDB and an INT64 elsewhere; it
// reads as NUMERIC(38,0) here so the aggregate keeps its value and its number-ness.
func TestIntegerSumsAreNumeric(t *testing.T) {
	be := open(t)
	r := run(t, be, `SELECT sum(range) AS total FROM range(1000)`)
	require.Equal(t, backend.TypeNumeric, r.res.Schema[0].Type)
	require.Equal(t, "499500", render(r.column(0)[0]))
}

// 128 bits hold values NUMERIC(38,0) cannot, and such a value is refused.
func TestIntegerPastThirtyEightDigitsIsRefused(t *testing.T) {
	be := open(t)
	res, err := be.Query(context.Background(), `SELECT 170141183460469231731687303715884105727::HUGEINT AS h`, backend.QueryOptions{})
	require.NoError(t, err)
	defer res.Batches.Close()
	_, err = res.Batches.Next()
	require.Equal(t, serr.Unsupported, serr.CodeOf(err))
	require.ErrorContains(t, err, "38 digits")
}

// A value Arrow cannot carry exactly is refused, and the message names the column.
func TestAValueArrowCannotCarryIsRefusedWithItsColumn(t *testing.T) {
	be := open(t)
	res, err := be.Query(context.Background(), `SELECT 1 AS ok, to_microseconds(10000000000000000) AS too_long`, backend.QueryOptions{})
	require.NoError(t, err)
	defer res.Batches.Close()
	_, err = res.Batches.Next()
	require.Equal(t, serr.Unsupported, serr.CodeOf(err))
	require.ErrorContains(t, err, `column "too_long"`)
}

func TestSessionStateDoesNotLeakBetweenRequests(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TEMP TABLE scratch (a INT)`)
	require.Equal(t, serr.NotFound, code(t, be, `SELECT * FROM scratch`))
}

// --- parameters ---

func param(name string, typ backend.ColumnType, value string) backend.QueryParam {
	return backend.QueryParam{Name: name, Type: typ, Value: value}
}

func TestParametersBindAsTheirDeclaredType(t *testing.T) {
	be := open(t)
	for _, tc := range []struct {
		name     string
		p        backend.QueryParam
		portable backend.ColumnType
		native   string
		value    string
	}{
		{"bool", param("p", backend.TypeBool, "true"), backend.TypeBool, "BOOLEAN", "true"},
		{"int64", param("p", backend.TypeInt64, "-9223372036854775808"), backend.TypeInt64, "BIGINT", "-9223372036854775808"},
		{"float64", param("p", backend.TypeFloat64, "2.5"), backend.TypeFloat64, "DOUBLE", "2.5"},
		{"string", param("p", backend.TypeString, "it's"), backend.TypeString, "VARCHAR", "it's"},
		{"date", param("p", backend.TypeDate, "2024-02-29"), backend.TypeDate, "DATE", "2024-02-29"},
		{"time", param("p", backend.TypeTime, "10:11:12.5"), backend.TypeTime, "TIME", "10:11:12.5"},
		{"timestamp", param("p", backend.TypeTimestamp, "2024-01-02T03:04:05.25"), backend.TypeTimestamp, "TIMESTAMP", "2024-01-02T03:04:05.25"},
		{"timestamp with a space", param("p", backend.TypeTimestamp, "2024-01-02 03:04:05"), backend.TypeTimestamp, "TIMESTAMP", "2024-01-02T03:04:05"},
		{"timestamptz", param("p", backend.TypeTimestampTZ, "2024-01-02T03:04:05+02:00"), backend.TypeTimestampTZ, "TIMESTAMPTZ", "2024-01-02T01:04:05Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := run(t, be, `SELECT $p AS v`, backend.QueryOptions{Params: []backend.QueryParam{tc.p}})
			require.Equal(t, tc.portable, r.res.Schema[0].Type)
			require.Equal(t, tc.native, r.res.Schema[0].NativeType)
			require.Equal(t, tc.value, render(r.column(0)[0]))
		})
	}
}

func TestNullParameter(t *testing.T) {
	be := open(t)
	r := run(t, be, `SELECT $p::BIGINT AS v`, backend.QueryOptions{Params: []backend.QueryParam{{Name: "p", IsNull: true}}})
	require.Equal(t, "NULL", render(r.column(0)[0]))
}

// A parameter with no declared type is bound as text and DuckDB casts it to
// what the SQL around it expects.
func TestUntypedParametersTakeTheTypeOfTheirContext(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE people (id BIGINT, name VARCHAR, born DATE)`)
	mustExec(t, be, `INSERT INTO people VALUES (1, 'ada', DATE '1815-12-10'), (2, 'alan', DATE '1912-06-23')`)
	r := run(t, be, `SELECT name FROM people WHERE id = $id AND born < $before`, backend.QueryOptions{Params: []backend.QueryParam{
		{Name: "id", Value: "2"}, {Name: "before", Value: "1950-01-01"},
	}})
	require.Equal(t, []any{"alan"}, r.column(0))
}

func TestParameterMismatchesAreRefused(t *testing.T) {
	be := open(t)
	for _, tc := range []struct {
		name   string
		sql    string
		params []backend.QueryParam
		want   serr.Code
		msg    string
	}{
		{"missing", `SELECT $a`, nil, serr.InvalidArgument, "$a"},
		{"unused", `SELECT 1`, []backend.QueryParam{param("a", backend.TypeInt64, "1")}, serr.InvalidArgument, "not used"},
		{"at-name is not rewritten", `SELECT @a`, []backend.QueryParam{param("a", backend.TypeInt64, "1")}, serr.InvalidArgument, ""},
		{"duplicate", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeInt64, "1"), param("a", backend.TypeInt64, "2")}, serr.InvalidArgument, "twice"},
		{"unnamed", `SELECT 1`, []backend.QueryParam{param("", backend.TypeInt64, "1")}, serr.InvalidArgument, "no name"},
		{"bad int", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeInt64, "abc")}, serr.InvalidArgument, "INT64"},
		{"bad bool", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeBool, "maybe")}, serr.InvalidArgument, "BOOL"},
		{"bad float", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeFloat64, "1,5")}, serr.InvalidArgument, "FLOAT64"},
		{"bad date", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeDate, "02/29/2024")}, serr.InvalidArgument, "DATE"},
		{"bad time", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeTime, "25:00:00")}, serr.InvalidArgument, "TIME"},
		{"a zoned wall-clock timestamp", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeTimestamp, "2024-01-02T03:04:05Z")}, serr.InvalidArgument, "TIMESTAMP"},
		{"a zoneless instant", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeTimestampTZ, "2024-01-02T03:04:05")}, serr.InvalidArgument, "TIMESTAMPTZ"},
		// The contract defines no text form for these, and the driver cannot bind a DECIMAL.
		{"numeric", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeNumeric, "1.5")}, serr.Unsupported, "NUMERIC"},
		{"bytes", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeBytes, "aGk=")}, serr.Unsupported, "BYTES"},
		{"interval", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeInterval, "1 day")}, serr.Unsupported, "INTERVAL"},
		{"json", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeJSON, "{}")}, serr.Unsupported, "JSON"},
		{"array", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeArray, "[1]")}, serr.Unsupported, "ARRAY"},
		{"geography", `SELECT $a`, []backend.QueryParam{param("a", backend.TypeGeography, "POINT(1 2)")}, serr.Unsupported, "GEOGRAPHY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := be.Query(context.Background(), tc.sql, backend.QueryOptions{Params: tc.params})
			require.Error(t, err)
			require.Equal(t, tc.want, serr.CodeOf(err), err.Error())
			require.Contains(t, err.Error(), tc.msg)
		})
	}
}

// --- dry run ---

func TestDryRunDescribesWithoutRunning(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE people (id BIGINT, name VARCHAR)`)

	r := run(t, be, `SELECT id, name FROM people WHERE id > $min`, backend.QueryOptions{
		DryRun: true, Params: []backend.QueryParam{param("min", backend.TypeInt64, "0")},
	})
	require.Nil(t, r.res.Batches)
	require.Len(t, r.res.Schema, 2)
	require.Equal(t, backend.TypeInt64, r.res.Schema[0].Type)
	require.Equal(t, backend.TypeString, r.res.Schema[1].Type)
	require.NotEmpty(t, r.res.ArrowSchema)
	require.Equal(t, backend.JobDone, r.res.Job.State)
	require.Zero(t, r.res.Job.Stats.BytesBilled, "nothing is priced")

	// DML and DDL are validated, not run.
	for _, sql := range []string{
		`INSERT INTO people VALUES (1, 'ada')`,
		`DELETE FROM people`,
		`CREATE TABLE never (a INT)`,
		`DROP TABLE people`,
	} {
		res, err := be.Query(context.Background(), sql, backend.QueryOptions{DryRun: true})
		require.NoError(t, err, sql)
		require.Nil(t, res.Batches)
		require.Empty(t, res.Schema)
	}
	require.Equal(t, []any{int64(0)}, run(t, be, `SELECT count(*) FROM people`).column(0), "the dry-run INSERT must not have run")
	require.Equal(t, serr.NotFound, code(t, be, `SELECT * FROM never`))
}

func TestDryRunReportsWhatRunningWould(t *testing.T) {
	be := open(t)
	dry := backend.QueryOptions{DryRun: true}
	require.Equal(t, serr.InvalidArgument, code(t, be, `SELEC 1`, dry))
	require.Equal(t, serr.NotFound, code(t, be, `SELECT * FROM absent`, dry))
	require.Equal(t, serr.Unsupported, code(t, be, `SET threads = 1`, dry))
	// A column typed by a parameter cannot be described without a value.
	require.Equal(t, serr.Unsupported, code(t, be, `SELECT $p AS v`, backend.QueryOptions{
		DryRun: true, Params: []backend.QueryParam{param("p", backend.TypeInt64, "1")},
	}))
}

// The driver runs every statement but the last of a multi-statement text while
// preparing it. A dry run — or any request — must never reach that.
func TestMultipleStatementsAreRefusedBeforeAnyRuns(t *testing.T) {
	be := open(t)
	for _, opts := range []backend.QueryOptions{{}, {DryRun: true}} {
		require.Equal(t, serr.InvalidArgument,
			code(t, be, `CREATE TABLE smuggled (a INT); SELECT 1`, opts), "dry=%v", opts.DryRun)
		require.Equal(t, serr.NotFound, code(t, be, `SELECT * FROM smuggled`), "the first statement must not have run")
	}
	require.Equal(t, serr.InvalidArgument, code(t, be, ``))
	require.Equal(t, serr.InvalidArgument, code(t, be, `-- only a comment`))
	// A trailing semicolon is still one statement.
	require.Equal(t, []any{int64(1)}, run(t, be, `SELECT 1;`).column(0))
}

// --- statement kinds ---

func TestDDLAndDMLReportRowsChanged(t *testing.T) {
	be := open(t)
	changed := func(sql string) int64 {
		res, err := be.Query(context.Background(), sql, backend.QueryOptions{})
		require.NoError(t, err, sql)
		require.Nil(t, res.Batches, sql)
		require.Empty(t, res.Schema, sql)
		require.Empty(t, res.ArrowSchema, sql)
		require.Equal(t, backend.JobDone, res.Job.State)
		return res.Job.Stats.RowsAffected
	}
	require.Zero(t, changed(`CREATE TABLE t (id BIGINT, name VARCHAR)`))
	require.Equal(t, int64(3), changed(`INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')`))
	require.Equal(t, int64(2), changed(`UPDATE t SET name = 'z' WHERE id < 3`))
	require.Equal(t, int64(1), changed(`DELETE FROM t WHERE id = 3`))
	require.Zero(t, changed(`DELETE FROM t WHERE id = 99`))
	require.Zero(t, changed(`ALTER TABLE t ADD COLUMN extra INT`))
	require.Zero(t, changed(`DROP TABLE t`))
	// A statement with a word that looks like a clause is still just a statement.
	require.Zero(t, changed(`CREATE TABLE r (note VARCHAR)`))
	require.Equal(t, int64(1), changed(`INSERT INTO r VALUES ('returning')`))
}

// RETURNING answers a DML statement with rows, which the portable contract has
// no place for, and nothing in a prepared statement tells it from a row count.
// It is refused before the statement runs.
func TestReturningIsRefusedAndDoesNotRun(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE t (id BIGINT)`)
	for _, sql := range []string{
		`INSERT INTO t VALUES (1) RETURNING id`,
		`insert into t values (1) returning *`,
		`UPDATE t SET id = 2 RETURNING id`,
		`DELETE FROM t RETURNING id`,
	} {
		require.Equal(t, serr.Unsupported, code(t, be, sql), sql)
	}
	require.Equal(t, []any{int64(0)}, run(t, be, `SELECT count(*) FROM t`).column(0))
}

func TestOnlyPortableStatementKindsRun(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE t (id BIGINT)`)
	for _, sql := range []string{
		`SET threads = 1`,
		`PRAGMA threads = 1`,
		`BEGIN TRANSACTION`,
		`USE main`,
		`VACUUM`,
		`CALL range(3)`,
		`ATTACH ':memory:' AS other`,
		`COMMENT ON TABLE t IS 'x'`, // an ALTER: allowed below, listed here only to pin the boundary
	} {
		if strings.HasPrefix(sql, "COMMENT") {
			mustExec(t, be, sql)
			continue
		}
		require.Equal(t, serr.Unsupported, code(t, be, sql), sql)
	}
	// What is allowed.
	for _, sql := range []string{
		`SELECT 1`, `EXPLAIN SELECT 1`, `SHOW TABLES`, `DESCRIBE t`, `FROM t`, `WITH x AS (SELECT 1) SELECT * FROM x`,
		`CREATE VIEW v AS SELECT 1`, `DROP VIEW v`, `CREATE SCHEMA s`, `DROP SCHEMA s`,
	} {
		res, err := be.Query(context.Background(), sql, backend.QueryOptions{})
		require.NoError(t, err, sql)
		if res.Batches != nil {
			_ = res.Batches.Close()
		}
	}
}

// --- errors ---

// A failure reaches the client as a code and a fixed message. The message is the
// backend's, never DuckDB's: the engine's own text quotes the caller's SQL, host
// paths and the catalog.
func TestErrorsMapToCodesAndNeverLeakTheEngine(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE pk (id BIGINT PRIMARY KEY)`)
	mustExec(t, be, `INSERT INTO pk VALUES (1)`)
	mustExec(t, be, `CREATE TABLE nn (id BIGINT NOT NULL)`)

	for _, tc := range []struct {
		name string
		sql  string
		want serr.Code
	}{
		{"missing table", `SELECT * FROM absent_table`, serr.NotFound},
		// DuckDB gives no structured subtype for a missing schema in a qualified name,
		// and the message is not parsed to find one, so this is the conservative code.
		{"missing schema", `SELECT * FROM absent_schema.t`, serr.InvalidArgument},
		{"missing function", `SELECT absent_function(1)`, serr.NotFound},
		{"drop missing", `DROP TABLE absent_table`, serr.NotFound},
		{"syntax", `SELEC 1`, serr.InvalidArgument},
		{"unknown column", `SELECT absent_column FROM pk`, serr.InvalidArgument},
		{"bad cast", `SELECT CAST('x' AS INTEGER)`, serr.InvalidArgument},
		{"out of range", `SELECT 100::TINYINT * 100::TINYINT`, serr.InvalidArgument},
		{"already exists", `CREATE TABLE pk (a INT)`, serr.AlreadyExists},
		{"schema exists", `CREATE SCHEMA main`, serr.AlreadyExists},
		{"primary key", `INSERT INTO pk VALUES (1)`, serr.PreconditionFailed},
		{"not null", `INSERT INTO nn VALUES (NULL)`, serr.PreconditionFailed},
		{"host file", `SELECT * FROM read_csv('/etc/hosts')`, serr.PermissionDenied},
		{"host write", `COPY (SELECT 1) TO '/tmp/swh-should-not-exist.csv'`, serr.PermissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := be.Query(context.Background(), tc.sql, backend.QueryOptions{})
			if err == nil && res.Batches != nil {
				// A failure at execution surfaces on the first read.
				_, err = res.Batches.Next()
				_ = res.Batches.Close()
			}
			require.Error(t, err)
			require.Equal(t, tc.want, serr.CodeOf(err), err.Error())
			msg := err.Error()
			for _, leak := range []string{"Error:", "LINE 1", "Did you mean", "/etc/hosts", "/tmp/", "DuckDB", "duckdb_", "pg_"} {
				require.NotContains(t, msg, leak, "engine text reached the client")
			}
		})
	}
}

// The catalog lookup in the message is the caller's own name, not a suggestion
// from the engine.
func TestNotFoundNamesWhatTheCallerAskedFor(t *testing.T) {
	be := open(t)
	_, err := be.Query(context.Background(), `SELECT * FROM absent_table`, backend.QueryOptions{})
	require.ErrorContains(t, err, `absent_table`)
	require.NotContains(t, err.Error(), "Did you mean")
}

// --- limits ---

func TestByteCapsAreRefusedNotIgnored(t *testing.T) {
	be := open(t)
	require.Equal(t, serr.Unsupported, code(t, be, `SELECT 1`, backend.QueryOptions{MaxBytesBilled: 1 << 20}))

	_, err := Open(context.Background(), backend.Config{Kind: "duckdb", MaxQueryBytes: 1 << 20})
	require.Equal(t, serr.Unsupported, serr.CodeOf(err))
	require.ErrorContains(t, err, "SWH_MAX_QUERY_BYTES")
}

func TestTimeoutInterruptsAQuery(t *testing.T) {
	be := open(t)
	slow := `SELECT count(*) FROM range(100000000000) a, range(100000000000) b`

	start := time.Now()
	require.Equal(t, serr.DeadlineExceeded, code(t, be, slow, backend.QueryOptions{Timeout: 150 * time.Millisecond}))
	require.Less(t, time.Since(start), 10*time.Second)

	// SWH_QUERY_TIMEOUT applies when the request names none.
	capped := open(t, func(c *backend.Config) { c.QueryTimeout = 150 * time.Millisecond })
	start = time.Now()
	require.Equal(t, serr.DeadlineExceeded, code(t, capped, slow))
	require.Less(t, time.Since(start), 10*time.Second)
}

func TestTheOperatorsTimeoutBeatsALongerRequest(t *testing.T) {
	be := open(t, func(c *backend.Config) { c.QueryTimeout = time.Second })
	require.Equal(t, time.Second, be.timeout(time.Hour))
	require.Equal(t, 100*time.Millisecond, be.timeout(100*time.Millisecond))
	require.Equal(t, time.Second, be.timeout(0))
	require.Equal(t, 5*time.Second, open(t).timeout(5*time.Second))
	require.Zero(t, open(t).timeout(0))
}

func TestACancelledRequestStopsTheQuery(t *testing.T) {
	be := open(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := be.Query(ctx, `SELECT count(*) FROM range(100000000000) a, range(100000000000) b`, backend.QueryOptions{})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the query did not stop when its request was cancelled")
	}
}

// --- JSON ---

// A JSON column cannot be read back exactly through the driver, so reading one
// is refused; the stored text is available by casting it.
func TestJSONColumnsAreRefusedUntilTheyCanBeExact(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE docs (id BIGINT, body JSON)`)
	mustExec(t, be, `INSERT INTO docs VALUES (1, '{"id": 9007199254740993, "k": [1, 2]}'), (2, 'null')`)

	res, err := be.Query(context.Background(), `SELECT body FROM docs`, backend.QueryOptions{})
	require.Nil(t, res)
	require.Equal(t, serr.Unsupported, serr.CodeOf(err))
	require.ErrorContains(t, err, "cast")
	require.ErrorContains(t, err, `"body"`)

	// Nested inside a list or struct it is refused the same way.
	require.Equal(t, serr.Unsupported, code(t, be, `SELECT [body] FROM docs`))

	r := run(t, be, `SELECT CAST(body AS VARCHAR) AS body FROM docs ORDER BY id`)
	require.Equal(t, backend.TypeString, r.res.Schema[0].Type)
	require.Equal(t, []any{`{"id": 9007199254740993, "k": [1, 2]}`, `null`}, r.column(0))
}

// --- concurrency ---

func TestConcurrentRequests(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE counts (worker BIGINT, n BIGINT)`)

	const workers, perWorker = 8, 25
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			for i := 0; i < perWorker; i++ {
				if _, err := be.Query(context.Background(), fmt.Sprintf(`INSERT INTO counts VALUES (%d, %d)`, w, i), backend.QueryOptions{}); err != nil {
					errs <- err
					return
				}
				res, err := be.Query(context.Background(), `SELECT count(*) FROM counts`, backend.QueryOptions{})
				if err != nil {
					errs <- err
					return
				}
				for {
					if _, err := res.Batches.Next(); err == io.EOF {
						break
					} else if err != nil {
						errs <- err
						return
					}
				}
				_ = res.Batches.Close()
			}
			errs <- nil
		}(w)
	}
	for w := 0; w < workers; w++ {
		require.NoError(t, <-errs)
	}
	require.Equal(t, []any{int64(workers * perWorker)}, run(t, be, `SELECT count(*) FROM counts`).column(0))
}
