package duckdb

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

func scan(t *testing.T, be backend.Backend, table string, opts backend.ScanOptions) *result {
	t.Helper()
	res, err := be.Scan(context.Background(), backend.TableRef{Dataset: "main", Table: table}, opts)
	require.NoError(t, err)
	return drain(t, res)
}

func scanErr(t *testing.T, be backend.Backend, table string, opts backend.ScanOptions) error {
	t.Helper()
	res, err := be.Scan(context.Background(), backend.TableRef{Dataset: "main", Table: table}, opts)
	if err == nil {
		_ = res.Batches.Close()
		t.Fatal("expected the scan to fail")
	}
	return err
}

func is(code serr.Code, err error) bool {
	var se *serr.Error
	return errors.As(err, &se) && se.Code == code
}

func pred(field string, op backend.PredicateOp, typ backend.ColumnType, values ...string) backend.Predicate {
	return backend.Predicate{Field: field, Op: op, Type: typ, Values: values}
}

func filter(ps ...backend.Predicate) backend.ScanOptions { return backend.ScanOptions{Filter: ps} }

// events is a table with a column of every type a filter can compare.
func events(t *testing.T) *Backend {
	be := open(t)
	mustExec(t, be, `CREATE TABLE events (
		id BIGINT, name VARCHAR, score DOUBLE, active BOOLEAN, day DATE, at_time TIME,
		wall TIMESTAMP, instant TIMESTAMPTZ, note VARCHAR)`)
	mustExec(t, be, `INSERT INTO events VALUES
		(1, 'alpha',  1.5, true,  DATE '2024-01-01', TIME '01:00:00', TIMESTAMP '2024-01-01 01:00:00', TIMESTAMPTZ '2024-01-01 01:00:00+00', NULL),
		(2, 'beta',   2.5, false, DATE '2024-02-01', TIME '02:00:00', TIMESTAMP '2024-02-01 02:00:00', TIMESTAMPTZ '2024-02-01 02:00:00+00', 'x'),
		(3, 'gamma',  3.5, true,  DATE '2024-03-01', TIME '03:00:00', TIMESTAMP '2024-03-01 03:00:00', TIMESTAMPTZ '2024-03-01 03:00:00+00', 'y'),
		(4, 'delta',  4.5, false, DATE '2024-04-01', TIME '04:00:00', TIMESTAMP '2024-04-01 04:00:00', TIMESTAMPTZ '2024-04-01 04:00:00+00', NULL)`)
	return be
}

func ids(t *testing.T, r *result) []any {
	t.Helper()
	return r.column(0)
}

func TestScanOperatorsAndTypes(t *testing.T) {
	be := events(t)
	sel := func(p backend.Predicate) []any {
		return ids(t, scan(t, be, "events", backend.ScanOptions{Selected: []string{"id"}, Filter: []backend.Predicate{p}}))
	}
	for _, tc := range []struct {
		name string
		p    backend.Predicate
		want []any
	}{
		{"EQ int", pred("id", backend.OpEq, backend.TypeInt64, "2"), []any{int64(2)}},
		{"EQ string", pred("name", backend.OpEq, backend.TypeString, "gamma"), []any{int64(3)}},
		{"EQ bool", pred("active", backend.OpEq, backend.TypeBool, "false"), []any{int64(2), int64(4)}},
		{"EQ float", pred("score", backend.OpEq, backend.TypeFloat64, "3.5"), []any{int64(3)}},
		{"IN ints", pred("id", backend.OpIn, backend.TypeInt64, "1", "4", "99"), []any{int64(1), int64(4)}},
		{"IN strings", pred("name", backend.OpIn, backend.TypeString, "alpha", "beta"), []any{int64(1), int64(2)}},
		{"LT date", pred("day", backend.OpLt, backend.TypeDate, "2024-02-01"), []any{int64(1)}},
		{"LE date", pred("day", backend.OpLe, backend.TypeDate, "2024-02-01"), []any{int64(1), int64(2)}},
		{"GT time", pred("at_time", backend.OpGt, backend.TypeTime, "03:00:00"), []any{int64(4)}},
		{"GE time", pred("at_time", backend.OpGe, backend.TypeTime, "03:00:00"), []any{int64(3), int64(4)}},
		{"GE wall timestamp", pred("wall", backend.OpGe, backend.TypeTimestamp, "2024-03-01T03:00:00"), []any{int64(3), int64(4)}},
		{"LT instant", pred("instant", backend.OpLt, backend.TypeTimestampTZ, "2024-02-01T02:00:00Z"), []any{int64(1)}},
		{"instant in another zone", pred("instant", backend.OpEq, backend.TypeTimestampTZ, "2024-02-01T04:00:00+02:00"), []any{int64(2)}},
		{"GT float", pred("score", backend.OpGt, backend.TypeFloat64, "2.5"), []any{int64(3), int64(4)}},
		{"IS_NULL", pred("note", backend.OpIsNull, backend.TypeUnknown), []any{int64(1), int64(4)}},
		{"IS_NOT_NULL", pred("note", backend.OpIsNotNull, backend.TypeUnknown), []any{int64(2), int64(3)}},
		{"a column named in another case", pred("NAME", backend.OpEq, backend.TypeString, "beta"), []any{int64(2)}},
		{"no row matches", pred("id", backend.OpEq, backend.TypeInt64, "99"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sel(tc.p))
		})
	}
}

func TestScanFilterIsAConjunction(t *testing.T) {
	be := events(t)
	got := scan(t, be, "events", backend.ScanOptions{
		Selected: []string{"id"},
		Filter: []backend.Predicate{
			pred("active", backend.OpEq, backend.TypeBool, "true"),
			pred("id", backend.OpGt, backend.TypeInt64, "1"),
			pred("note", backend.OpIsNotNull, backend.TypeUnknown),
		},
	})
	require.Equal(t, []any{int64(3)}, ids(t, got))
}

func TestScanSelectedFieldsKeepTheRequestedOrder(t *testing.T) {
	be := events(t)
	r := scan(t, be, "events", backend.ScanOptions{
		Selected: []string{"name", "id"},
		Filter:   []backend.Predicate{pred("id", backend.OpEq, backend.TypeInt64, "2")},
	})
	require.Equal(t, []string{"name", "id"}, []string{r.res.Schema[0].Name, r.res.Schema[1].Name})
	require.Equal(t, []any{"beta"}, r.column(0))
	require.Equal(t, []any{int64(2)}, r.column(1))
	// The Arrow schema agrees with the portable one.
	require.Equal(t, 2, len(r.recs[0].Schema().Fields()))
	require.Equal(t, "name", r.recs[0].Schema().Field(0).Name)
}

func TestScanWithNoSelectionAndNoFilterReadsEverything(t *testing.T) {
	be := events(t)
	r := scan(t, be, "events", backend.ScanOptions{})
	require.Equal(t, 4, r.rows())
	require.Len(t, r.res.Schema, 9)
	require.Equal(t, "id", r.res.Schema[0].Name, "the table's own order")
	require.Empty(t, r.res.Job.ID, "a scan has no job to look up")
}

func TestScanReadsAViewAndTheStoredSpelling(t *testing.T) {
	be := events(t)
	mustExec(t, be, `CREATE VIEW "Even Events" AS SELECT * FROM events WHERE id % 2 = 0`)
	r := scan(t, be, "even events", backend.ScanOptions{Selected: []string{"ID"}})
	require.Equal(t, []any{int64(2), int64(4)}, r.column(0))
	require.Equal(t, "id", r.res.Schema[0].Name, "the table's spelling, not the request's")
}

// Identifiers are quoted: a table and a column whose names are SQL are read as
// names.
func TestScanQuotesIdentifiers(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE "odd ""table" ("select" BIGINT, "we""ird col" VARCHAR, "a b" BIGINT)`)
	mustExec(t, be, `INSERT INTO "odd ""table" VALUES (1, 'one', 10), (2, 'two', 20)`)
	r := scan(t, be, `odd "table`, backend.ScanOptions{
		Selected: []string{`we"ird col`, "select"},
		Filter:   []backend.Predicate{pred(`a b`, backend.OpGe, backend.TypeInt64, "20")},
	})
	require.Equal(t, []any{"two"}, r.column(0))
	require.Equal(t, []any{int64(2)}, r.column(1))
}

// A value is bound, never spliced: text that would end a literal, comment out
// the rest or start a second statement is only a value, and finds the row that
// holds exactly that text.
func TestScanBindsValuesNeverSplicesThem(t *testing.T) {
	be := open(t)
	mustExec(t, be, `CREATE TABLE notes (id BIGINT, body VARCHAR)`)
	hostile := []string{
		`it's`,
		`say "hi"`,
		`back\slash\`,
		`'; DROP TABLE notes; --`,
		`\'; DROP TABLE notes; --`,
		`$1 and $p0`,
		`line` + "\n" + `break`,
		"tab\tand\x01control",
		`'' OR '1'='1`,
	}
	for i, v := range hostile {
		_, err := be.Query(context.Background(), `INSERT INTO notes VALUES ($id, $body)`, backend.QueryOptions{Params: []backend.QueryParam{
			{Name: "id", Type: backend.TypeInt64, Value: string(rune('0' + i))},
			{Name: "body", Type: backend.TypeString, Value: v},
		}})
		require.NoError(t, err)
	}
	for i, v := range hostile {
		r := scan(t, be, "notes", backend.ScanOptions{
			Selected: []string{"id"},
			Filter:   []backend.Predicate{pred("body", backend.OpEq, backend.TypeString, v)},
		})
		require.Equal(t, []any{int64(i)}, r.column(0), "value %q", v)
	}
	r := scan(t, be, "notes", backend.ScanOptions{
		Selected: []string{"id"},
		Filter:   []backend.Predicate{pred("body", backend.OpIn, backend.TypeString, hostile...)},
	})
	require.Equal(t, len(hostile), r.rows())
	require.Equal(t, int64(len(hostile)), rowCount(t, be, "notes"), "no statement of a value ran")

	// The SQL a scan runs holds no value at all.
	cols := []tableColumn{{name: "id"}, {name: "body"}}
	sql, params, err := scanSQL("memory", backend.TableRef{Dataset: "main", Table: "notes"}, cols,
		filter(pred("body", backend.OpIn, backend.TypeString, hostile...)))
	require.NoError(t, err)
	require.Len(t, params, len(hostile))
	for _, v := range hostile {
		require.NotContains(t, sql, v)
	}
}

func TestScanRefusesWhatTheTableLacks(t *testing.T) {
	be := events(t)
	for name, opts := range map[string]backend.ScanOptions{
		"filter on an unknown column":   filter(pred("nope", backend.OpEq, backend.TypeInt64, "1")),
		"select an unknown column":      {Selected: []string{"id", "nope"}},
		"IS NULL on an unknown column":  filter(pred("nope", backend.OpIsNull, backend.TypeUnknown)),
		"a column that is only a quote": filter(pred(`"`, backend.OpIsNull, backend.TypeUnknown)),
	} {
		t.Run(name, func(t *testing.T) {
			err := scanErr(t, be, "events", opts)
			require.True(t, is(serr.InvalidArgument, err), "%v", err)
		})
	}
	err := scanErr(t, be, "missing", backend.ScanOptions{})
	require.True(t, is(serr.NotFound, err), "%v", err)
	_, err = be.Scan(context.Background(), backend.TableRef{Dataset: "nowhere", Table: "events"}, backend.ScanOptions{})
	require.True(t, is(serr.NotFound, err), "%v", err)
}

func TestScanRefusesAMalformedRequest(t *testing.T) {
	be := events(t)
	for name, opts := range map[string]backend.ScanOptions{
		"no operator":              filter(backend.Predicate{Field: "id", Type: backend.TypeInt64, Values: []string{"1"}}),
		"no field":                 filter(pred("", backend.OpEq, backend.TypeInt64, "1")),
		"EQ with two values":       filter(pred("id", backend.OpEq, backend.TypeInt64, "1", "2")),
		"EQ with none":             filter(pred("id", backend.OpEq, backend.TypeInt64)),
		"IN with none":             filter(pred("id", backend.OpIn, backend.TypeInt64)),
		"values without a type":    filter(pred("id", backend.OpEq, backend.TypeUnknown, "1")),
		"IS NULL with a value":     filter(pred("id", backend.OpIsNull, backend.TypeUnknown, "1")),
		"IS NULL with a type":      filter(pred("id", backend.OpIsNull, backend.TypeInt64)),
		"a malformed integer":      filter(pred("id", backend.OpEq, backend.TypeInt64, "1 OR 1=1")),
		"a zoned wall-clock value": filter(pred("wall", backend.OpEq, backend.TypeTimestamp, "2024-01-01T01:00:00Z")),
		"a selected field twice":   {Selected: []string{"id", "ID"}},
		"an empty selected field":  {Selected: []string{""}},
	} {
		t.Run(name, func(t *testing.T) {
			err := scanErr(t, be, "events", opts)
			require.True(t, is(serr.InvalidArgument, err), "%v", err)
		})
	}
	for name, p := range map[string]backend.Predicate{
		"an ARRAY value":  pred("id", backend.OpEq, backend.TypeArray, "1"),
		"a NUMERIC value": pred("id", backend.OpEq, backend.TypeNumeric, "1.5"),
		"a BYTES value":   pred("name", backend.OpEq, backend.TypeBytes, "YQ=="),
		"an INTERVAL":     pred("id", backend.OpEq, backend.TypeInterval, "1"),
		"a JSON value":    pred("id", backend.OpEq, backend.TypeJSON, "1"),
	} {
		t.Run(name, func(t *testing.T) {
			err := scanErr(t, be, "events", filter(p))
			require.True(t, is(serr.Unsupported, err), "%v", err)
		})
	}
}

func TestScanIsOfferedAndStreamsAreRefused(t *testing.T) {
	be := open(t)
	require.True(t, be.Capabilities().TableScan)

	_, err := Open(context.Background(), backend.Config{Kind: "duckdb", ScanStreams: 4})
	require.True(t, is(serr.Unsupported, err), "%v", err)
}

func TestScanStopsWhenItsContextIsDone(t *testing.T) {
	be := events(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := be.Scan(ctx, backend.TableRef{Dataset: "main", Table: "events"}, backend.ScanOptions{})
	require.Error(t, err)
}
