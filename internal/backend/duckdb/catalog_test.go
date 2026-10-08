package duckdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

func TestDatasetLifecycle(t *testing.T) {
	ctx := context.Background()
	be := open(t)

	// A fresh database has its default dataset and nothing the engine owns.
	list, err := be.ListDatasets(ctx, backend.ListDatasetsOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"main"}, datasetNames(list.Datasets))

	ds, err := be.CreateDataset(ctx, "analytics", backend.CreateDatasetOptions{})
	require.NoError(t, err)
	require.Equal(t, "analytics", ds.Name)

	_, err = be.CreateDataset(ctx, "analytics", backend.CreateDatasetOptions{})
	require.Equal(t, serr.AlreadyExists, serr.CodeOf(err))
	_, err = be.CreateDataset(ctx, "analytics", backend.CreateDatasetOptions{IfNotExists: true})
	require.NoError(t, err)
	_, err = be.CreateDataset(ctx, "main", backend.CreateDatasetOptions{})
	require.Equal(t, serr.AlreadyExists, serr.CodeOf(err), "the default dataset exists")

	list, err = be.ListDatasets(ctx, backend.ListDatasetsOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"analytics", "main"}, datasetNames(list.Datasets))

	require.NoError(t, be.DropDataset(ctx, "analytics", backend.DropDatasetOptions{}))
	require.Equal(t, serr.NotFound, serr.CodeOf(be.DropDataset(ctx, "analytics", backend.DropDatasetOptions{})))
	require.NoError(t, be.DropDataset(ctx, "analytics", backend.DropDatasetOptions{IfExists: true}))
	require.Equal(t, serr.PreconditionFailed, serr.CodeOf(be.DropDataset(ctx, "main", backend.DropDatasetOptions{})))
}

func datasetNames(ds []backend.Dataset) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.Name
	}
	return out
}

func TestDropDatasetNeedsCascadeToDropWhatItHolds(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	_, err := be.CreateDataset(ctx, "ds", backend.CreateDatasetOptions{})
	require.NoError(t, err)
	ref := backend.TableRef{Dataset: "ds", Table: "t"}
	_, err = be.CreateTable(ctx, ref, backend.CreateTableOptions{Columns: []backend.Column{{Name: "a", Type: backend.TypeInt64, Nullable: true}}})
	require.NoError(t, err)

	require.Equal(t, serr.PreconditionFailed, serr.CodeOf(be.DropDataset(ctx, "ds", backend.DropDatasetOptions{})))
	_, err = be.GetTable(ctx, ref)
	require.NoError(t, err, "a refused drop leaves the table")

	require.NoError(t, be.DropDataset(ctx, "ds", backend.DropDatasetOptions{Cascade: true}))
	require.Equal(t, serr.NotFound, serr.CodeOf(be.DropDataset(ctx, "ds", backend.DropDatasetOptions{})))
}

// What a dataset has in other warehouses and DuckDB does not is refused, not
// accepted and dropped.
func TestDatasetOptionsDuckDBCannotHonorAreRefused(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	for name, opts := range map[string]backend.CreateDatasetOptions{
		"location":    {Location: "eu"},
		"labels":      {Labels: map[string]string{"env": "dev"}},
		"description": {Description: "events"},
	} {
		_, err := be.CreateDataset(ctx, "x", opts)
		require.Equal(t, serr.Unsupported, serr.CodeOf(err), name)
	}
	list, err := be.ListDatasets(ctx, backend.ListDatasetsOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"main"}, datasetNames(list.Datasets), "a refused create creates nothing")
}

func TestNamesAreCheckedAndQuoted(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	for _, bad := range []string{"", "a\x00b", "\xff"} {
		_, err := be.CreateDataset(ctx, bad, backend.CreateDatasetOptions{})
		require.Equal(t, serr.InvalidArgument, serr.CodeOf(err), "%q", bad)
	}
	// A name that would be SQL if it were not quoted is just a name.
	hostile := `x"; DROP SCHEMA main; --`
	_, err := be.CreateDataset(ctx, hostile, backend.CreateDatasetOptions{})
	require.NoError(t, err)
	ref := backend.TableRef{Dataset: hostile, Table: `t"; DROP TABLE x; --`}
	_, err = be.CreateTable(ctx, ref, backend.CreateTableOptions{Columns: []backend.Column{{Name: `c" INT); DROP SCHEMA main; --`, Type: backend.TypeInt64, Nullable: true}}})
	require.NoError(t, err)
	got, err := be.GetTable(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, `c" INT); DROP SCHEMA main; --`, got.Columns[0].Name)

	list, err := be.ListDatasets(ctx, backend.ListDatasetsOptions{})
	require.NoError(t, err)
	require.Contains(t, datasetNames(list.Datasets), "main")
	require.Contains(t, datasetNames(list.Datasets), hostile)
}

// Every portable type DuckDB can store goes out through CreateTable and comes
// back from GetTable as the same portable type.
func TestPortableTypesRoundTripThroughDDL(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	ref := backend.TableRef{Dataset: "main", Table: "all_types"}
	in := []backend.Column{
		{Name: "b", Type: backend.TypeBool},
		{Name: "i", Type: backend.TypeInt64},
		{Name: "f", Type: backend.TypeFloat64},
		{Name: "n_default", Type: backend.TypeNumeric},
		{Name: "n", Type: backend.TypeNumeric, Precision: 12, Scale: 3},
		{Name: "s", Type: backend.TypeString},
		{Name: "by", Type: backend.TypeBytes},
		{Name: "d", Type: backend.TypeDate},
		{Name: "t", Type: backend.TypeTime},
		{Name: "ts", Type: backend.TypeTimestamp},
		{Name: "tz", Type: backend.TypeTimestampTZ},
		{Name: "iv", Type: backend.TypeInterval},
		{Name: "j", Type: backend.TypeJSON},
		{Name: "arr", Type: backend.TypeArray, Fields: []backend.Column{{Type: backend.TypeInt64}}},
		{Name: "st", Type: backend.TypeStruct, Fields: []backend.Column{
			{Name: "a", Type: backend.TypeString},
			{Name: "tags", Type: backend.TypeArray, Fields: []backend.Column{{Type: backend.TypeString}}},
		}},
	}
	for i := range in {
		in[i].Nullable = i%2 == 0
	}
	in[3].Description = "the default NUMERIC"

	created, err := be.CreateTable(ctx, ref, backend.CreateTableOptions{Columns: in})
	require.NoError(t, err)
	got, err := be.GetTable(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, created.Columns, got.Columns)
	require.Len(t, got.Columns, len(in))

	for i, want := range in {
		col := got.Columns[i]
		require.Equal(t, want.Name, col.Name)
		require.Equal(t, want.Type, col.Type, want.Name)
		require.Equal(t, want.Nullable, col.Nullable, want.Name)
	}
	require.Equal(t, int32(38), got.Columns[3].Precision, "NUMERIC with no precision is the portable default (38,9)")
	require.Equal(t, int32(9), got.Columns[3].Scale)
	require.Equal(t, "the default NUMERIC", got.Columns[3].Description)
	require.Equal(t, int32(12), got.Columns[4].Precision)
	require.Equal(t, int32(3), got.Columns[4].Scale)
	require.Equal(t, backend.TypeInt64, got.Columns[13].Fields[0].Type)
	require.Equal(t, "a", got.Columns[14].Fields[0].Name)
	require.Equal(t, backend.TypeArray, got.Columns[14].Fields[1].Type)
	require.Equal(t, backend.KindTable, got.Info.Kind)
	require.Equal(t, "all_types", got.Info.Ref.Table)
}

func TestColumnsDuckDBCannotStoreAreRefused(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	ref := backend.TableRef{Dataset: "main", Table: "t"}
	for name, col := range map[string]backend.Column{
		"geography": {Name: "g", Type: backend.TypeGeography},
		"unknown":   {Name: "u", Type: backend.TypeUnknown, NativeType: "VARCHAR); DROP SCHEMA main; --"},
	} {
		_, err := be.CreateTable(ctx, ref, backend.CreateTableOptions{Columns: []backend.Column{col}})
		require.Equal(t, serr.Unsupported, serr.CodeOf(err), name)
	}
	_, err := be.GetTable(ctx, ref)
	require.Equal(t, serr.NotFound, serr.CodeOf(err), "a refused create creates nothing")

	for name, col := range map[string]backend.Column{
		"numeric precision too large":  {Name: "n", Type: backend.TypeNumeric, Precision: 39, Scale: 2},
		"numeric scale over precision": {Name: "n", Type: backend.TypeNumeric, Precision: 5, Scale: 6},
		"array with no element":        {Name: "a", Type: backend.TypeArray},
		"struct with no fields":        {Name: "s", Type: backend.TypeStruct},
		"empty column name":            {Name: "", Type: backend.TypeInt64},
	} {
		_, err := be.CreateTable(ctx, ref, backend.CreateTableOptions{Columns: []backend.Column{col}})
		require.Equal(t, serr.InvalidArgument, serr.CodeOf(err), name)
	}
}

func TestTableOptionsDuckDBCannotHonorAreRefused(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	ref := backend.TableRef{Dataset: "main", Table: "t"}
	cols := []backend.Column{{Name: "a", Type: backend.TypeInt64}}
	for name, opts := range map[string]backend.CreateTableOptions{
		"partition_by": {Columns: cols, PartitionBy: []string{"a"}},
		"cluster_by":   {Columns: cols, ClusterBy: []string{"a"}},
	} {
		_, err := be.CreateTable(ctx, ref, opts)
		require.Equal(t, serr.Unsupported, serr.CodeOf(err), name)
	}
	for name, opts := range map[string]backend.CreateTableOptions{
		"no columns":                  {},
		"columns and as_select":       {Columns: cols, AsSelect: "SELECT 1"},
		"duplicate column":            {Columns: []backend.Column{{Name: "a", Type: backend.TypeInt64}, {Name: "A", Type: backend.TypeInt64}}},
		"as_select that is not one":   {AsSelect: "CREATE TABLE y (a INT)"},
		"as_select with a second one": {AsSelect: "SELECT 1; SELECT 2"},
		"as_select with a parameter":  {AsSelect: "SELECT $a"},
	} {
		_, err := be.CreateTable(ctx, ref, opts)
		require.Equal(t, serr.InvalidArgument, serr.CodeOf(err), name)
	}
}

func TestTableLifecycle(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	_, err := be.CreateDataset(ctx, "ds", backend.CreateDatasetOptions{})
	require.NoError(t, err)
	ref := backend.TableRef{Dataset: "ds", Table: "events"}
	cols := []backend.Column{{Name: "id", Type: backend.TypeInt64}, {Name: "name", Type: backend.TypeString, Nullable: true}}

	_, err = be.CreateTable(ctx, backend.TableRef{Dataset: "absent", Table: "t"}, backend.CreateTableOptions{Columns: cols})
	require.Equal(t, serr.NotFound, serr.CodeOf(err))

	created, err := be.CreateTable(ctx, ref, backend.CreateTableOptions{Columns: cols})
	require.NoError(t, err)
	require.Equal(t, ref, created.Info.Ref)

	_, err = be.CreateTable(ctx, ref, backend.CreateTableOptions{Columns: cols})
	require.Equal(t, serr.AlreadyExists, serr.CodeOf(err))
	again, err := be.CreateTable(ctx, ref, backend.CreateTableOptions{Columns: cols, IfNotExists: true})
	require.NoError(t, err)
	require.Equal(t, created.Columns, again.Columns)

	// An unset Nullable is NOT NULL, as the contract spells it.
	mustExec(t, be, `INSERT INTO ds.events VALUES (1, NULL)`)
	require.Equal(t, serr.PreconditionFailed, code(t, be, `INSERT INTO ds.events VALUES (NULL, 'x')`))

	mustExec(t, be, `CREATE VIEW ds.recent AS SELECT * FROM ds.events`)
	tables, err := be.ListTables(ctx, backend.ListTablesOptions{Dataset: "ds"})
	require.NoError(t, err)
	require.Len(t, tables.Tables, 2)
	require.Equal(t, "events", tables.Tables[0].Ref.Table)
	require.Equal(t, backend.KindTable, tables.Tables[0].Kind)
	require.Equal(t, int64(1), tables.Tables[0].NumRows)
	require.Equal(t, "recent", tables.Tables[1].Ref.Table)
	require.Equal(t, backend.KindView, tables.Tables[1].Kind)

	view, err := be.GetTable(ctx, backend.TableRef{Dataset: "ds", Table: "recent"})
	require.NoError(t, err)
	require.Equal(t, backend.KindView, view.Info.Kind)
	require.Len(t, view.Columns, 2)

	require.NoError(t, be.DropTable(ctx, backend.TableRef{Dataset: "ds", Table: "recent"}, backend.DropTableOptions{}), "a view drops through DropTable")
	require.NoError(t, be.DropTable(ctx, ref, backend.DropTableOptions{}))
	require.Equal(t, serr.NotFound, serr.CodeOf(be.DropTable(ctx, ref, backend.DropTableOptions{})))
	require.NoError(t, be.DropTable(ctx, ref, backend.DropTableOptions{IfExists: true}))
	_, err = be.GetTable(ctx, ref)
	require.Equal(t, serr.NotFound, serr.CodeOf(err))
	_, err = be.ListTables(ctx, backend.ListTablesOptions{Dataset: "absent"})
	require.Equal(t, serr.NotFound, serr.CodeOf(err))
}

func TestCreateTableAsSelect(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	ref := backend.TableRef{Dataset: "main", Table: "squares"}
	got, err := be.CreateTable(ctx, ref, backend.CreateTableOptions{AsSelect: `SELECT range AS n, range * range AS sq FROM range(5)`})
	require.NoError(t, err)
	require.Len(t, got.Columns, 2, "columns are inferred")
	require.Equal(t, backend.TypeInt64, got.Columns[0].Type)
	require.Equal(t, "30", render(run(t, be, `SELECT sum(sq) FROM squares`).column(0)[0]))
}

func TestListingsPage(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		_, err := be.CreateDataset(ctx, name, backend.CreateDatasetOptions{})
		require.NoError(t, err)
		_, err = be.CreateTable(ctx, backend.TableRef{Dataset: "main", Table: "t_" + name},
			backend.CreateTableOptions{Columns: []backend.Column{{Name: "a", Type: backend.TypeInt64}}})
		require.NoError(t, err)
	}

	var seen []string
	token := ""
	for pages := 0; ; pages++ {
		res, err := be.ListDatasets(ctx, backend.ListDatasetsOptions{Limit: 2, PageToken: token})
		require.NoError(t, err)
		require.LessOrEqual(t, len(res.Datasets), 2)
		seen = append(seen, datasetNames(res.Datasets)...)
		if res.NextPageToken == "" {
			break
		}
		token = res.NextPageToken
		require.Less(t, pages, 10)
	}
	require.Equal(t, []string{"a", "b", "c", "d", "e", "main"}, seen)

	seen, token = nil, ""
	for {
		res, err := be.ListTables(ctx, backend.ListTablesOptions{Dataset: "main", Limit: 3, PageToken: token})
		require.NoError(t, err)
		for _, ti := range res.Tables {
			seen = append(seen, ti.Ref.Table)
		}
		if res.NextPageToken == "" {
			break
		}
		token = res.NextPageToken
	}
	require.Equal(t, []string{"t_a", "t_b", "t_c", "t_d", "t_e"}, seen)
}

// Unqualified names in the caller's SQL resolve in the request's dataset, else
// SWH_DATASET.
func TestDefaultDataset(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	_, err := be.CreateDataset(ctx, "analytics", backend.CreateDatasetOptions{})
	require.NoError(t, err)
	mustExec(t, be, `CREATE TABLE analytics.events (id BIGINT)`)
	mustExec(t, be, `INSERT INTO analytics.events VALUES (1), (2)`)

	require.Equal(t, serr.NotFound, code(t, be, `SELECT * FROM events`))
	r := run(t, be, `SELECT count(*) FROM events`, backend.QueryOptions{DefaultDataset: "analytics"})
	require.Equal(t, []any{int64(2)}, r.column(0))
	require.Equal(t, serr.NotFound, code(t, be, `SELECT 1`, backend.QueryOptions{DefaultDataset: "absent"}))

	// A configured default dataset must exist: names resolve in it, and running
	// SQL against a search path that is not there would resolve them elsewhere.
	configured := open(t, func(c *backend.Config) { c.DefaultDataset = "analytics" })
	require.Equal(t, serr.NotFound, code(t, configured, `SELECT 1`))
	_, err = configured.CreateDataset(ctx, "analytics", backend.CreateDatasetOptions{})
	require.NoError(t, err)
	mustExec(t, configured, `CREATE TABLE events (id BIGINT)`)
	require.Equal(t, []any{int64(0)}, run(t, configured, `SELECT count(*) FROM events`).column(0))
	// A catalog call that names no dataset uses the configured one too.
	got, err := configured.GetTable(ctx, backend.TableRef{Table: "events"})
	require.NoError(t, err)
	require.Equal(t, "analytics", got.Info.Ref.Dataset)
	_, err = be.GetTable(ctx, backend.TableRef{Table: "events"})
	require.Equal(t, serr.NotFound, serr.CodeOf(err), "with no configured dataset the call means main, which holds no such table")
}

func TestSpecialCharactersInDatasetNames(t *testing.T) {
	ctx := context.Background()
	be := open(t)
	for i, name := range []string{"with space", "ünïcode", "UPPER", "dotted.name", "temp", "system"} {
		_, err := be.CreateDataset(ctx, name, backend.CreateDatasetOptions{})
		require.NoError(t, err, name)
		ref := backend.TableRef{Dataset: name, Table: fmt.Sprintf("t%d", i)}
		_, err = be.CreateTable(ctx, ref, backend.CreateTableOptions{Columns: []backend.Column{{Name: "a", Type: backend.TypeInt64}}})
		require.NoError(t, err, name)
		_, err = be.GetTable(ctx, ref)
		require.NoError(t, err, name)
		r := run(t, be, `SELECT count(*) FROM t`+fmt.Sprint(i), backend.QueryOptions{DefaultDataset: name})
		require.Equal(t, []any{int64(0)}, r.column(0), name)
	}
}
