package duckdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// A dataset is a DuckDB schema in the opened database, and a table is a table
// or view in it. The catalog is read from DuckDB's own catalog functions,
// scoped to that one database, so the engine's system schemas are not datasets.
//
// DuckDB compares names without regard to case (a table created as People is
// reached as people), so every lookup here does too, and what a call returns is
// the name as it is stored.

const datasetsSQL = `SELECT schema_name FROM duckdb_schemas()
	WHERE database_name = $1 AND (NOT internal OR schema_name = 'main')
	ORDER BY schema_name`

// tablesBody lists the tables and views of one dataset ($1 database, $2 schema).
const tablesBody = `SELECT table_name AS name, 'TABLE' AS kind, estimated_size AS size FROM duckdb_tables()
		WHERE database_name = $1 AND schema_name = $2 AND NOT internal
	UNION ALL
	SELECT view_name, 'VIEW', 0 FROM duckdb_views()
		WHERE database_name = $1 AND schema_name = $2 AND NOT internal`

const columnsSQL = `SELECT column_name, is_nullable, comment FROM duckdb_columns()
	WHERE database_name = $1 AND schema_name = $2 AND table_name = $3
	ORDER BY column_index`

// page returns the items after token, at most limit of them (0 is all), and the
// token that continues the listing. items are sorted by key, and the token is
// the last key returned, so a listing stays correct while the catalog changes
// between pages.
func page[T any](items []T, key func(T) string, token string, limit int32) ([]T, string) {
	if token != "" {
		i := sort.Search(len(items), func(i int) bool { return key(items[i]) > token })
		items = items[i:]
	}
	if limit > 0 && int(limit) < len(items) {
		return items[:limit], key(items[limit-1])
	}
	return items, ""
}

func (b *Backend) ListDatasets(ctx context.Context, opts backend.ListDatasetsOptions) (*backend.ListDatasetsResult, error) {
	s, err := b.session(ctx, "")
	if err != nil {
		return nil, err
	}
	defer s.close()
	rows, err := s.fetchAll(ctx, datasetsSQL, b.catalog)
	if err != nil {
		return nil, err
	}
	all := make([]backend.Dataset, len(rows))
	for i, r := range rows {
		all[i] = backend.Dataset{Name: asString(r[0])}
	}
	items, next := page(all, func(d backend.Dataset) string { return d.Name }, opts.PageToken, opts.Limit)
	return &backend.ListDatasetsResult{Datasets: items, NextPageToken: next}, nil
}

func (b *Backend) ListTables(ctx context.Context, opts backend.ListTablesOptions) (*backend.ListTablesResult, error) {
	dataset := b.dataset(opts.Dataset)
	if err := checkName(dataset); err != nil {
		return nil, serr.New(serr.InvalidArgument, "ListTables", "dataset: "+err.Error())
	}
	s, err := b.session(ctx, "")
	if err != nil {
		return nil, err
	}
	defer s.close()
	dataset, err = b.requireDataset(ctx, s, "ListTables", dataset)
	if err != nil {
		return nil, err
	}
	rows, err := s.fetchAll(ctx, `SELECT name, kind, size FROM (`+tablesBody+`) ORDER BY name`, b.catalog, dataset)
	if err != nil {
		return nil, err
	}
	all := make([]backend.TableInfo, len(rows))
	for i, r := range rows {
		all[i] = tableInfo(dataset, r)
	}
	items, next := page(all, func(t backend.TableInfo) string { return t.Ref.Table }, opts.PageToken, opts.Limit)
	return &backend.ListTablesResult{Tables: items, NextPageToken: next}, nil
}

// tableInfo reads a row of tablesBody. The row count is the engine's estimate,
// which is what a table's "coarse size" is for; the engine tracks no byte size
// or timestamps, and those stay zero.
func tableInfo(dataset string, r []driver.Value) backend.TableInfo {
	info := backend.TableInfo{Ref: backend.TableRef{Dataset: dataset, Table: asString(r[0])}, Kind: backend.KindTable}
	if asString(r[1]) == "VIEW" {
		info.Kind = backend.KindView
	}
	if n, ok := r[2].(int64); ok {
		info.NumRows = n
	}
	return info
}

func (b *Backend) GetTable(ctx context.Context, ref backend.TableRef) (*backend.TableSchema, error) {
	ref, err := b.resolve("GetTable", ref)
	if err != nil {
		return nil, err
	}
	s, err := b.session(ctx, "")
	if err != nil {
		return nil, err
	}
	defer s.close()
	info, err := b.locate(ctx, s, "GetTable", ref)
	if err != nil {
		return nil, err
	}
	return b.describe(ctx, s, "GetTable", info)
}

// resolve fills in the default dataset and checks both names.
func (b *Backend) resolve(op string, ref backend.TableRef) (backend.TableRef, error) {
	ref.Dataset = b.dataset(ref.Dataset)
	if err := checkName(ref.Dataset); err != nil {
		return ref, serr.New(serr.InvalidArgument, op, "dataset: "+err.Error())
	}
	if err := checkName(ref.Table); err != nil {
		return ref, serr.New(serr.InvalidArgument, op, "table: "+err.Error())
	}
	return ref, nil
}

func qualified(catalog string, ref backend.TableRef) string {
	return quoteIdent(catalog) + "." + quoteIdent(ref.Dataset) + "." + quoteIdent(ref.Table)
}

// findDataset returns the stored name of the dataset a name addresses.
func (b *Backend) findDataset(ctx context.Context, s *session, name string) (string, bool, error) {
	rows, err := s.fetchAll(ctx, `SELECT schema_name FROM duckdb_schemas()
		WHERE database_name = $1 AND lower(schema_name) = lower($2) AND (NOT internal OR schema_name = 'main')`, b.catalog, name)
	if err != nil || len(rows) == 0 {
		return "", false, err
	}
	return asString(rows[0][0]), true, nil
}

// requireDataset is findDataset, with NotFound for a dataset that is not there.
func (b *Backend) requireDataset(ctx context.Context, s *session, op, name string) (string, error) {
	stored, found, err := b.findDataset(ctx, s, name)
	if err != nil {
		return "", err
	}
	if !found {
		return "", serr.New(serr.NotFound, op, fmt.Sprintf("dataset %q not found", name))
	}
	return stored, nil
}

// find looks a table or view up in a dataset that exists, by the dataset's
// stored name.
func (b *Backend) find(ctx context.Context, s *session, dataset, table string) (backend.TableInfo, bool, error) {
	rows, err := s.fetchAll(ctx, `SELECT name, kind, size FROM (`+tablesBody+`) WHERE lower(name) = lower($3)`, b.catalog, dataset, table)
	if err != nil || len(rows) == 0 {
		return backend.TableInfo{}, false, err
	}
	return tableInfo(dataset, rows[0]), true, nil
}

// locate finds a table or view, or says which of the dataset and the table is
// missing. The TableInfo it returns carries the stored names.
func (b *Backend) locate(ctx context.Context, s *session, op string, ref backend.TableRef) (backend.TableInfo, error) {
	dataset, err := b.requireDataset(ctx, s, op, ref.Dataset)
	if err != nil {
		return backend.TableInfo{}, err
	}
	info, found, err := b.find(ctx, s, dataset, ref.Table)
	if err != nil {
		return info, err
	}
	if !found {
		return info, serr.New(serr.NotFound, op, fmt.Sprintf("table %q not found in dataset %q", ref.Table, ref.Dataset))
	}
	return info, nil
}

// tableColumn is one column of a table as the catalog holds it.
type tableColumn struct {
	name     string
	dt       *dtype
	nullable bool
	comment  string
}

// columns reads a table's columns. Their types come from preparing a SELECT
// over the table, which gives the engine's structured types (including nested
// ones) without parsing a type's spelling; nullability and comments come from
// the catalog. ref carries the stored names.
func (b *Backend) columns(ctx context.Context, s *session, op string, ref backend.TableRef) ([]tableColumn, error) {
	stmt, err := s.prepare(ctx, op, "SELECT * FROM "+qualified(b.catalog, ref))
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	n, err := stmt.ColumnCount()
	if err != nil {
		return nil, serr.New(serr.Internal, op, "the duckdb engine failed")
	}

	meta, err := s.fetchAll(ctx, columnsSQL, b.catalog, ref.Dataset, ref.Table)
	if err != nil {
		return nil, err
	}
	byName := make(map[string]tableColumn, len(meta))
	for _, r := range meta {
		nullable, _ := r[1].(bool)
		byName[asString(r[0])] = tableColumn{nullable: nullable, comment: asString(r[2])}
	}

	cols := make([]tableColumn, 0, n)
	for i := 0; i < n; i++ {
		name, err := stmt.ColumnName(i)
		if err != nil {
			return nil, serr.New(serr.Internal, op, "the duckdb engine failed")
		}
		ti, err := stmt.ColumnTypeInfo(i)
		if err != nil {
			return nil, serr.New(serr.Internal, op, "the duckdb engine failed")
		}
		dt, err := fromTypeInfo(ti)
		if err != nil {
			return nil, serr.New(serr.Internal, op, "the duckdb engine failed")
		}
		m, ok := byName[name]
		cols = append(cols, tableColumn{name: name, dt: dt, nullable: !ok || m.nullable, comment: m.comment})
	}
	return cols, nil
}

// describe reads a table's full schema.
func (b *Backend) describe(ctx context.Context, s *session, op string, info backend.TableInfo) (*backend.TableSchema, error) {
	cols, err := b.columns(ctx, s, op, info.Ref)
	if err != nil {
		return nil, err
	}
	schema := &backend.TableSchema{Info: info}
	for _, c := range cols {
		col := c.dt.column(c.name, c.nullable)
		col.Description = c.comment
		schema.Columns = append(schema.Columns, col)
	}
	return schema, nil
}

func (b *Backend) CreateDataset(ctx context.Context, name string, opts backend.CreateDatasetOptions) (*backend.Dataset, error) {
	if err := checkName(name); err != nil {
		return nil, serr.New(serr.InvalidArgument, "CreateDataset", "name: "+err.Error())
	}
	// A dataset has these in other warehouses. Taking the dataset and dropping
	// them would answer a request the engine did not honor, so they are refused.
	switch {
	case opts.Location != "":
		return nil, serr.New(serr.Unsupported, "CreateDataset", "duckdb has no regions, so a dataset cannot have a location")
	case len(opts.Labels) > 0:
		return nil, serr.New(serr.Unsupported, "CreateDataset", "duckdb schemas carry no labels")
	case opts.Description != "":
		return nil, serr.New(serr.Unsupported, "CreateDataset", "duckdb schemas carry no description")
	}

	b.ddl.Lock()
	defer b.ddl.Unlock()
	s, err := b.session(ctx, "")
	if err != nil {
		return nil, err
	}
	defer s.close()

	stored, found, err := b.findDataset(ctx, s, name)
	if err != nil {
		return nil, err
	}
	if found {
		if opts.IfNotExists {
			return &backend.Dataset{Name: stored}, nil
		}
		return nil, serr.New(serr.AlreadyExists, "CreateDataset", fmt.Sprintf("dataset %q already exists", name))
	}
	if _, err := s.exec(ctx, "CREATE SCHEMA "+quoteIdent(b.catalog)+"."+quoteIdent(name)); err != nil {
		return nil, err
	}
	return &backend.Dataset{Name: name}, nil
}

func (b *Backend) DropDataset(ctx context.Context, name string, opts backend.DropDatasetOptions) error {
	if err := checkName(name); err != nil {
		return serr.New(serr.InvalidArgument, "DropDataset", "name: "+err.Error())
	}
	b.ddl.Lock()
	defer b.ddl.Unlock()
	s, err := b.session(ctx, "")
	if err != nil {
		return err
	}
	defer s.close()

	stored, found, err := b.findDataset(ctx, s, name)
	if err != nil {
		return err
	}
	if !found {
		if opts.IfExists {
			return nil
		}
		return serr.New(serr.NotFound, "DropDataset", fmt.Sprintf("dataset %q not found", name))
	}
	if stored == "main" {
		return serr.New(serr.PreconditionFailed, "DropDataset", "the default dataset main cannot be dropped")
	}
	stmt := "DROP SCHEMA " + quoteIdent(b.catalog) + "." + quoteIdent(stored)
	if opts.Cascade {
		stmt += " CASCADE"
	}
	// Without cascade a dataset that still holds objects is refused by the
	// engine as a dependency failure, which maps to PreconditionFailed.
	_, err = s.exec(ctx, stmt)
	return err
}

func (b *Backend) CreateTable(ctx context.Context, ref backend.TableRef, opts backend.CreateTableOptions) (*backend.TableSchema, error) {
	ref, err := b.resolve("CreateTable", ref)
	if err != nil {
		return nil, err
	}
	// Partitioning and clustering are a layout choice DuckDB does not have.
	if len(opts.PartitionBy) > 0 || len(opts.ClusterBy) > 0 {
		return nil, serr.New(serr.Unsupported, "CreateTable", "duckdb tables have no partitioning or clustering")
	}
	if opts.AsSelect != "" && len(opts.Columns) > 0 {
		return nil, serr.New(serr.InvalidArgument, "CreateTable", "as_select infers the columns; give columns or as_select, not both")
	}
	if opts.AsSelect == "" && len(opts.Columns) == 0 {
		return nil, serr.New(serr.InvalidArgument, "CreateTable", "a table needs columns or an as_select query")
	}
	definition, comments, err := tableDefinition(opts)
	if err != nil {
		return nil, err
	}

	b.ddl.Lock()
	defer b.ddl.Unlock()
	s, err := b.session(ctx, b.searchPath(""))
	if err != nil {
		return nil, err
	}
	defer s.close()

	ref.Dataset, err = b.requireDataset(ctx, s, "CreateTable", ref.Dataset)
	if err != nil {
		return nil, err
	}
	existing, found, err := b.find(ctx, s, ref.Dataset, ref.Table)
	if err != nil {
		return nil, err
	}
	if found {
		if opts.IfNotExists {
			return b.describe(ctx, s, "CreateTable", existing)
		}
		return nil, serr.New(serr.AlreadyExists, "CreateTable", fmt.Sprintf("table %q already exists in dataset %q", ref.Table, ref.Dataset))
	}

	if opts.AsSelect != "" {
		if err := checkSelect(ctx, s, opts.AsSelect); err != nil {
			return nil, err
		}
		definition = " AS " + opts.AsSelect
	}
	err = s.inTx(ctx, func() error {
		if _, err := s.exec(ctx, "CREATE TABLE "+qualified(b.catalog, ref)+definition); err != nil {
			return err
		}
		for col, text := range comments {
			if _, err := s.exec(ctx, "COMMENT ON COLUMN "+qualified(b.catalog, ref)+"."+quoteIdent(col)+" IS "+quoteLiteral(text)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	created, err := b.locate(ctx, s, "CreateTable", ref)
	if err != nil {
		return nil, err
	}
	return b.describe(ctx, s, "CreateTable", created)
}

// tableDefinition spells a column list as the body of a CREATE TABLE, and
// returns the column descriptions to attach afterwards.
func tableDefinition(opts backend.CreateTableOptions) (string, map[string]string, error) {
	if opts.AsSelect != "" {
		return "", nil, nil
	}
	seen := map[string]bool{}
	parts := make([]string, 0, len(opts.Columns))
	comments := map[string]string{}
	for _, c := range opts.Columns {
		if err := checkName(c.Name); err != nil {
			return "", nil, serr.New(serr.InvalidArgument, "CreateTable", "column: "+err.Error())
		}
		// DuckDB names fold case, so two names that differ only in case are the
		// same column.
		key := strings.ToLower(c.Name)
		if seen[key] {
			return "", nil, serr.New(serr.InvalidArgument, "CreateTable", fmt.Sprintf("column %q is defined twice", c.Name))
		}
		seen[key] = true
		typ, err := ddlType(c)
		if err != nil {
			return "", nil, err
		}
		part := quoteIdent(c.Name) + " " + typ
		if !c.Nullable {
			part += " NOT NULL"
		}
		parts = append(parts, part)
		if c.Description != "" {
			comments[c.Name] = c.Description
		}
	}
	return " (" + strings.Join(parts, ", ") + ")", comments, nil
}

// checkSelect requires the CTAS source to be one parameterless SELECT, so
// as_select cannot carry anything but the query it names.
func checkSelect(ctx context.Context, s *session, sql string) error {
	stmt, err := s.prepare(ctx, "CreateTable", sql)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	typ, err := stmt.StatementType()
	if err != nil || typ != duckdb.STATEMENT_TYPE_SELECT {
		return serr.New(serr.InvalidArgument, "CreateTable", "as_select must be a single SELECT statement")
	}
	if stmt.NumInput() > 0 {
		return serr.New(serr.InvalidArgument, "CreateTable", "as_select cannot take parameters")
	}
	return nil
}

func (b *Backend) DropTable(ctx context.Context, ref backend.TableRef, opts backend.DropTableOptions) error {
	ref, err := b.resolve("DropTable", ref)
	if err != nil {
		return err
	}
	b.ddl.Lock()
	defer b.ddl.Unlock()
	s, err := b.session(ctx, "")
	if err != nil {
		return err
	}
	defer s.close()

	info, err := b.locate(ctx, s, "DropTable", ref)
	if err != nil {
		if opts.IfExists && serr.Is(err, serr.NotFound) {
			return nil
		}
		return err
	}
	kind := "TABLE"
	if info.Kind == backend.KindView {
		kind = "VIEW"
	}
	_, err = s.exec(ctx, "DROP "+kind+" "+qualified(b.catalog, info.Ref))
	return err
}
