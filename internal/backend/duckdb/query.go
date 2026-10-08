package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// batchRows is how many rows go into one Arrow record batch.
const batchRows = 8192

var arrowAllocator memory.Allocator = memory.DefaultAllocator

func isEOF(err error) bool { return errors.Is(err, io.EOF) }

// statementKind says what running a statement yields.
type statementKind int

const (
	// returnsRows is a statement whose result is a table of rows.
	returnsRows statementKind = iota
	// changesRows is DDL or DML: its result is a count, reported in the job.
	changesRows
)

// classify admits the statements this backend serves and refuses the rest.
//
// The list is closed on purpose: SQL that reads a table, changes rows, or
// changes the catalog is the portable surface. Everything else — SET, ATTACH,
// COPY, EXPORT, LOAD, INSTALL, PRAGMA writes, transactions — is either session
// state a per-request connection cannot keep, or reaches outside the database,
// and is Unsupported rather than run.
func classify(stmt *duckdb.Stmt, sql string) (statementKind, error) {
	typ, err := stmt.StatementType()
	if err != nil {
		return 0, serr.New(serr.Internal, "Query", "the duckdb engine failed")
	}
	switch typ {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_EXPLAIN:
		return returnsRows, nil
	case duckdb.STATEMENT_TYPE_INSERT, duckdb.STATEMENT_TYPE_UPDATE, duckdb.STATEMENT_TYPE_DELETE:
		// DuckDB answers a DML statement with a one-row count, unless it has a
		// RETURNING clause, which answers with rows. Nothing in the prepared
		// statement tells the two apart (a RETURNING of one BIGINT column named
		// Count looks the same), and RETURNING is not part of the portable
		// contract, so a statement that spells it is refused.
		if hasKeyword(sql, "returning") {
			return 0, serr.New(serr.Unsupported, "Query", "RETURNING is not part of the portable contract")
		}
		return changesRows, nil
	case duckdb.STATEMENT_TYPE_CREATE, duckdb.STATEMENT_TYPE_ALTER, duckdb.STATEMENT_TYPE_DROP:
		return changesRows, nil
	}
	return 0, serr.New(serr.Unsupported, "Query",
		"the duckdb backend serves SELECT, EXPLAIN, INSERT, UPDATE, DELETE and CREATE, ALTER and DROP statements only")
}

// Query runs one SQL statement. A statement that returns rows streams them as
// Arrow after a header; DDL and DML return a header with no schema and the
// count of rows changed. DuckDB has no job model, so the job is already done
// when the header is sent: the result is materialized before the first batch.
func (b *Backend) Query(parent context.Context, sql string, opts backend.QueryOptions) (*backend.QueryResult, error) {
	if opts.MaxBytesBilled > 0 {
		return nil, serr.New(serr.Unsupported, "Query",
			"duckdb cannot meter bytes scanned, so it cannot enforce max_bytes_billed")
	}
	ctx, cancel := b.withTimeout(parent, opts.Timeout)
	started := time.Now()

	s, err := b.session(ctx, b.searchPath(opts.DefaultDataset))
	if err != nil {
		cancel()
		return nil, err
	}
	stmt, err := s.prepare(ctx, "Query", sql)
	if err != nil {
		s.close()
		cancel()
		return nil, err
	}

	// release unwinds everything the call holds; it is the whole cleanup for
	// every exit but a streaming result, which hands the same unwinding to
	// batchReader.Close.
	release := func() {
		_ = stmt.Close()
		s.close()
		cancel()
	}

	kind, err := classify(stmt, sql)
	if err != nil {
		release()
		return nil, err
	}
	args, err := bindParams(stmt, opts.Params)
	if err != nil {
		release()
		return nil, err
	}

	job := backend.Job{State: backend.JobDone, Created: started, Started: started}

	if kind == changesRows {
		defer release()
		if !opts.DryRun {
			res, err := stmt.ExecContext(ctx, args)
			if err != nil {
				return nil, mapError(ctx, "Query", err)
			}
			job.Stats.RowsAffected, _ = res.RowsAffected()
		}
		job.Ended = time.Now()
		return &backend.QueryResult{Job: job}, nil
	}

	if opts.DryRun {
		defer release()
		names, types, err := describeStatement(stmt)
		if err != nil {
			return nil, err
		}
		job.Ended = time.Now()
		return resultHeader(job, names, types, nil)
	}

	rows, err := stmt.QueryContext(ctx, args)
	if err != nil {
		// Map before releasing: release cancels ctx, and a cancelled context
		// would be reported as the cause of every failure.
		err = mapError(ctx, "Query", err)
		release()
		return nil, err
	}
	closeAll := func() {
		_ = rows.Close()
		release()
	}
	names := rows.Columns()
	types, err := resultTypes(stmt, rows, names)
	if err != nil {
		closeAll()
		return nil, err
	}
	job.Ended = time.Now()
	res, err := resultHeader(job, names, types, func(schema *arrow.Schema, enc *arrowipc.Encoder) backend.BatchReader {
		return &batchReader{ctx: ctx, rows: rows, schema: schema, types: types, enc: enc, release: closeAll}
	})
	if err != nil {
		closeAll()
		return nil, err
	}
	return res, nil
}

// searchPath is the dataset unqualified names resolve in: the request's, else
// SWH_DATASET, else none (DuckDB's own default).
func (b *Backend) searchPath(requested string) string {
	if requested != "" {
		return requested
	}
	return b.cfg.DefaultDataset
}

// resultHeader builds the portable and Arrow descriptions of a result. When
// reader is non-nil it is given the Arrow encoder and supplies the batches.
func resultHeader(job backend.Job, names []string, types []*dtype, reader func(*arrow.Schema, *arrowipc.Encoder) backend.BatchReader) (*backend.QueryResult, error) {
	schema := arrowSchema(names, types)
	ipcSchema, err := arrowipc.SchemaMessage(schema)
	if err != nil {
		return nil, serr.New(serr.Internal, "Query", "the result schema cannot be encoded")
	}
	cols := make([]backend.Column, len(names))
	for i := range names {
		cols[i] = types[i].column(names[i], true)
	}
	res := &backend.QueryResult{Job: job, Schema: cols, ArrowSchema: ipcSchema}
	if reader != nil {
		enc, err := arrowipc.NewEncoder(schema)
		if err != nil {
			return nil, serr.New(serr.Internal, "Query", "the result schema cannot be encoded")
		}
		res.Batches = reader(schema, enc)
	}
	return res, nil
}

// describeStatement names and types the columns of a prepared statement without
// running it, which is what a dry run reports.
func describeStatement(stmt *duckdb.Stmt) ([]string, []*dtype, error) {
	n, err := stmt.ColumnCount()
	if err != nil {
		return nil, nil, serr.New(serr.Internal, "Query", "the duckdb engine failed")
	}
	names, types := make([]string, n), make([]*dtype, n)
	for i := range names {
		if names[i], err = stmt.ColumnName(i); err != nil {
			return nil, nil, serr.New(serr.Internal, "Query", "the duckdb engine failed")
		}
		ti, err := stmt.ColumnTypeInfo(i)
		if err != nil {
			return nil, nil, serr.New(serr.Unsupported, "Query", fmt.Sprintf(
				"result column %q has a type that depends on a parameter, so a dry run cannot describe it; cast it in the SQL", names[i]))
		}
		if types[i], err = resolveColumn(names[i], ti); err != nil {
			return nil, nil, err
		}
	}
	return names, types, nil
}

// resultTypes describes the columns of an executed statement. The prepared
// statement's structured types are used when it has them; a column whose type
// hung on a parameter has none, and falls back to the type name the executed
// result reports, which is enough for a scalar.
func resultTypes(stmt *duckdb.Stmt, rows driver.Rows, names []string) ([]*dtype, error) {
	types := make([]*dtype, len(names))
	for i, name := range names {
		ti, err := stmt.ColumnTypeInfo(i)
		if err == nil {
			if types[i], err = resolveColumn(name, ti); err != nil {
				return nil, err
			}
			continue
		}
		var dt *dtype
		if named, ok := rows.(driver.RowsColumnTypeDatabaseTypeName); ok {
			dt, _ = fromScalarName(named.ColumnTypeDatabaseTypeName(i))
		}
		if dt == nil {
			return nil, serr.New(serr.Unsupported, "Query", fmt.Sprintf(
				"result column %q has a type that cannot be determined; cast it in the SQL", name))
		}
		if err := dt.readable(); err != nil {
			return nil, unreadable(name, err)
		}
		types[i] = dt
	}
	return types, nil
}

func resolveColumn(name string, ti duckdb.TypeInfo) (*dtype, error) {
	dt, err := fromTypeInfo(ti)
	if err == nil {
		err = dt.readable()
	}
	if err != nil {
		return nil, unreadable(name, err)
	}
	return dt, nil
}

func unreadable(column string, why error) error {
	return serr.New(serr.Unsupported, "Query", fmt.Sprintf("result column %q: %v", column, why))
}

// batchReader streams a materialized result as Arrow record-batch messages.
type batchReader struct {
	ctx     context.Context
	rows    driver.Rows
	schema  *arrow.Schema
	types   []*dtype
	enc     *arrowipc.Encoder
	release func()

	done   bool
	closed bool
}

func (r *batchReader) Next() ([]byte, error) {
	if r.done || r.closed {
		return nil, io.EOF
	}
	if err := r.ctx.Err(); err != nil {
		return nil, mapError(r.ctx, "Query", err)
	}
	rec, exhausted, err := rowsToRecord(r.schema, r.types, r.rows.Next, batchRows)
	if err != nil {
		return nil, mapError(r.ctx, "Query", err)
	}
	if rec == nil {
		r.done = true
		r.finish()
		return nil, io.EOF
	}
	defer rec.Release()
	msg, err := r.enc.Encode(rec)
	if err != nil {
		return nil, serr.New(serr.Internal, "Query", "a result batch cannot be encoded")
	}
	if exhausted {
		r.done = true
		r.finish()
	}
	return msg, nil
}

// finish releases the connection as soon as the last row is read, rather than
// when the caller gets round to Close.
func (r *batchReader) finish() {
	if !r.closed {
		r.closed = true
		_ = r.enc.Close()
		r.release()
	}
}

func (r *batchReader) Close() error {
	r.finish()
	return nil
}

// --- parameters ---

// bindParams matches the supplied parameters to the statement's. The match is
// strict in both directions: a parameter the SQL needs and the request omits is
// an error, and so is one the request supplies and the SQL never reads — DuckDB
// binds $name, and a query written with @name or :name would otherwise run with
// its parameters silently unused.
func bindParams(stmt *duckdb.Stmt, params []backend.QueryParam) ([]driver.NamedValue, error) {
	supplied := make(map[string]backend.QueryParam, len(params))
	for _, p := range params {
		if p.Name == "" {
			return nil, serr.New(serr.InvalidArgument, "Query", "a parameter has no name")
		}
		if _, dup := supplied[p.Name]; dup {
			return nil, serr.New(serr.InvalidArgument, "Query", fmt.Sprintf("parameter %q is supplied twice", p.Name))
		}
		supplied[p.Name] = p
	}

	out := make([]driver.NamedValue, 0, stmt.NumInput())
	used := make(map[string]bool, len(supplied))
	for i := 1; i <= stmt.NumInput(); i++ {
		name, err := stmt.ParamName(i)
		if err != nil {
			return nil, serr.New(serr.Internal, "Query", "the duckdb engine failed")
		}
		p, ok := supplied[name]
		if !ok {
			return nil, serr.New(serr.InvalidArgument, "Query", fmt.Sprintf("the SQL uses $%s, which the request does not supply", name))
		}
		v, err := paramValue(p)
		if err != nil {
			return nil, err
		}
		out = append(out, driver.NamedValue{Name: name, Ordinal: i, Value: v})
		used[name] = true
	}

	var unused []string
	for name := range supplied {
		if !used[name] {
			unused = append(unused, name)
		}
	}
	if len(unused) > 0 {
		sort.Strings(unused)
		return nil, serr.New(serr.InvalidArgument, "Query", fmt.Sprintf(
			"parameter %q is not used by the SQL; the duckdb backend binds $name (not @name or :name)", unused[0]))
	}
	return out, nil
}

// paramValue binds p's text as the portable type it declares, so DuckDB sees a
// value of that type and not a string it has to guess about.
//
// A type whose text form the contract does not define (BYTES, INTERVAL, JSON,
// ARRAY, STRUCT) is Unsupported, as is NUMERIC, because the driver cannot bind a
// DECIMAL: a client casts a STRING parameter in the SQL instead. A parameter
// with no declared type is bound as text and DuckDB casts it to what the
// surrounding SQL expects.
func paramValue(p backend.QueryParam) (driver.Value, error) {
	if p.IsNull {
		return nil, nil
	}
	bad := func(what string) error {
		return serr.New(serr.InvalidArgument, "Query", fmt.Sprintf("parameter %q: %q is not a valid %s", p.Name, p.Value, what))
	}
	switch p.Type {
	case backend.TypeBool:
		v, err := strconv.ParseBool(p.Value)
		if err != nil {
			return nil, bad("BOOL")
		}
		return v, nil
	case backend.TypeInt64:
		v, err := strconv.ParseInt(p.Value, 10, 64)
		if err != nil {
			return nil, bad("INT64")
		}
		return v, nil
	case backend.TypeFloat64:
		v, err := strconv.ParseFloat(p.Value, 64)
		if err != nil {
			return nil, bad("FLOAT64")
		}
		return v, nil
	case backend.TypeString, backend.TypeUnknown:
		return p.Value, nil
	case backend.TypeDate:
		t, err := time.Parse("2006-01-02", p.Value)
		if err != nil {
			return nil, bad("DATE (YYYY-MM-DD)")
		}
		return duckdb.Typed(t, duckdb.TYPE_DATE), nil
	case backend.TypeTime:
		t, err := time.Parse("15:04:05", p.Value)
		if err != nil {
			return nil, bad("TIME (HH:MM:SS[.ffffff])")
		}
		return duckdb.Typed(t, duckdb.TYPE_TIME), nil
	case backend.TypeTimestamp:
		// A wall-clock value carries no zone; text that names one is ambiguous.
		for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
			if t, err := time.Parse(layout, p.Value); err == nil {
				return duckdb.Typed(t, duckdb.TYPE_TIMESTAMP), nil
			}
		}
		return nil, bad("TIMESTAMP (YYYY-MM-DDTHH:MM:SS[.ffffff], no zone)")
	case backend.TypeTimestampTZ:
		t, err := time.Parse(time.RFC3339Nano, p.Value)
		if err != nil {
			return nil, bad("TIMESTAMPTZ (RFC 3339, with a zone)")
		}
		return duckdb.Typed(t.UTC(), duckdb.TYPE_TIMESTAMP_TZ), nil
	}
	return nil, serr.New(serr.Unsupported, "Query", fmt.Sprintf(
		"parameter %q: the duckdb backend cannot bind a %s parameter; pass a STRING and cast it in the SQL",
		p.Name, portableName(p.Type)))
}

func portableName(t backend.ColumnType) string {
	switch t {
	case backend.TypeNumeric:
		return "NUMERIC"
	case backend.TypeBytes:
		return "BYTES"
	case backend.TypeInterval:
		return "INTERVAL"
	case backend.TypeJSON:
		return "JSON"
	case backend.TypeArray:
		return "ARRAY"
	case backend.TypeStruct:
		return "STRUCT"
	case backend.TypeGeography:
		return "GEOGRAPHY"
	}
	return "UNKNOWN"
}
