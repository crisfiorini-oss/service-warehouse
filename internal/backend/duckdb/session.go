package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// namedArgs numbers positional values for the driver.
func namedArgs(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, a := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return out
}

// prepare prepares exactly one statement. The driver prepares a multi-statement
// text by running every statement but the last, which would make a dry run
// execute, so the single-statement form is the only one used: it refuses a text
// with more than one statement (or none) before anything runs.
func (s *session) prepare(ctx context.Context, op, sql string) (*duckdb.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, mapError(ctx, op, err)
	}
	st, err := s.conn.Prepare(sql)
	if err != nil {
		var de *duckdb.Error
		if !errors.As(err, &de) {
			return nil, serr.New(serr.InvalidArgument, op, "the SQL must be exactly one statement")
		}
		return nil, mapError(ctx, op, err)
	}
	return st.(*duckdb.Stmt), nil
}

// exec runs one statement that returns no rows and reports the rows it changed.
func (s *session) exec(ctx context.Context, sql string, args ...driver.Value) (int64, error) {
	st, err := s.prepare(ctx, "exec", sql)
	if err != nil {
		return 0, err
	}
	defer func() { _ = st.Close() }()
	res, err := st.ExecContext(ctx, namedArgs(args))
	if err != nil {
		return 0, mapError(ctx, "exec", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// fetchAll runs one query and returns every row. It is for the catalog's small
// introspection queries; a caller's query streams through batchReader instead.
func (s *session) fetchAll(ctx context.Context, sql string, args ...driver.Value) ([][]driver.Value, error) {
	st, err := s.prepare(ctx, "query", sql)
	if err != nil {
		return nil, err
	}
	defer func() { _ = st.Close() }()
	rows, err := st.QueryContext(ctx, namedArgs(args))
	if err != nil {
		return nil, mapError(ctx, "query", err)
	}
	defer func() { _ = rows.Close() }()

	var out [][]driver.Value
	for {
		dst := make([]driver.Value, len(rows.Columns()))
		err := rows.Next(dst)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, mapError(ctx, "query", err)
		}
		out = append(out, dst)
	}
}

// inTx runs fn in one transaction: committed when fn succeeds, rolled back when
// it does not.
func (s *session) inTx(ctx context.Context, fn func() error) error {
	if _, err := s.exec(ctx, "BEGIN TRANSACTION"); err != nil {
		return err
	}
	if err := fn(); err != nil {
		// A rollback that fails leaves the connection to be closed, which
		// abandons the transaction; the error that matters is fn's.
		_, _ = s.exec(context.WithoutCancel(ctx), "ROLLBACK")
		return err
	}
	if _, err := s.exec(ctx, "COMMIT"); err != nil {
		_, _ = s.exec(context.WithoutCancel(ctx), "ROLLBACK")
		return err
	}
	return nil
}

func asString(v driver.Value) string {
	s, _ := v.(string)
	return s
}
