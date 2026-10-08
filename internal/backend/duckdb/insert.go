package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// target is where one Arrow column of an insert is written.
type target struct {
	column   string // the table's own spelling of the column name
	dt       *dtype
	required bool // the column is NOT NULL
}

// InsertRows appends Arrow batches to a table. Columns are matched by name
// (case-insensitively, as DuckDB names are); a table column the batches omit
// takes its default or NULL.
//
// The insert is not atomic, and says which rows it refused. A batch is tried as
// a whole in one transaction; if the engine refuses it, each of its rows is
// tried alone, so the accepted rows are in the table and each refused row is
// reported by its offset in the stream. A refusal that is not about a row — the
// table is gone, the engine failed — fails the call instead of being blamed on
// every row.
func (b *Backend) InsertRows(ctx context.Context, ref backend.TableRef, schema []byte, batches backend.BatchReader) (*backend.InsertResult, error) {
	ref, err := b.resolve("InsertRows", ref)
	if err != nil {
		return nil, err
	}
	s, err := b.session(ctx, "")
	if err != nil {
		return nil, err
	}
	defer s.close()

	info, err := b.locate(ctx, s, "InsertRows", ref)
	if err != nil {
		return nil, err
	}
	if info.Kind != backend.KindTable {
		return nil, serr.New(serr.PreconditionFailed, "InsertRows", "rows can be inserted into a table, not a view")
	}
	ref = info.Ref // the stored names
	cols, err := b.columns(ctx, s, "InsertRows", ref)
	if err != nil {
		return nil, err
	}

	rd, err := arrowipc.NewReader(schema, batches, arrowipc.WithMaxMessageBytes(b.cfg.MaxArrowMessageBytes))
	if err != nil {
		return nil, streamError(err, "the Arrow IPC schema could not be read")
	}
	defer rd.Release()
	targets, err := matchColumns(rd.Schema(), cols, ref)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(targets))
	for i, t := range targets {
		names[i] = t.column
	}

	res := &backend.InsertResult{}
	var offset int64
	for rd.Next() {
		rec := rd.RecordBatch()
		rows, offsets := make([][]driver.Value, 0, rec.NumRows()), make([]int64, 0, rec.NumRows())
		for r := 0; r < int(rec.NumRows()); r++ {
			vals, err := rowValues(targets, rec, r)
			if err != nil {
				reason, msg := refusal(err)
				res.Errors = append(res.Errors, backend.RowError{RowIndex: offset + int64(r), Reason: reason, Error: msg})
				continue
			}
			rows, offsets = append(rows, vals), append(offsets, offset+int64(r))
		}
		offset += rec.NumRows()

		n, refused, err := b.appendRows(ctx, s, ref, names, rows, offsets)
		res.RowsInserted += n
		res.Errors = append(res.Errors, refused...)
		if err != nil {
			return res, err
		}
	}
	if err := rd.Err(); err != nil {
		return res, streamError(err, "the Arrow IPC stream is malformed")
	}
	return res, nil
}

// streamError is the error to return when the call's Arrow could not be read:
// the client's malformed data is an invalid argument with a message of ours, and
// anything else, such as the transport failing, is returned as itself.
func streamError(err error, msg string) error {
	if errors.Is(err, arrowipc.ErrMalformed) {
		return serr.New(serr.InvalidArgument, "InsertRows", msg)
	}
	return err
}

// matchColumns pairs each Arrow field with its table column and checks the
// types agree, so a mismatched request fails before any row is written.
func matchColumns(as *arrow.Schema, cols []tableColumn, ref backend.TableRef) ([]target, error) {
	if len(as.Fields()) == 0 {
		return nil, serr.New(serr.InvalidArgument, "InsertRows", "the batches have no columns")
	}
	byName := make(map[string]tableColumn, len(cols))
	for _, c := range cols {
		byName[strings.ToLower(c.name)] = c
	}
	seen := map[string]bool{}
	out := make([]target, len(as.Fields()))
	for i, f := range as.Fields() {
		key := strings.ToLower(f.Name)
		c, ok := byName[key]
		if !ok {
			return nil, serr.New(serr.InvalidArgument, "InsertRows",
				"table "+quoteIdent(ref.Table)+" has no column "+quoteIdent(f.Name))
		}
		if seen[key] {
			return nil, serr.New(serr.InvalidArgument, "InsertRows", "column "+quoteIdent(f.Name)+" appears twice in the batches")
		}
		seen[key] = true
		if err := checkArrow(c.name, c.dt, f.Type); err != nil {
			return nil, err
		}
		out[i] = target{column: c.name, dt: c.dt, required: !c.nullable}
	}
	return out, nil
}

// rowValues converts row r of a record to the values the appender takes.
func rowValues(targets []target, rec arrow.RecordBatch, r int) ([]driver.Value, error) {
	vals := make([]driver.Value, len(targets))
	for c, t := range targets {
		if t.required && rec.Column(c).IsNull(r) {
			// Found here, from the table's own nullability, so the row is refused
			// with its reason and without asking the engine, whose constraint
			// error does not say which constraint failed.
			return nil, &valueError{"a required column has no value"}
		}
		v, err := arrowValue(t.dt, rec.Column(c), r)
		if err != nil {
			return nil, err
		}
		vals[c] = v
	}
	return vals, nil
}

// appendRows writes rows, whole first and one by one if the whole is refused.
// It returns how many were written and the rows the engine refused.
func (b *Backend) appendRows(ctx context.Context, s *session, ref backend.TableRef, cols []string, rows [][]driver.Value, offsets []int64) (int64, []backend.RowError, error) {
	if len(rows) == 0 {
		return 0, nil, nil
	}
	err := b.appendAtomic(ctx, s, ref, cols, rows)
	if err == nil {
		return int64(len(rows)), nil, nil
	}
	if !aboutARow(err) {
		return 0, nil, err
	}
	var (
		written int64
		refused []backend.RowError
	)
	for i := range rows {
		err := b.appendAtomic(ctx, s, ref, cols, rows[i:i+1])
		switch {
		case err == nil:
			written++
		case aboutARow(err):
			reason, msg := refusal(err)
			refused = append(refused, backend.RowError{RowIndex: offsets[i], Reason: reason, Error: msg})
		default:
			return written, refused, err
		}
	}
	return written, refused, nil
}

// appendAtomic writes rows in one transaction: all of them, or none.
func (b *Backend) appendAtomic(ctx context.Context, s *session, ref backend.TableRef, cols []string, rows [][]driver.Value) error {
	return s.inTx(ctx, func() error {
		app, err := duckdb.NewAppenderWithColumns(s.conn, b.catalog, ref.Dataset, ref.Table, cols)
		if err != nil {
			return mapError(ctx, "InsertRows", err)
		}
		for _, r := range rows {
			if err := app.AppendRow(r...); err != nil {
				_ = app.Clear()
				_ = app.Close()
				// AppendRow only fails on the value it was given — the engine
				// has not seen it yet — so this is the row's fault, and the
				// driver's message about it is not passed on.
				return serr.New(serr.InvalidArgument, "InsertRows", "a value could not be stored in its column")
			}
		}
		// Close flushes, which is where the engine checks constraints.
		return mapError(ctx, "InsertRows", app.Close())
	})
}

// aboutARow reports whether the engine refused the rows themselves — a
// constraint, a value it could not convert — as opposed to failing.
func aboutARow(err error) bool {
	switch serr.CodeOf(err) {
	case serr.PreconditionFailed, serr.InvalidArgument:
		return true
	}
	return false
}

// refusal is what a refused row carries to the client: the portable reason and
// a message that is ours, never the engine's.
//
// A value that cannot be stored in its column (out of range, malformed, not a
// member of an enum, a null in a required column) is RefusalInvalidValue. The
// engine reports a violated key, unique, check or foreign-key constraint as one
// kind of error without saying which, and none of the portable reasons names a
// duplicate key, so that refusal is RefusalUnspecified; reading the engine's
// message to tell them apart is not done. DuckDB has no row-size limit, so
// RefusalRowTooLarge is never set, and a table that disagrees with the batch
// fails the whole call before any row is written, so RefusalSchemaMismatch is
// never set either.
func refusal(err error) (backend.RowRefusal, string) {
	var ve *valueError
	if errors.As(err, &ve) {
		return backend.RefusalInvalidValue, ve.msg
	}
	var se *serr.Error
	if errors.As(err, &se) {
		msg := "the row was refused"
		if se.Err != nil {
			msg = se.Err.Error()
		}
		if se.Code == serr.InvalidArgument {
			return backend.RefusalInvalidValue, msg
		}
		return backend.RefusalUnspecified, msg
	}
	return backend.RefusalUnspecified, "the row was refused"
}
