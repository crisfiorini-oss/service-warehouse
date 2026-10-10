package duckdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// Scan reads the rows of one table that match a structured filter. The columns
// are checked against the table first, and then the read is a SELECT of quoted
// names whose every value is a bound parameter: nothing from the request is
// spliced into the SQL but identifiers the table itself spelled. The SELECT runs
// through Query, so a scan has its encoding, its type rules, its time limit and
// its connection handling.
func (b *Backend) Scan(ctx context.Context, ref backend.TableRef, opts backend.ScanOptions) (*backend.QueryResult, error) {
	const op = "Scan"
	if err := backend.ValidateScan(op, opts); err != nil {
		return nil, err
	}
	ref, err := b.resolve(op, ref)
	if err != nil {
		return nil, err
	}
	cols, ref, err := b.scanColumns(ctx, op, ref)
	if err != nil {
		return nil, err
	}
	sql, params, err := scanSQL(b.catalog, ref, cols, opts)
	if err != nil {
		return nil, err
	}
	return b.Query(ctx, sql, backend.QueryOptions{Params: params})
}

// scanColumns reads the columns of the table a scan names, with the table's own
// spelling of its names.
func (b *Backend) scanColumns(ctx context.Context, op string, ref backend.TableRef) ([]tableColumn, backend.TableRef, error) {
	s, err := b.session(ctx, "")
	if err != nil {
		return nil, ref, err
	}
	defer s.close()
	info, err := b.locate(ctx, s, op, ref)
	if err != nil {
		return nil, ref, err
	}
	cols, err := b.columns(ctx, s, op, info.Ref)
	return cols, info.Ref, err
}

// scanSQL spells the scan. The names it quotes are the table's own, found by
// matching the request's case-insensitively, so a request names nothing the
// table lacks.
func scanSQL(catalog string, ref backend.TableRef, cols []tableColumn, opts backend.ScanOptions) (string, []backend.QueryParam, error) {
	const op = "Scan"
	find := func(name string) (string, error) {
		for _, c := range cols {
			if strings.EqualFold(c.name, name) {
				return c.name, nil
			}
		}
		return "", serr.New(serr.InvalidArgument, op, fmt.Sprintf("table %q has no column %q", ref.Table, name))
	}

	var selected []string
	if len(opts.Selected) == 0 {
		for _, c := range cols {
			selected = append(selected, quoteIdent(c.name))
		}
	}
	for _, name := range opts.Selected {
		stored, err := find(name)
		if err != nil {
			return "", nil, err
		}
		selected = append(selected, quoteIdent(stored))
	}

	var (
		terms  []string
		params []backend.QueryParam
	)
	bind := func(p backend.Predicate, value string) string {
		name := fmt.Sprintf("p%d", len(params))
		params = append(params, backend.QueryParam{Name: name, Type: p.Type, Value: value})
		return "$" + name
	}
	for _, p := range opts.Filter {
		stored, err := find(p.Field)
		if err != nil {
			return "", nil, err
		}
		col := quoteIdent(stored)
		switch p.Op {
		case backend.OpIsNull:
			terms = append(terms, col+" IS NULL")
		case backend.OpIsNotNull:
			terms = append(terms, col+" IS NOT NULL")
		case backend.OpIn:
			holes := make([]string, len(p.Values))
			for i, v := range p.Values {
				holes[i] = bind(p, v)
			}
			terms = append(terms, col+" IN ("+strings.Join(holes, ", ")+")")
		default:
			terms = append(terms, col+" "+comparison[p.Op]+" "+bind(p, p.Values[0]))
		}
	}

	sql := "SELECT " + strings.Join(selected, ", ") + " FROM " + qualified(catalog, ref)
	if len(terms) > 0 {
		sql += " WHERE " + strings.Join(terms, " AND ")
	}
	return sql, params, nil
}

var comparison = map[backend.PredicateOp]string{
	backend.OpEq: "=",
	backend.OpLt: "<",
	backend.OpLe: "<=",
	backend.OpGt: ">",
	backend.OpGe: ">=",
}
