package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// maxRowsPerInsert is BigQuery's recommended streaming request size; a stream of
// more rows is sent as several requests.
const maxRowsPerInsert = 500

// maxRequestBytes bounds the rows one request carries, as BigQuery encodes them.
// BigQuery refuses a request over 10 MB and, with the same limit, a single row
// over it, and a row count says nothing of that: a few hundred rows with large
// text are many megabytes. The bound sits under the limit to leave room for the
// request's own envelope and for the difference between this estimate and the
// encoding. A row that alone passes it can never be sent and is refused as too
// large; the row is the problem, so retrying cannot help.
const maxRequestBytes = 8 << 20

// rowEnvelopeBytes is what a row adds to a request beyond its values: the keys
// and punctuation around them and its insert id.
const rowEnvelopeBytes = 64

// reasonInvalid is the reason BigQuery gives a streamed row whose own content it
// refuses. The other reasons a row carries (timeout, backendError, internalError,
// rateLimitExceeded, quotaExceeded, stopped) say nothing against the row.
const reasonInvalid = "invalid"

// rowInserter is the streaming-insert seam; *bq.Inserter satisfies it.
type rowInserter interface {
	Put(ctx context.Context, src any) error
}

// insertRow is a row waiting for a request.
type insertRow struct {
	index  int64
	values map[string]bq.Value
	size   int
}

// Save implements bq.ValueSaver. An empty insert id asks the client library for
// a random one, so no two rows are ever collapsed by BigQuery's best-effort
// de-duplication.
func (r insertRow) Save() (map[string]bq.Value, string, error) { return r.values, "", nil }

// InsertRows appends the rows of every Arrow batch to a table, with streaming
// inserts, and reports what BigQuery refused row by row.
//
// What a failure says about a row decides how it is reported:
//
//   - A row whose own content BigQuery refuses (it answers "invalid") is
//     reported with RefusalInvalidValue. The rest of its request is still
//     inserted: the inserter skips invalid rows, so the call is not atomic and a
//     refused row does not shorten the batch.
//   - A row that cannot be sent in any request, or whose values cannot be
//     encoded, is reported with RefusalRowTooLarge or RefusalInvalidValue before
//     anything of it is sent.
//   - When the batch's columns disagree with the table's (a column the table
//     lacks, a required column the batch lacks) every row is reported with
//     RefusalSchemaMismatch and none is sent: the table, not any row, would have
//     to change. The disagreement is found by comparing the batch's schema with
//     the table's metadata, so no vendor text is read to decide it.
//   - Anything else (a request that failed, a throttle, a quota, a timeout, a
//     dead backend, a row reported with any other reason) fails the call. Rows of
//     earlier requests may already be stored, so a caller that retries the call
//     can store a row twice: delivery is at least once.
func (b *Backend) InsertRows(ctx context.Context, ref backend.TableRef, schema []byte, batches backend.BatchReader) (*backend.InsertResult, error) {
	const op = "InsertRows"
	ref, err := b.resolve(op, ref)
	if err != nil {
		return nil, err
	}
	stream, err := arrowipc.NewReader(schema, batches)
	if err != nil {
		return nil, streamError(err)
	}
	defer stream.Release()

	table := b.client.Dataset(ref.Dataset).Table(ref.Table)
	md, err := table.Metadata(ctx)
	if err != nil {
		return nil, classify(op, err)
	}
	inserter := table.Inserter()
	// Rows BigQuery finds invalid are skipped and reported back, so the valid
	// rows of a request are stored and the invalid ones come back by index.
	inserter.SkipInvalidRows = true
	return insertAll(ctx, stream, md.Schema, inserter)
}

// streamError is the error to return for a failure to read the call's Arrow: the
// client's malformed data is an invalid argument, and anything else, such as the
// transport failing, is returned as itself.
func streamError(err error) error {
	if errors.Is(err, arrowipc.ErrMalformed) {
		return serr.Wrap(serr.InvalidArgument, "InsertRows", fmt.Errorf("the Arrow data is malformed: %w", err))
	}
	return err
}

// insertAll runs the stream through plan, send and the classification of what
// BigQuery answered.
func insertAll(ctx context.Context, stream *arrowipc.Reader, table bq.Schema, inserter rowInserter) (*backend.InsertResult, error) {
	const op = "InsertRows"
	var (
		result   backend.InsertResult
		next     int64 // the index of the next row of the stream
		conflict string
		checked  bool
		request  []insertRow
		reqBytes int
	)
	flush := func() error {
		if len(request) == 0 {
			return nil
		}
		inserted, refused, err := send(ctx, inserter, request)
		result.RowsInserted += int64(inserted)
		result.Errors = append(result.Errors, refused...)
		request, reqBytes = nil, 0
		return err
	}

	// The stream owns each record it hands out until the next call, so none is
	// released here.
	for stream.Next() {
		rec := stream.RecordBatch()
		if !checked {
			checked = true
			if err := checkArrowSchema(rec.Schema()); err != nil {
				return nil, serr.New(serr.Unsupported, op, err.Error())
			}
			if name, twice := repeatedColumn(rec.Schema()); twice {
				// Two columns of one name would reach BigQuery as one, and a value
				// would be dropped without a word.
				return nil, serr.New(serr.InvalidArgument, op, fmt.Sprintf("the batch has two columns named %q (BigQuery names are not case-sensitive)", name))
			}
			conflict = schemaConflict(rec.Schema(), table)
		}
		rows := int(rec.NumRows())
		if conflict != "" {
			for r := 0; r < rows; r++ {
				result.Errors = append(result.Errors, backend.RowError{
					RowIndex: next + int64(r), Reason: backend.RefusalSchemaMismatch, Error: conflict,
				})
			}
			next += int64(rows)
			continue
		}
		for r := 0; r < rows; r++ {
			row, refusal := encodeRow(rec, r, next+int64(r))
			if refusal != nil {
				result.Errors = append(result.Errors, *refusal)
				continue
			}
			if len(request) == maxRowsPerInsert || reqBytes+row.size > maxRequestBytes {
				if err := flush(); err != nil {
					return nil, err
				}
			}
			request = append(request, row)
			reqBytes += row.size
		}
		next += int64(rows)
	}
	if err := stream.Err(); err != nil {
		return nil, streamError(err)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return &result, nil
}

// encodeRow converts row r of rec to what a request carries. A row that cannot
// be sent comes back as the refusal to report instead.
func encodeRow(rec arrow.RecordBatch, r int, index int64) (insertRow, *backend.RowError) {
	values := make(map[string]bq.Value, rec.NumCols())
	for c, field := range rec.Schema().Fields() {
		v, err := jsonValue(field.Name, rec.Column(c), r)
		if err != nil {
			text := "the row has a value BigQuery cannot be given"
			if ve, ok := asValueError(err); ok {
				text = "the row has a value BigQuery cannot be given: " + ve.Error()
			}
			return insertRow{}, &backend.RowError{RowIndex: index, Reason: backend.RefusalInvalidValue, Error: text}
		}
		values[field.Name] = v
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return insertRow{}, &backend.RowError{
			RowIndex: index, Reason: backend.RefusalInvalidValue, Error: "the row's values cannot be encoded",
		}
	}
	size := len(encoded) + rowEnvelopeBytes
	if size > maxRequestBytes {
		return insertRow{}, &backend.RowError{
			RowIndex: index, Reason: backend.RefusalRowTooLarge,
			Error: fmt.Sprintf("the row is %d bytes and one request carries at most %d", size, maxRequestBytes),
		}
	}
	return insertRow{index: index, values: values, size: size}, nil
}

// send inserts one request and classifies the answer. It returns how many rows
// were stored and the rows BigQuery permanently refused; any other failure is
// the error, and nothing about it is said of a row.
func send(ctx context.Context, inserter rowInserter, request []insertRow) (int, []backend.RowError, error) {
	savers := make([]bq.ValueSaver, len(request))
	for i, row := range request {
		savers[i] = row
	}
	err := inserter.Put(ctx, savers)
	if err == nil {
		return len(request), nil, nil
	}
	var multi bq.PutMultiError
	if !errors.As(err, &multi) || len(multi) == 0 {
		return 0, nil, classify("InsertRows", err)
	}
	var refused []backend.RowError
	for _, rowErr := range multi {
		if rowErr.RowIndex < 0 || rowErr.RowIndex >= len(request) {
			return 0, nil, serr.New(serr.Internal, "InsertRows", "BigQuery refused a row that is not in the request")
		}
		if !rowIsInvalid(rowErr) {
			// A reason that says nothing against the row: the call failed, not the row.
			return 0, nil, classify("InsertRows", rowErr.Errors)
		}
		refused = append(refused, backend.RowError{
			RowIndex: request[rowErr.RowIndex].index,
			Reason:   backend.RefusalInvalidValue,
			Error:    refusalText(rowErr),
		})
	}
	return len(request) - len(refused), refused, nil
}

// rowIsInvalid reports whether BigQuery gave the row the reason "invalid".
func rowIsInvalid(row bq.RowInsertionError) bool {
	for _, rowErr := range row.Errors {
		var byPointer *bq.Error
		var byValue bq.Error
		switch {
		case errors.As(rowErr, &byPointer):
			if byPointer.Reason == reasonInvalid {
				return true
			}
		case errors.As(rowErr, &byValue):
			if byValue.Reason == reasonInvalid {
				return true
			}
		}
	}
	return false
}

// refusalText describes a refused row in words of this server's: the column
// BigQuery located the refusal at, never BigQuery's own message.
func refusalText(row bq.RowInsertionError) string {
	for _, rowErr := range row.Errors {
		var byPointer *bq.Error
		var byValue bq.Error
		location := ""
		switch {
		case errors.As(rowErr, &byPointer):
			location = byPointer.Location
		case errors.As(rowErr, &byValue):
			location = byValue.Location
		}
		if location != "" {
			return fmt.Sprintf("the warehouse refused the value of column %q", location)
		}
	}
	return "the warehouse refused the row as invalid"
}

// repeatedColumn reports the first column name the schema uses twice, comparing
// names the way BigQuery does.
func repeatedColumn(schema *arrow.Schema) (string, bool) {
	seen := make(map[string]bool, schema.NumFields())
	for _, f := range schema.Fields() {
		key := strings.ToLower(f.Name)
		if seen[key] {
			return f.Name, true
		}
		seen[key] = true
	}
	return "", false
}

// schemaConflict says how a batch's columns disagree with the table's, or "" when
// they do not. It checks the names (BigQuery's are case-insensitive) and which
// columns are required, recursing into records; the types of the values are
// BigQuery's to judge, row by row.
func schemaConflict(batch *arrow.Schema, table bq.Schema) string {
	return fieldsConflict("", batch.Fields(), table)
}

func fieldsConflict(path string, batch []arrow.Field, table bq.Schema) string {
	known := make(map[string]*bq.FieldSchema, len(table))
	for _, f := range table {
		known[strings.ToLower(f.Name)] = f
	}
	carried := make(map[string]bool, len(batch))
	for _, f := range batch {
		name := strings.ToLower(f.Name)
		carried[name] = true
		column, ok := known[name]
		if !ok {
			return fmt.Sprintf("the table has no column %q", path+f.Name)
		}
		if st, ok := recordFields(f.Type); ok && fieldType(column.Type) == bq.RecordFieldType {
			if conflict := fieldsConflict(path+f.Name+".", st, column.Schema); conflict != "" {
				return conflict
			}
		}
	}
	for _, f := range table {
		if f.Required && !f.Repeated && strings.TrimSpace(f.DefaultValueExpression) == "" && !carried[strings.ToLower(f.Name)] {
			return fmt.Sprintf("the table requires column %q and the batch has none", path+f.Name)
		}
	}
	return ""
}

// recordFields is the fields of a struct, or of a list of structs, which is how
// a repeated record arrives.
func recordFields(t arrow.DataType) ([]arrow.Field, bool) {
	switch t := t.(type) {
	case *arrow.StructType:
		return t.Fields(), true
	case *arrow.ListType:
		return recordFields(t.Elem())
	case *arrow.LargeListType:
		return recordFields(t.Elem())
	default:
		return nil, false
	}
}
