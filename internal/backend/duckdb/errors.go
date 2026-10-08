package duckdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// engineError is DuckDB's structured error. The engine is opened with
// errors_as_json, so a failure carries an enumerated exception type (and, for
// a catalog failure, an enumerated subtype) instead of prose; the mapping below
// reads those enumerations and never the human-readable message.
type engineError struct {
	Exception string `json:"exception_type"`
	Subtype   string `json:"error_subtype"`
	Kind      string `json:"type"`     // the catalog entry kind, e.g. "Table"
	Name      string `json:"name"`     // the catalog entry name, from the caller's own SQL
	Position  string `json:"position"` // byte offset into the caller's SQL
}

// decodeEngineError extracts the structured error from a DuckDB failure. The
// driver prefixes some messages with a type label, so the JSON object is taken
// from its first brace.
func decodeEngineError(err error) (engineError, bool) {
	var de *duckdb.Error
	if !errors.As(err, &de) {
		return engineError{}, false
	}
	start := strings.IndexByte(de.Msg, '{')
	if start < 0 {
		return engineError{}, false
	}
	var ee engineError
	if json.Unmarshal([]byte(de.Msg[start:]), &ee) != nil || ee.Exception == "" {
		return engineError{}, false
	}
	return ee, true
}

type rule struct {
	code serr.Code
	msg  string
}

// rules maps DuckDB's exception types to a code and a fixed message. The
// message is ours: it names the category and never repeats engine prose, which
// can quote the caller's SQL, host paths and catalog suggestions.
var rules = map[string]rule{
	"Parser":                 {serr.InvalidArgument, "SQL syntax error"},
	"Syntax":                 {serr.InvalidArgument, "SQL syntax error"},
	"Binder":                 {serr.InvalidArgument, "query does not bind: unknown column, ambiguous reference or mismatched types"},
	"Planner":                {serr.InvalidArgument, "query cannot be planned"},
	"Expression":             {serr.InvalidArgument, "invalid expression"},
	"Mismatch Type":          {serr.InvalidArgument, "operand types do not match"},
	"Invalid type":           {serr.InvalidArgument, "invalid type"},
	"Unknown Type":           {serr.InvalidArgument, "unknown type"},
	"Parameter Not Resolved": {serr.InvalidArgument, "a parameter's type cannot be determined; cast it in the SQL"},
	"Parameter Not Allowed":  {serr.InvalidArgument, "a parameter is not allowed here"},
	"Conversion":             {serr.InvalidArgument, "a value could not be converted to the required type"},
	"Out of Range":           {serr.InvalidArgument, "a value is out of range"},
	"Decimal":                {serr.InvalidArgument, "a decimal value is out of range"},
	"Divide by Zero":         {serr.InvalidArgument, "division by zero"},
	"Invalid Input":          {serr.InvalidArgument, "invalid input"},
	"Sequence":               {serr.InvalidArgument, "invalid sequence operation"},
	"Constraint":             {serr.PreconditionFailed, "a constraint was violated"},
	"Dependency":             {serr.PreconditionFailed, "other objects depend on this one"},
	"TransactionContext":     {serr.PreconditionFailed, "transaction conflict"},
	"Permission":             {serr.PermissionDenied, "operation not permitted by this server"},
	"Not implemented":        {serr.Unsupported, "the duckdb engine does not implement this"},
	"Missing Extension":      {serr.Unsupported, "the duckdb engine does not provide this extension"},
	"Extension Autoloading":  {serr.Unsupported, "the duckdb engine does not provide this extension"},
	"Settings":               {serr.Unsupported, "engine settings cannot be changed through this server"},
	"Invalid Configuration":  {serr.Unsupported, "engine settings cannot be changed through this server"},
	"Out of Memory":          {serr.Throttled, "the engine's memory limit was exceeded"},
}

// mapError turns any failure from the engine or the driver into a serr.Error.
// ctx is the context the failed call ran under: an interrupted statement is
// reported by what stopped it, not by the engine's interrupt message.
func mapError(ctx context.Context, op string, err error) error {
	if err == nil {
		return nil
	}
	var se *serr.Error
	if errors.As(err, &se) {
		return err
	}
	var ve *valueError
	if errors.As(err, &ve) {
		return serr.New(serr.Unsupported, op, "a result value cannot be carried exactly: "+ve.msg)
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
		return serr.New(serr.DeadlineExceeded, op, "the query exceeded its time limit")
	case errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled):
		return serr.New(serr.Internal, op, "the request was cancelled")
	}

	ee, ok := decodeEngineError(err)
	if !ok {
		log.Printf("duckdb: %s: unclassified driver error", op)
		return serr.New(serr.Internal, op, "the duckdb engine failed")
	}

	if ee.Exception == "Catalog" {
		return catalogError(op, ee)
	}
	if r, ok := rules[ee.Exception]; ok {
		msg := r.msg
		if ee.Position != "" && (ee.Exception == "Parser" || ee.Exception == "Syntax") {
			msg += " at offset " + ee.Position
		}
		return serr.New(r.code, op, msg)
	}
	// Everything left is the engine's own failure — I/O, internal, fatal,
	// serialization — which a client cannot act on. The category goes to the
	// operator's log; the client gets a fixed message.
	log.Printf("duckdb: %s: engine failure, exception_type=%q subtype=%q", op, ee.Exception, ee.Subtype)
	return serr.New(serr.Internal, op, "the duckdb engine failed")
}

// catalogError separates a missing object from an existing one, which DuckDB
// reports as the same exception type and tells apart only by subtype.
func catalogError(op string, ee engineError) error {
	subject := "object"
	if ee.Kind != "" {
		subject = strings.ToLower(ee.Kind)
	}
	if ee.Name != "" {
		subject += fmt.Sprintf(" %q", ee.Name)
	}
	switch ee.Subtype {
	case "MISSING_ENTRY":
		return serr.New(serr.NotFound, op, subject+" not found")
	case "ENTRY_ALREADY_EXISTS":
		return serr.New(serr.AlreadyExists, op, subject+" already exists")
	}
	return serr.New(serr.InvalidArgument, op, "the SQL names a catalog object that cannot be resolved")
}

// openCode classifies a failure to open the database. The engine has no
// structured error yet at that point (errors_as_json takes effect once the
// database exists), so the driver's own error type is all there is to read.
func openCode(err error) serr.Code {
	var de *duckdb.Error
	if errors.As(err, &de) && de.Type == duckdb.ErrorTypePermission {
		return serr.PermissionDenied
	}
	return serr.Internal
}
