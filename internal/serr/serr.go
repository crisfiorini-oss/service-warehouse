// Package serr is the normalized error model for the generic warehouse API.
// Every backend maps its provider-specific errors into these codes so a client
// never parses a BigQuery reason string or a Snowflake SQLSTATE. The set is the
// proven denominator across the warehouses.
package serr

import (
	"errors"
	"fmt"
)

// Code is the backend-independent error class.
type Code int

const (
	// Internal is an unexpected/unclassified failure.
	Internal Code = iota
	// NotFound means the dataset, table, or job does not exist.
	NotFound
	// AlreadyExists means an if-not-exists create found an existing object.
	AlreadyExists
	// PreconditionFailed means a conditional DDL/DML precondition was not met.
	PreconditionFailed
	// Unsupported means the backend does not offer this operation/option.
	Unsupported
	// PermissionDenied means the credentials lack authorization.
	PermissionDenied
	// Throttled means the backend rate-limited or quota-limited the request.
	Throttled
	// InvalidArgument means the request (or SQL) was malformed.
	InvalidArgument
	// DeadlineExceeded means the operation did not finish within its time
	// limit: the caller's or the server's configured one.
	DeadlineExceeded
)

func (c Code) String() string {
	switch c {
	case NotFound:
		return "NotFound"
	case AlreadyExists:
		return "AlreadyExists"
	case PreconditionFailed:
		return "PreconditionFailed"
	case Unsupported:
		return "Unsupported"
	case PermissionDenied:
		return "PermissionDenied"
	case Throttled:
		return "Throttled"
	case InvalidArgument:
		return "InvalidArgument"
	case DeadlineExceeded:
		return "DeadlineExceeded"
	default:
		return "Internal"
	}
}

// Error carries a normalized Code, the operation that produced it, and the
// underlying cause.
type Error struct {
	Code Code
	Op   string
	Err  error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("%s: %s", e.Op, e.Code)
	}
	return fmt.Sprintf("%s: %s: %v", e.Op, e.Code, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// New builds a normalized error with a message.
func New(code Code, op, msg string) *Error {
	return &Error{Code: code, Op: op, Err: errors.New(msg)}
}

// Wrap wraps a cause under a normalized code.
func Wrap(code Code, op string, err error) *Error {
	return &Error{Code: code, Op: op, Err: err}
}

// CodeOf extracts the normalized code, defaulting to Internal for a plain error.
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return Internal
}

// Is reports whether err carries the given normalized code.
func Is(err error, code Code) bool {
	return err != nil && CodeOf(err) == code
}
