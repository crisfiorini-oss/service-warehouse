package bigquery

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// logf is where the vendor's own text goes: to the operator, never to a client.
// A test swaps it to keep its output quiet.
var logf = log.Printf

// maxDetail bounds the diagnostic text of an invalid request that is passed on.
const maxDetail = 512

// failure is what a vendor error says about itself, read once so the
// classification below never touches an error type twice.
type failure struct {
	// http is the HTTP status BigQuery answered with, or 0 when there was none.
	http int
	// reasons are BigQuery's machine-readable reason tokens, the first being the
	// most specific.
	reasons []string
	// message is BigQuery's human-readable text.
	message string
	// grpc is the gRPC status code of a failure that came over gRPC, which is how
	// the Storage Read API answers; codes.OK when there was none.
	grpc codes.Code
}

// inspect reads the vendor error types this backend can meet. It reports false
// for an error that is none of them (a network failure, a context error).
func inspect(err error) (failure, bool) {
	var f failure
	found := false

	var api *googleapi.Error
	if errors.As(err, &api) {
		found = true
		f.http = api.Code
		f.message = api.Message
		for _, item := range api.Errors {
			if item.Reason != "" {
				f.reasons = append(f.reasons, item.Reason)
			}
		}
	}

	var byPointer *bq.Error
	var byValue bq.Error
	switch {
	case errors.As(err, &byPointer):
		found = true
		f.reasons = append(f.reasons, byPointer.Reason)
		if f.message == "" {
			f.message = byPointer.Message
		}
	case errors.As(err, &byValue):
		found = true
		f.reasons = append(f.reasons, byValue.Reason)
		if f.message == "" {
			f.message = byValue.Message
		}
	}

	var multi bq.MultiError
	if errors.As(err, &multi) {
		for _, one := range multi {
			if inner, ok := inspect(one); ok {
				found = true
				f.reasons = append(f.reasons, inner.reasons...)
				if f.http == 0 {
					f.http = inner.http
				}
				if f.message == "" {
					f.message = inner.message
				}
			}
		}
	}

	// The Storage Read API speaks gRPC, so its failures carry a status and no
	// HTTP code or reason token. A context error is not one: it has no status.
	if st, ok := status.FromError(err); ok && st.Code() != codes.OK && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		found = true
		f.grpc = st.Code()
		if f.message == "" {
			f.message = st.Message()
		}
	}

	var token *oauth2.RetrieveError
	if errors.As(err, &token) {
		// The credentials were refused when the server tried to sign in.
		found = true
		f.http = http.StatusUnauthorized
	}
	return f, found
}

// reasonCodes maps BigQuery's reason tokens to the normalized codes. This is the
// one place vendor tokens are read; nothing past this file sees them.
var reasonCodes = map[string]serr.Code{
	"notFound":                 serr.NotFound,
	"duplicate":                serr.AlreadyExists,
	"accessDenied":             serr.PermissionDenied,
	"forbidden":                serr.PermissionDenied,
	"billingNotEnabled":        serr.PermissionDenied,
	"invalidUser":              serr.PermissionDenied,
	"rateLimitExceeded":        serr.Throttled,
	"userRateLimitExceeded":    serr.Throttled,
	"quotaExceeded":            serr.Throttled,
	"bytesBilledLimitExceeded": serr.Throttled,
	"billingTierLimitExceeded": serr.Throttled,
	"resourcesExceeded":        serr.Throttled,
	"resourceInUse":            serr.PreconditionFailed,
	"invalid":                  serr.InvalidArgument,
	"invalidQuery":             serr.InvalidArgument,
	"badRequest":               serr.InvalidArgument,
	"notImplemented":           serr.Unsupported,
	"timeout":                  serr.DeadlineExceeded,
	"backendError":             serr.Internal,
	"internalError":            serr.Internal,
	"stopped":                  serr.Internal,
}

// httpCodes is the fallback when BigQuery named no reason it is known by.
var httpCodes = map[int]serr.Code{
	http.StatusBadRequest:          serr.InvalidArgument,
	http.StatusUnauthorized:        serr.PermissionDenied,
	http.StatusForbidden:           serr.PermissionDenied,
	http.StatusNotFound:            serr.NotFound,
	http.StatusRequestTimeout:      serr.DeadlineExceeded,
	http.StatusConflict:            serr.AlreadyExists,
	http.StatusPreconditionFailed:  serr.PreconditionFailed,
	http.StatusTooManyRequests:     serr.Throttled,
	http.StatusNotImplemented:      serr.Unsupported,
	http.StatusGatewayTimeout:      serr.DeadlineExceeded,
	http.StatusInternalServerError: serr.Internal,
	http.StatusBadGateway:          serr.Internal,
	http.StatusServiceUnavailable:  serr.Internal,
}

// grpcCodes is the fallback for a failure that came over gRPC and named no reason
// BigQuery is known by. Anything not listed (Unknown, Aborted, DataLoss, Canceled)
// is Internal.
var grpcCodes = map[codes.Code]serr.Code{
	codes.InvalidArgument:    serr.InvalidArgument,
	codes.OutOfRange:         serr.InvalidArgument,
	codes.Unauthenticated:    serr.PermissionDenied,
	codes.PermissionDenied:   serr.PermissionDenied,
	codes.NotFound:           serr.NotFound,
	codes.AlreadyExists:      serr.AlreadyExists,
	codes.FailedPrecondition: serr.PreconditionFailed,
	codes.ResourceExhausted:  serr.Throttled,
	codes.Unimplemented:      serr.Unsupported,
	codes.DeadlineExceeded:   serr.DeadlineExceeded,
	codes.Unavailable:        serr.Internal,
	codes.Internal:           serr.Internal,
}

// codeFor is the normalized code of a vendor failure: the first reason BigQuery
// gave that is known, else the HTTP status, else the gRPC status, else Internal.
func codeFor(f failure) serr.Code {
	for _, reason := range f.reasons {
		if code, ok := reasonCodes[reason]; ok {
			return code
		}
	}
	if code, ok := httpCodes[f.http]; ok {
		return code
	}
	if code, ok := grpcCodes[f.grpc]; ok {
		return code
	}
	return serr.Internal
}

// phrases are what a client is told of a failure, written here and not taken
// from the vendor: the same failure reads the same on every backend.
var phrases = map[serr.Code]string{
	serr.NotFound:           "the dataset, table or job does not exist",
	serr.AlreadyExists:      "the object already exists",
	serr.PreconditionFailed: "a precondition of the operation was not met",
	serr.Unsupported:        "the warehouse does not offer this operation",
	serr.PermissionDenied:   "the server's credentials are not allowed to do this",
	serr.Throttled:          "the warehouse throttled the request or a quota or byte limit was exceeded",
	serr.InvalidArgument:    "the request was rejected as invalid",
	serr.DeadlineExceeded:   "the operation did not finish within its time limit",
	serr.Internal:           "the warehouse reported an internal failure",
}

// describe is the message a client receives for code. An invalid request keeps
// the vendor's diagnosis, because that text describes the caller's own SQL or
// arguments ("Unrecognized name: x at [1:8]") and nothing else can; it never
// carries a reason token, a status or an identifier of the server's. Every
// other failure gets the fixed phrase: the vendor's text for a denial or an
// internal failure can name the server's own principals and resources, and goes
// to the operator's log instead.
func describe(code serr.Code, vendorMessage string) string {
	if code == serr.InvalidArgument {
		if text := strings.TrimSpace(vendorMessage); text != "" {
			if len(text) > maxDetail {
				text = text[:maxDetail]
			}
			return text
		}
	}
	return phrases[code]
}

// normalize is the normalized code of anything a call returned, and the message
// a client may be told of it.
func normalize(err error) (serr.Code, string) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return serr.DeadlineExceeded, phrases[serr.DeadlineExceeded]
	case errors.Is(err, context.Canceled):
		return serr.Internal, "the operation was canceled"
	}
	if f, ok := inspect(err); ok {
		code := codeFor(f)
		return code, describe(code, f.message)
	}
	return serr.Internal, phrases[serr.Internal]
}

// classify turns anything a call returned into a normalized serr error. An error
// already normalized passes through. The vendor's own error is logged when it is
// the operator's to read, and is never wrapped: Unwrap would hand its text to
// whoever formats the error.
func classify(op string, err error) error {
	if err == nil {
		return nil
	}
	var normalized *serr.Error
	if errors.As(err, &normalized) {
		return err
	}
	code, message := normalize(err)
	switch code {
	case serr.Internal, serr.PermissionDenied, serr.Throttled, serr.DeadlineExceeded, serr.Unsupported:
		logf("bigquery: %s: %v", op, err)
	}
	return serr.New(code, op, message)
}

// is reports whether err is the vendor failure that classifies as code.
func is(op string, err error, code serr.Code) bool {
	return err != nil && serr.Is(classify(op, err), code)
}
