package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"

	"github.com/codefly-dev/service-warehouse/internal/serr"
)

func apiErr(status int, reasons ...string) *googleapi.Error {
	e := &googleapi.Error{Code: status, Message: "VENDOR MESSAGE for " + http.StatusText(status)}
	for _, r := range reasons {
		e.Errors = append(e.Errors, googleapi.ErrorItem{Reason: r, Message: "VENDOR ITEM"})
	}
	return e
}

func TestClassifyMapsEveryBigQueryFailureToANormalizedCode(t *testing.T) {
	quiet(t)
	for name, tc := range map[string]struct {
		err  error
		want serr.Code
	}{
		"not found by reason":     {apiErr(404, "notFound"), serr.NotFound},
		"not found by status":     {apiErr(404), serr.NotFound},
		"duplicate":               {apiErr(409, "duplicate"), serr.AlreadyExists},
		"conflict":                {apiErr(409), serr.AlreadyExists},
		"access denied":           {apiErr(403, "accessDenied"), serr.PermissionDenied},
		"forbidden":               {apiErr(403, "forbidden"), serr.PermissionDenied},
		"billing not enabled":     {apiErr(403, "billingNotEnabled"), serr.PermissionDenied},
		"unauthenticated":         {apiErr(401), serr.PermissionDenied},
		"forbidden by status":     {apiErr(403), serr.PermissionDenied},
		"rate limit":              {apiErr(403, "rateLimitExceeded"), serr.Throttled},
		"user rate limit":         {apiErr(403, "userRateLimitExceeded"), serr.Throttled},
		"quota":                   {apiErr(403, "quotaExceeded"), serr.Throttled},
		"bytes billed cap":        {apiErr(400, "bytesBilledLimitExceeded"), serr.Throttled},
		"resources exceeded":      {apiErr(400, "resourcesExceeded"), serr.Throttled},
		"too many requests":       {apiErr(429), serr.Throttled},
		"dataset in use":          {apiErr(400, "resourceInUse"), serr.PreconditionFailed},
		"precondition":            {apiErr(412), serr.PreconditionFailed},
		"invalid query":           {apiErr(400, "invalidQuery"), serr.InvalidArgument},
		"invalid":                 {apiErr(400, "invalid"), serr.InvalidArgument},
		"bad request":             {apiErr(400), serr.InvalidArgument},
		"timeout reason":          {apiErr(400, "timeout"), serr.DeadlineExceeded},
		"request timeout":         {apiErr(408), serr.DeadlineExceeded},
		"gateway timeout":         {apiErr(504), serr.DeadlineExceeded},
		"backend error":           {apiErr(503, "backendError"), serr.Internal},
		"internal error":          {apiErr(500, "internalError"), serr.Internal},
		"bad gateway":             {apiErr(502), serr.Internal},
		"not implemented":         {apiErr(501), serr.Unsupported},
		"not implemented reason":  {apiErr(400, "notImplemented"), serr.Unsupported},
		"stopped job":             {apiErr(400, "stopped"), serr.Internal},
		"reason beats status":     {apiErr(400, "notFound"), serr.NotFound},
		"unknown reason by code":  {apiErr(404, "somethingNew"), serr.NotFound},
		"first known reason wins": {apiErr(400, "somethingNew", "accessDenied"), serr.PermissionDenied},
		"job error by pointer":    {&bq.Error{Reason: "invalidQuery", Message: "VENDOR"}, serr.InvalidArgument},
		"job error by value":      {bq.Error{Reason: "accessDenied", Message: "VENDOR"}, serr.PermissionDenied},
		"multi error":             {bq.MultiError{&bq.Error{Reason: "notFound"}}, serr.NotFound},
		"wrapped":                 {fmt.Errorf("while reading: %w", apiErr(404, "notFound")), serr.NotFound},
		"sign-in refused":         {&oauth2.RetrieveError{Response: &http.Response{StatusCode: 400}}, serr.PermissionDenied},
		"deadline":                {context.DeadlineExceeded, serr.DeadlineExceeded},
		"deadline in a transport": {&url.Error{Op: "Get", URL: "x", Err: context.DeadlineExceeded}, serr.DeadlineExceeded},
		"canceled":                {context.Canceled, serr.Internal},
		"unknown":                 {errors.New("boom"), serr.Internal},
	} {
		t.Run(name, func(t *testing.T) {
			err := classify("Op", tc.err)
			require.True(t, serr.Is(err, tc.want), "got %v", err)
		})
	}
	require.NoError(t, classify("Op", nil))
}

func TestClassifyLeavesANormalizedErrorAlone(t *testing.T) {
	original := serr.New(serr.Unsupported, "Inner", "already normalized")
	require.Same(t, error(original), classify("Outer", original))
	wrapped := fmt.Errorf("context: %w", original)
	require.True(t, serr.Is(classify("Outer", wrapped), serr.Unsupported))
}

func TestWhatAClientIsToldIsWrittenHereNotTakenFromTheVendor(t *testing.T) {
	logged := quiet(t)

	for name, err := range map[string]error{
		"denied":   apiErr(403, "accessDenied"),
		"internal": apiErr(500, "backendError"),
		"throttle": apiErr(403, "quotaExceeded"),
		"missing":  apiErr(404, "notFound"),
		"conflict": apiErr(409, "duplicate"),
		"in use":   apiErr(400, "resourceInUse"),
		"unknown":  errors.New("VENDOR SECRET svc@example.iam.gserviceaccount.com"),
	} {
		t.Run(name, func(t *testing.T) {
			got := classify("Op", err)
			text := got.Error()
			require.NotContains(t, text, "VENDOR")
			require.NotContains(t, text, "svc@")
			var api *googleapi.Error
			require.False(t, errors.As(got, &api), "the vendor's error is not wrapped, so nothing downstream can format it")
			require.Equal(t, got, error(got.(*serr.Error)))
			require.NotEmpty(t, phrases[serr.CodeOf(got)])
		})
	}

	// An invalid request keeps the vendor's diagnosis of the caller's own SQL, and
	// only that: never its reason token or status.
	got := classify("Query", apiErr(400, "invalidQuery"))
	require.Contains(t, got.Error(), "VENDOR MESSAGE")
	require.NotContains(t, got.Error(), "invalidQuery")
	require.NotContains(t, got.Error(), "VENDOR ITEM")

	// Bounded, and never empty.
	long := &googleapi.Error{Code: 400, Message: strings.Repeat("x", 10*maxDetail), Errors: []googleapi.ErrorItem{{Reason: "invalid"}}}
	require.Less(t, len(classify("Query", long).Error()), 2*maxDetail)
	empty := &googleapi.Error{Code: 400, Errors: []googleapi.ErrorItem{{Reason: "invalid"}}}
	require.Contains(t, classify("Query", empty).Error(), phrases[serr.InvalidArgument])

	// What is not for the client is for the operator.
	require.NotEmpty(t, *logged)
}

func TestOnlyWhatTheOperatorMustSeeIsLogged(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	previous := logf
	logf = func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, fmt.Sprintf(format, args...))
	}
	defer func() { logf = previous }()

	classify("Get", apiErr(404, "notFound"))
	classify("Create", apiErr(409, "duplicate"))
	classify("Query", apiErr(400, "invalidQuery"))
	require.Empty(t, lines, "a caller's own mistake is not an operator's concern")

	classify("Query", apiErr(403, "accessDenied"))
	classify("Query", apiErr(500, "internalError"))
	require.Len(t, lines, 2)
	require.Contains(t, lines[0], "VENDOR MESSAGE", "the vendor's text reaches the operator's log in full")
}
