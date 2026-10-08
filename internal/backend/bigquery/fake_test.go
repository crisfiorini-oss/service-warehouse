package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/codefly-dev/service-warehouse/internal/backend"
)

// The tests below run the backend against a fake of BigQuery's REST API served
// from this process. It is not BigQuery: it answers what a test programs and
// records what the client library sent, so a test can assert the requests the
// backend builds and the way it reads the answers. Nothing leaves the machine.
// What only the real service can say is the integration test's to check.

// recorded is one request the fake received.
type recorded struct {
	Method string
	Path   string
	Query  map[string][]string
	Body   map[string]any
}

type fake struct {
	srv *httptest.Server
	mux *http.ServeMux

	mu       sync.Mutex
	requests []recorded
}

func newFake(t *testing.T) *fake {
	t.Helper()
	f := &fake{mux: http.NewServeMux()}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec := recorded{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query()}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &rec.Body)
		}
		f.mu.Lock()
		f.requests = append(f.requests, rec)
		f.mu.Unlock()
		f.mux.ServeHTTP(w, r)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

// handle serves pattern ("POST /projects/{p}/jobs") with a JSON body.
func (f *fake) handle(pattern string, status int, body any) {
	f.mux.HandleFunc(pattern, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, status, body)
	})
}

func (f *fake) handleFunc(pattern string, fn func(w http.ResponseWriter, r *http.Request)) {
	f.mux.HandleFunc(pattern, fn)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// apiError is the body BigQuery gives a failed request.
func apiError(status int, reason, message string) map[string]any {
	return map[string]any{"error": map[string]any{
		"code":    status,
		"message": message,
		"errors":  []map[string]any{{"reason": reason, "message": message, "domain": "global"}},
		"status":  "SOME_VENDOR_STATUS",
	}}
}

// seen returns the requests whose method and path match.
func (f *fake) seen(method, pathContains string) []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recorded
	for _, r := range f.requests {
		if r.Method == method && strings.Contains(r.Path, pathContains) {
			out = append(out, r)
		}
	}
	return out
}

// open builds a Backend whose client talks to the fake.
func (f *fake) open(t *testing.T, cfg backend.Config) *Backend {
	t.Helper()
	if cfg.Database == "" {
		cfg.Database = "test-project"
	}
	b, err := Open(context.Background(), cfg,
		option.WithEndpoint(f.srv.URL), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// quiet keeps the operator log of a test run clean, and returns what was logged.
func quiet(t *testing.T) *[]string {
	t.Helper()
	var (
		mu     sync.Mutex
		logged []string
	)
	previous := logf
	logf = func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		logged = append(logged, fmt.Sprintf(format, args...))
	}
	t.Cleanup(func() { logf = previous })
	return &logged
}
