// Package config resolves the server configuration from environment variables.
// The default backend is DuckDB: local and test contexts always run against an
// embedded DuckDB, and only a deployed environment names a cloud warehouse —
// so you develop against DuckDB and ship on BigQuery, and the gap is absorbed
// once, here, not in every app.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codefly-dev/core/callertoken"
	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
)

// Config is the fully resolved server configuration.
type Config struct {
	ListenAddr string
	Backend    backend.Config
	// AuthToken is the shared secret every caller must present as the
	// callertoken.MetadataKey metadata header, whitespace-trimmed: a secret delivered
	// through a file or a block scalar arrives with a trailing newline that no
	// client would send. Empty means the listener is anonymous, which FromEnv
	// resolves only when the operator asked for it with SWH_ALLOW_ANONYMOUS.
	AuthToken string
}

// FromEnv resolves configuration from SWH_* environment variables.
func FromEnv() (Config, error) {
	port, err := envInt("SWH_PORT", 0)
	if err != nil {
		return Config{}, err
	}
	maxQueryBytes, err := envInt("SWH_MAX_QUERY_BYTES", 0)
	if err != nil {
		return Config{}, err
	}
	queryTimeout, err := envDuration("SWH_QUERY_TIMEOUT", 0)
	if err != nil {
		return Config{}, err
	}
	// Unlike the two above, this limit has no "0 means the default": a zero that
	// reached the Arrow reader would be read as no bound, so it is refused here.
	maxArrowMessageBytes, err := envInt("SWH_MAX_ARROW_MESSAGE_BYTES", arrowipc.DefaultMaxMessageBytes)
	if err != nil {
		return Config{}, err
	}
	if maxArrowMessageBytes < 1 {
		return Config{}, fmt.Errorf("SWH_MAX_ARROW_MESSAGE_BYTES: must be positive, got %d", maxArrowMessageBytes)
	}
	// 0 is "the backend's own default", as for the two limits below it; a
	// negative count is not fewer streams, and the Storage Read API caps a
	// session at 1000, so a larger one could only be refused later.
	scanStreams, err := envInt("SWH_SCAN_STREAMS", 0)
	if err != nil {
		return Config{}, err
	}
	if scanStreams < 0 || scanStreams > backend.MaxScanStreams {
		return Config{}, fmt.Errorf("SWH_SCAN_STREAMS: must be between 0 (the backend default) and %d, got %d", backend.MaxScanStreams, scanStreams)
	}
	// A negative limit is not a smaller limit: both fields spend 0 as "use the
	// backend default", so a negative value would reach the backend as no limit
	// at all — the opposite of what an operator setting one intends.
	if maxQueryBytes < 0 {
		return Config{}, fmt.Errorf("SWH_MAX_QUERY_BYTES: must not be negative, got %d", maxQueryBytes)
	}
	if queryTimeout < 0 {
		return Config{}, fmt.Errorf("SWH_QUERY_TIMEOUT: must not be negative, got %s", queryTimeout)
	}

	cfg := Config{
		ListenAddr: env("SWH_LISTEN", ":9465"),
		Backend: backend.Config{
			Kind:            env("SWH_BACKEND", "duckdb"),
			Database:        os.Getenv("SWH_DATABASE"),
			DefaultDataset:  os.Getenv("SWH_DATASET"),
			Location:        os.Getenv("SWH_LOCATION"),
			DSN:             os.Getenv("SWH_DSN"),
			Host:            os.Getenv("SWH_HOST"),
			Port:            int(port),
			User:            os.Getenv("SWH_USER"),
			Password:        os.Getenv("SWH_PASSWORD"),
			Account:         os.Getenv("SWH_ACCOUNT"),
			CredentialsFile: os.Getenv("SWH_CREDENTIALS_FILE"),
			MaxQueryBytes:   maxQueryBytes,
			QueryTimeout:    queryTimeout,

			MaxArrowMessageBytes: maxArrowMessageBytes,
			ScanStreams:          int(scanStreams),
		},
		AuthToken: strings.TrimSpace(os.Getenv("SWH_AUTH_TOKEN")),
	}

	// Resolve the kind before anything derived from it, so an unimplemented
	// backend says so instead of first demanding a database for itself.
	reg, ok := backend.Lookup(cfg.Backend.Kind)
	if !ok {
		return Config{}, fmt.Errorf("unknown backend kind %q (registered: %v)",
			cfg.Backend.Kind, backend.Registered())
	}
	if !reg.SelfContained && cfg.Backend.Database == "" && !(reg.DSN && cfg.Backend.DSN != "") {
		need := "SWH_DATABASE"
		if reg.DSN {
			need += " (or SWH_DSN)"
		}
		return Config{}, fmt.Errorf("%s is required for backend %q", need, cfg.Backend.Kind)
	}
	// Last, so a configuration broken in more than one way reports the backend
	// first: the token cannot be what stops a server that names no real backend.
	if err := checkListenerIsAuthenticated(cfg.AuthToken); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// checkListenerIsAuthenticated refuses to resolve a configuration that would
// run SQL against the bound database for anyone who can reach the listen port.
// The listen address says nothing about who that is, so the token is the only
// thing this server can enforce itself. This is authentication only: it asks
// whether the caller holds the deployment's token, never what that caller may
// do — that is settled before the request arrives.
func checkListenerIsAuthenticated(token string) error {
	anonymous, err := envBool("SWH_ALLOW_ANONYMOUS")
	if err != nil {
		return err
	}
	return callertoken.CheckListener(token, anonymous, callertoken.Listener{
		TokenVar:     "SWH_AUTH_TOKEN",
		AnonymousVar: "SWH_ALLOW_ANONYMOUS",
		Exposure:     "can run SQL against the bound database",
	})
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envInt parses key, or returns def when it is unset. A malformed value is an
// error: silently falling back to def would turn a misspelled limit into no
// limit, which is the opposite of what setting one asks for.
func envInt(key string, def int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not an integer", key, v)
	}
	return n, nil
}

// envBool parses key as a boolean, or returns false when it is unset. A
// malformed value is an error for the same reason as envInt: "ture" must not
// read as the opposite of what the operator typed.
func envBool(key string) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %q is not a boolean (want true or false)", key, v)
	}
	return b, nil
}

// envDuration parses key as a Go duration, or returns def when it is unset. A
// malformed value is an error for the same reason as envInt — and a unitless
// number ("90") is malformed, so it can never be read as a silent zero.
func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration (want a unit, e.g. \"90s\")", key, v)
	}
	return d, nil
}
