package config

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/backend"

	// The kinds FromEnv resolves exist only once their packages are linked in,
	// exactly as in cmd/service-warehouse. Without these the tests would assert
	// against an empty registry.
	_ "github.com/codefly-dev/service-warehouse/internal/backend/duckdb"
	_ "github.com/codefly-dev/service-warehouse/internal/backend/mem"
)

// boundKind registers a backend that is bound to a database and can be given a
// DSN instead, standing in for the cloud backends that are not implemented.
// Registering a real kind's name would assert a support claim this repository
// does not make. noDSNKind is the same without a DSN, which is what a project-
// bound backend such as BigQuery looks like to the configuration.
const (
	boundKind = "test-bound"
	noDSNKind = "test-bound-no-dsn"
)

func init() {
	backend.Register(boundKind, backend.Registration{
		DSN:  true,
		Open: func(context.Context, backend.Config) (backend.Backend, error) { return nil, nil },
	})
	backend.Register(noDSNKind, backend.Registration{
		Open: func(context.Context, backend.Config) (backend.Backend, error) { return nil, nil },
	})
}

// clearEnv blanks every SWH_* variable so a test describes the whole
// environment rather than inheriting the developer's shell. The one thing it
// then sets is a token: resolution refuses an unauthenticated listener, and
// that is orthogonal to what most tests here assert. The auth tests clear it
// again to describe their own posture.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"SWH_LISTEN", "SWH_BACKEND", "SWH_DATABASE", "SWH_DATASET", "SWH_LOCATION",
		"SWH_DSN", "SWH_HOST", "SWH_PORT", "SWH_USER", "SWH_PASSWORD", "SWH_ACCOUNT",
		"SWH_CREDENTIALS_FILE", "SWH_MAX_QUERY_BYTES", "SWH_QUERY_TIMEOUT",
		"SWH_AUTH_TOKEN", "SWH_ALLOW_ANONYMOUS",
	} {
		t.Setenv(k, "")
	}
	t.Setenv("SWH_AUTH_TOKEN", "a-per-run-secret")
}

func TestSelfContainedBackendsNeedNoDatabase(t *testing.T) {
	for _, kind := range []string{"mem", "duckdb"} {
		t.Run(kind, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SWH_BACKEND", kind)

			cfg, err := FromEnv()
			require.NoError(t, err)
			require.Equal(t, kind, cfg.Backend.Kind)
			require.Empty(t, cfg.Backend.Database)
		})
	}
}

func TestBoundBackendRequiresDatabaseOrDSN(t *testing.T) {
	clearEnv(t)
	t.Setenv("SWH_BACKEND", boundKind)

	_, err := FromEnv()
	require.ErrorContains(t, err, "SWH_DATABASE")
	require.ErrorContains(t, err, boundKind)
}

// A backend that reads no DSN must not be told to supply one: that sends the
// operator to set a variable the backend then refuses.
func TestBackendWithoutDSNAsksForTheDatabaseOnly(t *testing.T) {
	clearEnv(t)
	t.Setenv("SWH_BACKEND", noDSNKind)

	_, err := FromEnv()
	require.ErrorContains(t, err, "SWH_DATABASE")
	require.NotContains(t, err.Error(), "SWH_DSN")

	t.Setenv("SWH_DSN", "user:pass@account/db")
	_, err = FromEnv()
	require.ErrorContains(t, err, "SWH_DATABASE", "a DSN does not stand in for the database of a backend that has none")
}

func TestBoundBackendAcceptsDSNAlone(t *testing.T) {
	clearEnv(t)
	t.Setenv("SWH_BACKEND", boundKind)
	t.Setenv("SWH_DSN", "user:pass@account/db")

	cfg, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, "user:pass@account/db", cfg.Backend.DSN)
}

// An unregistered kind is reported as unimplemented, not as a missing database:
// telling an operator to supply SWH_DATABASE sends them to provision one for a
// backend that does not exist.
func TestUnknownBackendIsRejectedBeforeItsDatabase(t *testing.T) {
	clearEnv(t)
	t.Setenv("SWH_BACKEND", "snowflake")

	_, err := FromEnv()
	require.ErrorContains(t, err, `unknown backend kind "snowflake"`)
	require.ErrorContains(t, err, "mem")
	require.NotContains(t, err.Error(), "SWH_DATABASE")
}

// A malformed limit must not resolve to the zero value: every backend spends 0
// as "no limit", so silently falling back would uncap a query the operator
// meant to cap.
func TestMalformedLimitsAreRejected(t *testing.T) {
	for _, tc := range []struct{ key, value, want string }{
		{"SWH_MAX_QUERY_BYTES", "1GB", "not an integer"},
		{"SWH_MAX_QUERY_BYTES", "1_000_000", "not an integer"},
		{"SWH_MAX_QUERY_BYTES", "1e9", "not an integer"},
		{"SWH_MAX_QUERY_BYTES", "-5", "must not be negative"},
		{"SWH_QUERY_TIMEOUT", "90", "not a duration"},
		{"SWH_QUERY_TIMEOUT", "5 minutes", "not a duration"},
		{"SWH_QUERY_TIMEOUT", "-30s", "must not be negative"},
		{"SWH_PORT", "abc", "not an integer"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(tc.key, tc.value)

			_, err := FromEnv()
			require.Error(t, err)
			require.ErrorContains(t, err, tc.key)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestDefaultsAndParsing(t *testing.T) {
	clearEnv(t)
	t.Setenv("SWH_MAX_QUERY_BYTES", "4096")
	t.Setenv("SWH_QUERY_TIMEOUT", "90s")
	t.Setenv("SWH_PORT", "5439")

	cfg, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, ":9465", cfg.ListenAddr)
	require.Equal(t, "duckdb", cfg.Backend.Kind)
	require.Equal(t, int64(4096), cfg.Backend.MaxQueryBytes)
	require.Equal(t, 90*time.Second, cfg.Backend.QueryTimeout)
	require.Equal(t, 5439, cfg.Backend.Port)
}

// Unset limits keep meaning "use the backend default"; rejecting malformed
// values must not make an absent value an error.
func TestUnsetLimitsFallBackToDefaults(t *testing.T) {
	clearEnv(t)

	cfg, err := FromEnv()
	require.NoError(t, err)
	require.Zero(t, cfg.Backend.MaxQueryBytes)
	require.Zero(t, cfg.Backend.QueryTimeout)
	require.Zero(t, cfg.Backend.Port)
}

// TestFromEnvRefusesAnUnauthenticatedListener is the startup guard: with
// neither a token nor an explicit opt-out the server does not resolve, and the
// message names both ways out and the exposed capability.
func TestFromEnvRefusesAnUnauthenticatedListener(t *testing.T) {
	clearEnv(t)
	t.Setenv("SWH_AUTH_TOKEN", "")

	_, err := FromEnv()
	require.Error(t, err)
	for _, want := range []string{"SWH_AUTH_TOKEN", "can run SQL", "SWH_ALLOW_ANONYMOUS"} {
		require.ErrorContains(t, err, want)
	}
}

// A whitespace-only token is not a token: it would install interceptors nobody
// can satisfy, or be taken for a configured credential while naming nothing.
func TestFromEnvTreatsABlankTokenAsAbsent(t *testing.T) {
	clearEnv(t)
	t.Setenv("SWH_AUTH_TOKEN", "   \n")

	_, err := FromEnv()
	require.ErrorContains(t, err, "SWH_AUTH_TOKEN is required")
}

// A secret delivered through a file or a block scalar carries a trailing
// newline. The enforced token must be what a client can actually send, or every
// caller is refused while the server logs that auth is on.
func TestFromEnvTrimsTheToken(t *testing.T) {
	clearEnv(t)
	t.Setenv("SWH_AUTH_TOKEN", "s3cr3t\n")

	cfg, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, "s3cr3t", cfg.AuthToken)
}

// A token together with the anonymous opt-out is the shape of a half-finished
// migration; picking either silently leaves the operator believing the other.
func TestFromEnvRejectsATokenWithTheAnonymousOptOut(t *testing.T) {
	clearEnv(t)
	t.Setenv("SWH_ALLOW_ANONYMOUS", "true")

	_, err := FromEnv()
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestFromEnvAcceptsAnExplicitAnonymousOptOut(t *testing.T) {
	clearEnv(t)
	t.Setenv("SWH_AUTH_TOKEN", "")
	t.Setenv("SWH_ALLOW_ANONYMOUS", "true")

	cfg, err := FromEnv()
	require.NoError(t, err)
	require.Empty(t, cfg.AuthToken)
}

// A misspelled opt-out is an error, not a silent false: the refusal that
// followed would blame the missing token for a server whose operator did set the
// opt-out, and send them to provision a secret they chose not to have.
func TestFromEnvRejectsAMalformedAnonymousOptOut(t *testing.T) {
	for _, value := range []string{"ture", "yes", "2"} {
		t.Run(value, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SWH_AUTH_TOKEN", "")
			t.Setenv("SWH_ALLOW_ANONYMOUS", value)

			_, err := FromEnv()
			require.ErrorContains(t, err, "SWH_ALLOW_ANONYMOUS")
			require.ErrorContains(t, err, "not a boolean")
		})
	}
}

// A configuration broken in more than one way reports the backend first: the
// operator fixing the token should not then meet an error that was there all
// along.
func TestFromEnvReportsTheBackendBeforeTheToken(t *testing.T) {
	clearEnv(t)
	t.Setenv("SWH_AUTH_TOKEN", "")
	t.Setenv("SWH_BACKEND", "bigquery")

	_, err := FromEnv()
	require.ErrorContains(t, err, `unknown backend kind "bigquery"`)
	require.NotContains(t, err.Error(), "SWH_AUTH_TOKEN")
}
