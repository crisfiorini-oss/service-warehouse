package duckdb

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// The kind is registered, needs no database, and opens: the default
// SWH_BACKEND=duckdb starts.
func TestRegisteredAndOpensFromTheRegistry(t *testing.T) {
	reg, ok := backend.Lookup("duckdb")
	require.True(t, ok)
	require.True(t, reg.SelfContained, "the database is an optional path")

	be, err := backend.Open(context.Background(), backend.Config{Kind: "duckdb"})
	require.NoError(t, err)
	require.Equal(t, "duckdb", be.Name())
	require.NoError(t, be.Close())
	require.NoError(t, be.Close(), "closing twice is harmless")
}

func TestFileDatabasePersistsAcrossOpens(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "warehouse.duckdb")

	first, err := Open(ctx, backend.Config{Kind: "duckdb", Database: path})
	require.NoError(t, err)
	_, err = first.CreateDataset(ctx, "analytics", backend.CreateDatasetOptions{})
	require.NoError(t, err)
	mustExec(t, first, `CREATE TABLE analytics.events (id BIGINT)`)
	mustExec(t, first, `INSERT INTO analytics.events VALUES (1), (2), (3)`)
	require.NoError(t, first.Close())
	require.FileExists(t, path)

	second, err := Open(ctx, backend.Config{Kind: "duckdb", Database: path})
	require.NoError(t, err)
	defer second.Close()
	tables, err := second.ListTables(ctx, backend.ListTablesOptions{Dataset: "analytics"})
	require.NoError(t, err)
	require.Len(t, tables.Tables, 1)
	require.Equal(t, []any{int64(3)}, run(t, second, `SELECT count(*) FROM analytics.events`).column(0))
	// Datasets are the schemas of this file's database, whatever the file is called.
	list, err := second.ListDatasets(ctx, backend.ListDatasetsOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"analytics", "main"}, datasetNames(list.Datasets))
}

func TestInMemoryDatabasesAreIndependent(t *testing.T) {
	a, b := open(t), open(t)
	mustExec(t, a, `CREATE TABLE only_in_a (x INT)`)
	require.Equal(t, serr.NotFound, code(t, b, `SELECT * FROM only_in_a`))
	for _, db := range []string{"", ":memory:"} {
		be, err := Open(context.Background(), backend.Config{Kind: "duckdb", Database: db})
		require.NoError(t, err, db)
		require.NoError(t, be.Close())
	}
}

// SWH_DSN carries the engine's own options and overrides SWH_DATABASE.
func TestDSNOverridesDatabaseAndCarriesEngineOptions(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "w.duckdb")
	seed, err := Open(ctx, backend.Config{Kind: "duckdb", Database: path})
	require.NoError(t, err)
	mustExec(t, seed, `CREATE TABLE t (x INT)`)
	require.NoError(t, seed.Close())

	ro, err := Open(ctx, backend.Config{Kind: "duckdb", Database: "ignored.duckdb", DSN: path + "?access_mode=read_only"})
	require.NoError(t, err)
	defer ro.Close()
	require.Equal(t, []any{int64(0)}, run(t, ro, `SELECT count(*) FROM t`).column(0))
	_, err = ro.Query(ctx, `INSERT INTO t VALUES (1)`, backend.QueryOptions{})
	require.Error(t, err, "the DSN's read_only mode holds")
	require.NotEqual(t, serr.Internal, serr.CodeOf(err))
	_, statErr := os.Stat("ignored.duckdb")
	require.True(t, os.IsNotExist(statErr), "SWH_DATABASE is not used when SWH_DSN is set")
}

func TestEngineOptionsFixedByTheServerCannotBeSetByTheDSN(t *testing.T) {
	for key := range fixedOptions {
		_, err := Open(context.Background(), backend.Config{Kind: "duckdb", DSN: "?" + key + "=true"})
		require.Equal(t, serr.InvalidArgument, serr.CodeOf(err), key)
		require.ErrorContains(t, err, key)
	}
	_, err := Open(context.Background(), backend.Config{Kind: "duckdb", DSN: "?no_such_option=1"})
	require.Error(t, err)
}

func TestEngineDSN(t *testing.T) {
	dsn, err := engineDSN(backend.Config{Database: "/data/w.duckdb"})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(dsn, "/data/w.duckdb?"), dsn)
	for k, v := range fixedOptions {
		require.Contains(t, dsn, k+"="+v)
	}
	dsn, err = engineDSN(backend.Config{Database: "ignored", DSN: "/data/x.duckdb?threads=2"})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(dsn, "/data/x.duckdb?"), dsn)
	require.Contains(t, dsn, "threads=2")
}

// A DSN the driver cannot split would drop the options without a word, leaving
// an engine that is open to the host. The options are read back after opening.
func TestStartupRefusesAnEngineWhoseOptionsDidNotApply(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a#b.duckdb") // '#' ends the DSN's query as a fragment
	_, err := Open(context.Background(), backend.Config{Kind: "duckdb", Database: path})
	require.Error(t, err)
	require.ErrorContains(t, err, "did not reach the engine")
}

// SQL a client sends cannot reach the host the server runs on. Each of these
// would work on a stock DuckDB.
func TestSQLCannotReachTheHost(t *testing.T) {
	be := open(t)
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret.csv")
	require.NoError(t, os.WriteFile(secret, []byte("a,b\n1,2\n"), 0o600))
	written := filepath.Join(dir, "written.csv")

	for name, sql := range map[string]string{
		"read a csv":        `SELECT * FROM read_csv('` + secret + `')`,
		"read a text file":  `SELECT * FROM read_text('` + secret + `')`,
		"list the host":     `SELECT * FROM glob('/*')`,
		"read a parquet":    `SELECT * FROM read_parquet('` + secret + `')`,
		"copy out":          `COPY (SELECT 1 AS a) TO '` + written + `'`,
		"attach a file":     `ATTACH '` + filepath.Join(dir, "other.duckdb") + `' AS other`,
		"export":            `EXPORT DATABASE '` + dir + `'`,
		"install":           `INSTALL httpfs`,
		"load":              `LOAD httpfs`,
		"reopen the door":   `SET enable_external_access = true`,
		"unlock":            `SET lock_configuration = false`,
		"allow extensions":  `SET autoinstall_known_extensions = true`,
		"over the network":  `SELECT * FROM read_csv('https://example.invalid/x.csv')`,
		"through a pragma":  `PRAGMA enable_external_access = true`,
		"from a call":       `CALL pragma_database_size()`,
		"in a subquery":     `SELECT (SELECT count(*) FROM read_csv('` + secret + `'))`,
		"to create a table": `CREATE TABLE x AS SELECT * FROM read_csv('` + secret + `')`,
	} {
		t.Run(name, func(t *testing.T) {
			res, err := be.Query(context.Background(), sql, backend.QueryOptions{})
			if err == nil && res.Batches != nil {
				_, err = res.Batches.Next()
				_ = res.Batches.Close()
			}
			require.Error(t, err, "this SQL must not run")
			require.Contains(t, []serr.Code{serr.PermissionDenied, serr.Unsupported, serr.InvalidArgument}, serr.CodeOf(err), err.Error())
			require.NotContains(t, err.Error(), dir, "a host path reached the client")
		})
	}
	_, statErr := os.Stat(written)
	require.True(t, os.IsNotExist(statErr), "COPY wrote a file on the host")
	require.Equal(t, serr.NotFound, code(t, be, `SELECT * FROM x`))

	r := run(t, be, `SELECT value FROM duckdb_settings() WHERE name = 'enable_external_access'`)
	require.Equal(t, []any{"false"}, r.column(0))
}

func TestCapabilitiesAreHonest(t *testing.T) {
	be := open(t)
	require.Equal(t, backend.Capabilities{
		Backend:         "duckdb",
		AsyncJobs:       false,
		DryRun:          true,
		Parameters:      true,
		DDL:             true,
		Load:            false,
		Unload:          false,
		StreamingInsert: true,
		ArrowResults:    true,
	}, be.Capabilities())
}

// What Capabilities says is not offered is Unsupported — never an empty success.
func TestWhatIsNotOfferedIsUnsupported(t *testing.T) {
	ctx := context.Background()
	be := open(t)

	_, err := be.GetJob(ctx, "any")
	require.Equal(t, serr.Unsupported, serr.CodeOf(err))
	require.Equal(t, serr.Unsupported, serr.CodeOf(be.CancelJob(ctx, "any")))
	_, err = be.Load(ctx, backend.LoadOptions{Dest: backend.TableRef{Dataset: "main", Table: "t"}, SourceURIs: []string{"gs://b/x.csv"}, Format: backend.FormatCSV})
	require.Equal(t, serr.Unsupported, serr.CodeOf(err))
	_, err = be.Unload(ctx, backend.UnloadOptions{SQL: "SELECT 1", DestURI: "gs://b/out", Format: backend.FormatParquet})
	require.Equal(t, serr.Unsupported, serr.CodeOf(err))
	_, err = be.Native(ctx, "checkpoint", nil)
	require.Equal(t, serr.Unsupported, serr.CodeOf(err))
}
