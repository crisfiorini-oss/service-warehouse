// Package duckdb is the embedded local warehouse engine: DuckDB running
// in-process, behind the same contract as the cloud warehouses. A service
// develops and tests against this and ships on BigQuery, so the difference
// between the two is absorbed here once instead of in every consumer.
//
// What it honors is what the portable contract can mean on DuckDB, and the rest
// is serr.Unsupported — never a nearby answer:
//
//   - SQL goes in with named parameters (DuckDB's native $name) and comes out as
//     Arrow, with every integer and float widened to int64 and float64 so a
//     client reads the same Arrow types here as on any other backend;
//   - catalog and DDL calls map a dataset to a DuckDB schema;
//   - InsertRows appends Arrow batches and reports a refused row by offset;
//   - Scan reads a table's rows through a filter of bound parameters, no SQL
//     from the caller;
//   - jobs, load, unload, native verbs and byte caps are not offered.
//
// The engine is opened locked down: no file or network access from SQL, no
// extension loading, and a configuration that SQL cannot change. This server
// authorizes nothing, so the engine must not be a way to read the host.
package duckdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

func init() {
	backend.Register("duckdb", backend.Registration{
		SelfContained: true,
		Open:          Open,
	})
}

// fixedOptions are applied to every engine this package opens and cannot be
// overridden from SWH_DSN.
//
//   - errors_as_json makes failures structured, which is what errors.go maps;
//   - enable_external_access=false closes the host filesystem and network to SQL
//     (read_csv, COPY, ATTACH of a file, INSTALL);
//   - the autoinstall/autoload/community switches close the extension route that
//     enable_external_access alone leaves to the engine's own initiative.
//
// lock_configuration is set after startup (see Open) so SQL cannot undo any of
// these.
var fixedOptions = map[string]string{
	"errors_as_json":               "true",
	"enable_external_access":       "false",
	"autoinstall_known_extensions": "false",
	"autoload_known_extensions":    "false",
	"allow_community_extensions":   "false",
}

// Backend is the DuckDB engine. It holds the database, not a connection: every
// operation takes its own connection and closes it, so no session state — a
// temporary table, a USE — leaks from one request to another.
type Backend struct {
	cfg       backend.Config
	connector *duckdb.Connector
	// catalog is the name DuckDB gives the opened database (the file's name, or
	// "memory"). Datasets are the schemas inside it, and every catalog query
	// is scoped to it so the engine's own catalogs never appear as datasets.
	catalog string
	// ddl serializes catalog mutations, so the existence check that precedes a
	// create or a drop and the statement that follows it are one step. The
	// database file admits one writing process, so within this process that is
	// the whole race.
	ddl sync.Mutex

	closeOnce sync.Once
	closeErr  error
}

// Open opens the database named by cfg: SWH_DSN when set, else SWH_DATABASE, a
// file path (empty or ":memory:" is an in-memory database).
func Open(ctx context.Context, cfg backend.Config) (backend.Backend, error) {
	if cfg.MaxQueryBytes > 0 {
		// DuckDB scans local data and bills nothing, so it has no byte count to
		// cap. An operator who set a cap meant it; running uncapped would be the
		// opposite of that.
		return nil, serr.New(serr.Unsupported, "duckdb.Open",
			"duckdb cannot meter bytes scanned, so it cannot enforce SWH_MAX_QUERY_BYTES; leave it unset for this backend")
	}
	if cfg.ScanStreams != 0 {
		// A scan reads one local table on one connection; there are no streams to
		// parallelize. Ignoring the setting would let an operator believe it applies.
		return nil, serr.New(serr.Unsupported, "duckdb.Open",
			"duckdb reads a scan on one connection, so it has no streams to set; leave SWH_SCAN_STREAMS unset for this backend")
	}
	dsn, err := engineDSN(cfg)
	if err != nil {
		return nil, err
	}
	connector, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		// A failure to open goes to the operator's terminal, not a client, so
		// the engine's own message is the useful part.
		return nil, serr.Wrap(openCode(err), "duckdb.Open", err)
	}
	b := &Backend{cfg: cfg, connector: connector}
	if err := b.start(ctx); err != nil {
		_ = connector.Close()
		return nil, err
	}
	return b, nil
}

// engineDSN is the DuckDB DSN: the operator's path and options plus fixedOptions.
func engineDSN(cfg backend.Config) (string, error) {
	raw := cfg.DSN
	if raw == "" {
		raw = cfg.Database
	}
	path, query, _ := strings.Cut(raw, "?")
	opts, err := url.ParseQuery(query)
	if err != nil {
		return "", serr.New(serr.InvalidArgument, "duckdb.Open", "SWH_DSN options are malformed: "+err.Error())
	}
	for k := range fixedOptions {
		if _, set := opts[k]; set {
			return "", serr.New(serr.InvalidArgument, "duckdb.Open",
				fmt.Sprintf("SWH_DSN must not set %s: this server fixes it so SQL cannot reach the host", k))
		}
	}
	for k, v := range fixedOptions {
		opts.Set(k, v)
	}
	return path + "?" + opts.Encode(), nil
}

// start verifies that the fixed options took effect, records the catalog name,
// and locks the configuration. A DSN the driver could not split (a path holding
// '#', say) would drop the options silently, so they are read back rather than
// assumed.
func (b *Backend) start(ctx context.Context) error {
	s, err := b.session(ctx, "")
	if err != nil {
		return err
	}
	defer s.close()

	rows, err := s.fetchAll(ctx, `SELECT name, value FROM duckdb_settings()
		WHERE name IN ('errors_as_json', 'enable_external_access', 'autoinstall_known_extensions',
		               'autoload_known_extensions', 'allow_community_extensions')`)
	if err != nil {
		return err
	}
	got := map[string]string{}
	for _, r := range rows {
		got[asString(r[0])] = asString(r[1])
	}
	for k, want := range fixedOptions {
		if got[k] != want {
			return serr.New(serr.Internal, "duckdb.Open",
				fmt.Sprintf("engine option %s is %q, want %q: the DSN options did not reach the engine", k, got[k], want))
		}
	}

	rows, err = s.fetchAll(ctx, `SELECT current_database()`)
	if err != nil {
		return err
	}
	b.catalog = asString(rows[0][0])

	_, err = s.exec(ctx, `SET lock_configuration = true`)
	return err
}

func (b *Backend) Name() string { return "duckdb" }

// Capabilities states what this engine honors. A false flag is a call that
// returns Unsupported; a true one is exercised by this package's tests.
func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{
		Backend:         "duckdb",
		AsyncJobs:       false, // queries run to completion before the header is sent
		DryRun:          true,  // prepared, not executed; nothing is priced
		Parameters:      true,  // DuckDB's $name
		DDL:             true,
		Load:            false,
		Unload:          false,
		StreamingInsert: true,
		ArrowResults:    true,
		MaxQueryBytes:   0, // not meterable; see Open
		LoadFormats:     nil,
		NativeVerbs:     nil,
		TableScan:       true,
	}
}

// Close releases the database. A file database checkpoints its log here, so a
// server that exits without closing risks replaying a log on the next start.
func (b *Backend) Close() error {
	b.closeOnce.Do(func() { b.closeErr = b.connector.Close() })
	return b.closeErr
}

// --- operations this engine does not offer ---

func (b *Backend) GetJob(context.Context, string) (*backend.Job, error) {
	return nil, serr.New(serr.Unsupported, "GetJob", "duckdb runs every query to completion and keeps no job history")
}

func (b *Backend) CancelJob(context.Context, string) error {
	return serr.New(serr.Unsupported, "CancelJob", "duckdb runs every query to completion and keeps no job history")
}

func (b *Backend) Load(context.Context, backend.LoadOptions) (*backend.Job, error) {
	return nil, serr.New(serr.Unsupported, "Load",
		"duckdb is closed to object storage and the host filesystem, so it cannot load files")
}

func (b *Backend) Unload(context.Context, backend.UnloadOptions) (*backend.Job, error) {
	return nil, serr.New(serr.Unsupported, "Unload",
		"duckdb is closed to object storage and the host filesystem, so it cannot export files")
}

func (b *Backend) Native(_ context.Context, verb string, _ map[string]string) (map[string]string, error) {
	return nil, serr.New(serr.Unsupported, "Native", "duckdb has no native verbs: "+verb)
}

// timeout is how long one request may run: the shorter of what the request
// asked for and the operator's SWH_QUERY_TIMEOUT, so the operator's limit holds
// against a client that asks for more. Zero is no limit.
func (b *Backend) timeout(requested time.Duration) time.Duration {
	limit := b.cfg.QueryTimeout
	if requested > 0 && (limit == 0 || requested < limit) {
		limit = requested
	}
	return limit
}

func (b *Backend) withTimeout(ctx context.Context, requested time.Duration) (context.Context, context.CancelFunc) {
	if d := b.timeout(requested); d > 0 {
		return context.WithTimeout(ctx, d)
	}
	return context.WithCancel(ctx)
}

// dataset is the namespace for a name that may be empty: the one the request
// named, else SWH_DATASET, else DuckDB's own default schema.
func (b *Backend) dataset(name string) string {
	switch {
	case name != "":
		return name
	case b.cfg.DefaultDataset != "":
		return b.cfg.DefaultDataset
	}
	return "main"
}

// session is one connection to the database, used for one operation.
type session struct {
	conn driver.Conn
}

// session connects. A non-empty dataset becomes the search path, which is how
// unqualified names in the caller's SQL resolve.
func (b *Backend) session(ctx context.Context, dataset string) (*session, error) {
	conn, err := b.connector.Connect(ctx)
	if err != nil {
		return nil, mapError(ctx, "connect", err)
	}
	s := &session{conn: conn}
	if dataset != "" {
		if err := checkName(dataset); err != nil {
			s.close()
			return nil, serr.New(serr.InvalidArgument, "connect", "dataset: "+err.Error())
		}
		if _, err := s.exec(ctx, "USE "+quoteIdent(b.catalog)+"."+quoteIdent(dataset)); err != nil {
			s.close()
			// The statement is built from a checked name, so what it can fail on
			// is the dataset not existing.
			if serr.CodeOf(err) == serr.InvalidArgument {
				return nil, serr.New(serr.NotFound, "connect", fmt.Sprintf("dataset %q not found", dataset))
			}
			return nil, err
		}
	}
	return s, nil
}

func (s *session) close() { _ = s.conn.Close() }
