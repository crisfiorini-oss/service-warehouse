// Package bigquery is the BigQuery backend: the whole catalog and DDL plane,
// SQL queries with bound parameters and a dry run, job inspection and
// cancellation, and streaming inserts, all in Arrow. Bulk load and unload and
// the Native verbs are not offered; they return serr.Unsupported and
// Capabilities says so, so a client is never told something false.
//
// The server is bound to one project (SWH_DATABASE). Credentials are
// Application Default Credentials, or the service-account file named by
// SWH_CREDENTIALS_FILE; nothing else is read from the environment and no
// credential is ever written into a request this server makes on its own.
//
// Every BigQuery failure is mapped to a serr.Code before it leaves this package
// (errors.go); a client branches on the code and never sees a reason token, an
// HTTP status or a vendor message, except that an invalid request keeps
// BigQuery's diagnosis of the caller's own SQL.
package bigquery

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"google.golang.org/api/option"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// Kind is the name this backend registers under, and the value of SWH_BACKEND
// that selects it.
const Kind = "bigquery"

func init() {
	backend.Register(Kind, backend.Registration{
		// A BigQuery server is bound to a project, which SWH_DATABASE names; it has
		// no connection string.
		SelfContained: false,
		DSN:           false,
		Open: func(ctx context.Context, cfg backend.Config) (backend.Backend, error) {
			b, err := Open(ctx, cfg)
			if err != nil {
				// Not `return b, err`: a nil *Backend in a backend.Backend is not nil.
				return nil, err
			}
			return b, nil
		},
	})
}

// Backend is the BigQuery warehouse backend. It is safe for concurrent use.
type Backend struct {
	cfg     backend.Config
	project string
	client  *bq.Client
}

var _ backend.Backend = (*Backend)(nil)

// projectID is the shape of a project id, including the legacy
// domain-scoped form (example.com:project). It is checked because the id is put
// into request paths.
var projectID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)

// Open builds the backend from cfg. Extra client options exist for tests, which
// point the client at a fake endpoint; the server passes none.
func Open(ctx context.Context, cfg backend.Config, opts ...option.ClientOption) (*Backend, error) {
	const op = "bigquery.Open"
	if cfg.Database == "" {
		return nil, serr.New(serr.InvalidArgument, op, "SWH_DATABASE must name the BigQuery project this server is bound to")
	}
	if !projectID.MatchString(cfg.Database) {
		return nil, serr.New(serr.InvalidArgument, op, fmt.Sprintf("SWH_DATABASE %q is not a valid BigQuery project id", cfg.Database))
	}
	// A setting this backend never reads must not be accepted: silently ignoring
	// SWH_USER would let an operator believe a login was configured.
	if unused := unusedSettings(cfg); len(unused) > 0 {
		return nil, serr.New(serr.InvalidArgument, op,
			fmt.Sprintf("%s is not used by the bigquery backend; remove it (credentials are Application Default Credentials or SWH_CREDENTIALS_FILE)",
				strings.Join(unused, ", ")))
	}
	if cfg.DefaultDataset != "" {
		if err := checkDatasetID(op, cfg.DefaultDataset); err != nil {
			return nil, err
		}
	}
	if cfg.Location != "" && !locationID.MatchString(cfg.Location) {
		return nil, serr.New(serr.InvalidArgument, op, fmt.Sprintf("SWH_LOCATION %q is not a BigQuery location", cfg.Location))
	}
	if cfg.CredentialsFile != "" {
		// Typed as a service account, so the file cannot silently be some other
		// kind of credential configuration.
		opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount, cfg.CredentialsFile))
	}
	client, err := bq.NewClient(ctx, cfg.Database, opts...)
	if err != nil {
		// Startup failures are the operator's to read, so the cause is kept.
		return nil, serr.Wrap(serr.Internal, op, fmt.Errorf("create the BigQuery client: %w", err))
	}
	client.Location = cfg.Location
	return &Backend{cfg: cfg, project: cfg.Database, client: client}, nil
}

// unusedSettings names the SWH_* settings that are set and mean nothing to
// BigQuery.
func unusedSettings(cfg backend.Config) []string {
	var unused []string
	for _, s := range []struct {
		name string
		set  bool
	}{
		{"SWH_DSN", cfg.DSN != ""},
		{"SWH_HOST", cfg.Host != ""},
		{"SWH_PORT", cfg.Port != 0},
		{"SWH_USER", cfg.User != ""},
		{"SWH_PASSWORD", cfg.Password != ""},
		{"SWH_ACCOUNT", cfg.Account != ""},
	} {
		if s.set {
			unused = append(unused, s.name)
		}
	}
	return unused
}

// Name is the backend kind.
func (b *Backend) Name() string { return Kind }

// Capabilities is what this backend honors. Load, unload and the native verbs
// are absent, and say so.
func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{
		Backend:         Kind,
		AsyncJobs:       true,
		DryRun:          true,
		Parameters:      true,
		DDL:             true,
		Load:            false,
		Unload:          false,
		StreamingInsert: true,
		ArrowResults:    true,
		MaxQueryBytes:   b.cfg.MaxQueryBytes,
		LoadFormats:     nil,
		NativeVerbs:     nil,
	}
}

// Load is not offered: bulk loads from object storage are a gap, not an
// emulation.
func (b *Backend) Load(context.Context, backend.LoadOptions) (*backend.Job, error) {
	return nil, serr.New(serr.Unsupported, "Load", "the bigquery backend does not offer bulk load")
}

// Unload is not offered.
func (b *Backend) Unload(context.Context, backend.UnloadOptions) (*backend.Job, error) {
	return nil, serr.New(serr.Unsupported, "Unload", "the bigquery backend does not offer unload")
}

// Native has no verbs on this backend.
func (b *Backend) Native(_ context.Context, verb string, _ map[string]string) (map[string]string, error) {
	return nil, serr.New(serr.Unsupported, "Native", "the bigquery backend has no native verbs: "+verb)
}

// Close releases the client.
func (b *Backend) Close() error { return b.client.Close() }

// resolve fills the default dataset into a table reference and validates the
// names, which are put into request paths.
func (b *Backend) resolve(op string, ref backend.TableRef) (backend.TableRef, error) {
	if ref.Dataset == "" {
		ref.Dataset = b.cfg.DefaultDataset
	}
	if ref.Dataset == "" {
		return ref, serr.New(serr.InvalidArgument, op, "the table names no dataset and SWH_DATASET is not set")
	}
	if err := checkDatasetID(op, ref.Dataset); err != nil {
		return ref, err
	}
	if err := checkTableID(op, ref.Table); err != nil {
		return ref, err
	}
	return ref, nil
}
