// Package mem is an in-memory warehouse backend: a fully working catalog
// (datasets + tables + DDL) with no query engine. It is the zero-dependency
// default for tests that need a backend and nothing else; the duckdb backend is
// the local engine when a query has to run. The query and data-movement plane
// returns serr.Unsupported, which the gRPC layer maps to UNIMPLEMENTED, so a
// client introspects Capabilities and never discovers the gap by a wrong
// answer.
package mem

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

func init() {
	backend.Register("mem", backend.Registration{
		SelfContained: true,
		Open: func(_ context.Context, cfg backend.Config) (backend.Backend, error) {
			return New(cfg), nil
		},
	})
}

type table struct {
	schema  backend.TableSchema
	created time.Time
}

// Backend is an in-memory catalog. It is safe for concurrent use.
type Backend struct {
	cfg backend.Config

	mu       sync.RWMutex
	datasets map[string]backend.Dataset
	tables   map[string]map[string]*table // dataset -> table -> def
}

// New builds an empty in-memory backend.
func New(cfg backend.Config) *Backend {
	return &Backend{
		cfg:      cfg,
		datasets: map[string]backend.Dataset{},
		tables:   map[string]map[string]*table{},
	}
}

func (b *Backend) Name() string { return "mem" }

func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{
		Backend:      "mem",
		DDL:          true,
		ArrowResults: false,
		NativeVerbs:  nil,
	}
}

// --- query plane: no engine ---

func (b *Backend) Query(context.Context, string, backend.QueryOptions) (*backend.QueryResult, error) {
	return nil, serr.New(serr.Unsupported, "Query", "mem backend has no query engine")
}

func (b *Backend) Scan(context.Context, backend.TableRef, backend.ScanOptions) (*backend.QueryResult, error) {
	return nil, serr.New(serr.Unsupported, "Scan", "mem backend holds no rows to read")
}

func (b *Backend) GetJob(context.Context, string) (*backend.Job, error) {
	return nil, serr.New(serr.NotFound, "GetJob", "mem backend runs no jobs")
}

func (b *Backend) CancelJob(context.Context, string) error {
	return serr.New(serr.NotFound, "CancelJob", "mem backend runs no jobs")
}

// --- catalog plane ---

func (b *Backend) ListDatasets(_ context.Context, _ backend.ListDatasetsOptions) (*backend.ListDatasetsResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]backend.Dataset, 0, len(b.datasets))
	for _, d := range b.datasets {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return &backend.ListDatasetsResult{Datasets: out}, nil
}

func (b *Backend) ListTables(_ context.Context, opts backend.ListTablesOptions) (*backend.ListTablesResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.datasets[opts.Dataset]; !ok {
		return nil, serr.New(serr.NotFound, "ListTables", "dataset not found: "+opts.Dataset)
	}
	tabs := b.tables[opts.Dataset]
	out := make([]backend.TableInfo, 0, len(tabs))
	for _, t := range tabs {
		out = append(out, t.schema.Info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref.Table < out[j].Ref.Table })
	return &backend.ListTablesResult{Tables: out}, nil
}

func (b *Backend) GetTable(_ context.Context, ref backend.TableRef) (*backend.TableSchema, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.tables[ref.Dataset][ref.Table]
	if !ok {
		return nil, serr.New(serr.NotFound, "GetTable", "table not found: "+ref.Dataset+"."+ref.Table)
	}
	s := t.schema
	return &s, nil
}

func (b *Backend) CreateDataset(_ context.Context, name string, opts backend.CreateDatasetOptions) (*backend.Dataset, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.datasets[name]; ok {
		if opts.IfNotExists {
			d := b.datasets[name]
			return &d, nil
		}
		return nil, serr.New(serr.AlreadyExists, "CreateDataset", "dataset exists: "+name)
	}
	d := backend.Dataset{Name: name, Location: opts.Location, Description: opts.Description, Labels: opts.Labels}
	b.datasets[name] = d
	b.tables[name] = map[string]*table{}
	return &d, nil
}

func (b *Backend) DropDataset(_ context.Context, name string, opts backend.DropDatasetOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.datasets[name]; !ok {
		if opts.IfExists {
			return nil
		}
		return serr.New(serr.NotFound, "DropDataset", "dataset not found: "+name)
	}
	if len(b.tables[name]) > 0 && !opts.Cascade {
		return serr.New(serr.PreconditionFailed, "DropDataset", "dataset not empty: "+name)
	}
	delete(b.datasets, name)
	delete(b.tables, name)
	return nil
}

func (b *Backend) CreateTable(_ context.Context, ref backend.TableRef, opts backend.CreateTableOptions) (*backend.TableSchema, error) {
	if opts.AsSelect != "" {
		return nil, serr.New(serr.Unsupported, "CreateTable", "CTAS needs a query engine")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.datasets[ref.Dataset]; !ok {
		return nil, serr.New(serr.NotFound, "CreateTable", "dataset not found: "+ref.Dataset)
	}
	if _, ok := b.tables[ref.Dataset][ref.Table]; ok {
		if opts.IfNotExists {
			s := b.tables[ref.Dataset][ref.Table].schema
			return &s, nil
		}
		return nil, serr.New(serr.AlreadyExists, "CreateTable", "table exists: "+ref.Table)
	}
	now := b.now()
	s := backend.TableSchema{
		Info: backend.TableInfo{
			Ref:      ref,
			Kind:     backend.KindTable,
			Created:  now,
			Modified: now,
		},
		Columns:     opts.Columns,
		PartitionBy: opts.PartitionBy,
		ClusterBy:   opts.ClusterBy,
	}
	b.tables[ref.Dataset][ref.Table] = &table{schema: s, created: now}
	return &s, nil
}

func (b *Backend) DropTable(_ context.Context, ref backend.TableRef, opts backend.DropTableOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.tables[ref.Dataset][ref.Table]; !ok {
		if opts.IfExists {
			return nil
		}
		return serr.New(serr.NotFound, "DropTable", "table not found: "+ref.Table)
	}
	delete(b.tables[ref.Dataset], ref.Table)
	return nil
}

// --- data movement: no engine ---

func (b *Backend) Load(context.Context, backend.LoadOptions) (*backend.Job, error) {
	return nil, serr.New(serr.Unsupported, "Load", "mem backend cannot load")
}

func (b *Backend) Unload(context.Context, backend.UnloadOptions) (*backend.Job, error) {
	return nil, serr.New(serr.Unsupported, "Unload", "mem backend cannot unload")
}

func (b *Backend) InsertRows(context.Context, backend.TableRef, []byte, backend.BatchReader) (*backend.InsertResult, error) {
	return nil, serr.New(serr.Unsupported, "InsertRows", "mem backend cannot ingest rows")
}

func (b *Backend) Native(_ context.Context, verb string, _ map[string]string) (map[string]string, error) {
	return nil, serr.New(serr.Unsupported, "Native", "mem backend has no native verbs: "+verb)
}

func (b *Backend) Close() error { return nil }

// now is overridable in tests; here it is wall-clock.
func (b *Backend) now() time.Time { return time.Now() }
