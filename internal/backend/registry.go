package backend

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Constructor opens a backend from a resolved Config.
type Constructor func(ctx context.Context, cfg Config) (Backend, error)

// Registration is what a backend kind declares about itself. A kind knows its
// own configuration requirements; nothing outside this package should restate
// them.
type Registration struct {
	// DSN marks a backend that reads SWH_DSN, a backend-native connection string
	// that stands in for the database. Without it the configuration error asks
	// for SWH_DATABASE alone, instead of offering a setting the backend would
	// refuse.
	DSN bool
	// SelfContained marks a backend that is not bound to a database — mem holds
	// its catalog in process, and DuckDB's database is an optional file path.
	// The zero value means bound, so a backend that says nothing is treated as
	// needing a database rather than silently skipping the check.
	SelfContained bool
	// Open constructs the backend.
	Open Constructor
}

var (
	mu       sync.RWMutex
	registry = map[string]Registration{}
)

// Register wires a backend kind to its registration. Backend packages call this
// from an init() so importing the package makes the kind available.
func Register(kind string, r Registration) {
	mu.Lock()
	defer mu.Unlock()
	registry[kind] = r
}

// Lookup returns the registration for a backend kind, reporting whether the
// kind is known at all.
func Lookup(kind string) (Registration, bool) {
	mu.RLock()
	defer mu.RUnlock()
	r, ok := registry[kind]
	return r, ok
}

// Open constructs the backend named by cfg.Kind.
func Open(ctx context.Context, cfg Config) (Backend, error) {
	r, ok := Lookup(cfg.Kind)
	if !ok {
		return nil, fmt.Errorf("unknown backend kind %q (registered: %v)", cfg.Kind, Registered())
	}
	return r.Open(ctx, cfg)
}

// Registered lists the available backend kinds, sorted.
func Registered() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
