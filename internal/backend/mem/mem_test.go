package mem

import (
	"context"
	"testing"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
	"github.com/stretchr/testify/require"
)

func TestCatalogLifecycle(t *testing.T) {
	ctx := context.Background()
	b := New(backend.Config{})

	_, err := b.CreateDataset(ctx, "analytics", backend.CreateDatasetOptions{})
	require.NoError(t, err)

	// Duplicate without if_not_exists fails; with it, it is idempotent.
	_, err = b.CreateDataset(ctx, "analytics", backend.CreateDatasetOptions{})
	require.True(t, serr.Is(err, serr.AlreadyExists))
	_, err = b.CreateDataset(ctx, "analytics", backend.CreateDatasetOptions{IfNotExists: true})
	require.NoError(t, err)

	ref := backend.TableRef{Dataset: "analytics", Table: "events"}
	_, err = b.CreateTable(ctx, ref, backend.CreateTableOptions{
		Columns: []backend.Column{
			{Name: "id", Type: backend.TypeInt64},
			{Name: "name", Type: backend.TypeString, Nullable: true},
		},
	})
	require.NoError(t, err)

	got, err := b.GetTable(ctx, ref)
	require.NoError(t, err)
	require.Len(t, got.Columns, 2)
	require.Equal(t, "id", got.Columns[0].Name)

	tables, err := b.ListTables(ctx, backend.ListTablesOptions{Dataset: "analytics"})
	require.NoError(t, err)
	require.Len(t, tables.Tables, 1)

	// A non-empty dataset needs cascade.
	err = b.DropDataset(ctx, "analytics", backend.DropDatasetOptions{})
	require.True(t, serr.Is(err, serr.PreconditionFailed))
	require.NoError(t, b.DropDataset(ctx, "analytics", backend.DropDatasetOptions{Cascade: true}))

	_, err = b.GetTable(ctx, ref)
	require.True(t, serr.Is(err, serr.NotFound))
}

func TestQueryUnsupported(t *testing.T) {
	b := New(backend.Config{})
	_, err := b.Query(context.Background(), "SELECT 1", backend.QueryOptions{})
	require.True(t, serr.Is(err, serr.Unsupported))
}

func TestCapabilities(t *testing.T) {
	b := New(backend.Config{})
	caps := b.Capabilities()
	require.Equal(t, "mem", caps.Backend)
	require.True(t, caps.DDL)
	require.False(t, caps.ArrowResults)
}

// mem has no engine to ingest rows, so it says so instead of accepting rows it
// would drop, and it never reports a per-row refusal it did not make.
func TestInsertRowsUnsupported(t *testing.T) {
	b := New(backend.Config{})
	res, err := b.InsertRows(context.Background(), backend.TableRef{Dataset: "d", Table: "t"}, nil, nil)
	require.Nil(t, res)
	require.True(t, serr.Is(err, serr.Unsupported))
	require.False(t, b.Capabilities().StreamingInsert)
}
