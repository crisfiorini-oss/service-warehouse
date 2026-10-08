package bigquery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

func TestRegisteredAsAProjectBoundBackendWithoutADSN(t *testing.T) {
	reg, ok := backend.Lookup("bigquery")
	require.True(t, ok)
	require.False(t, reg.SelfContained, "a BigQuery server is bound to a project")
	require.False(t, reg.DSN, "BigQuery has no connection string")
	require.Contains(t, backend.Registered(), "bigquery")
}

// A failed open must hand back a nil interface: a nil *Backend inside a
// backend.Backend compares unequal to nil, and a caller checking the backend
// instead of the error would use it.
func TestRegisteredOpenReturnsANilBackendOnFailure(t *testing.T) {
	reg, ok := backend.Lookup("bigquery")
	require.True(t, ok)
	be, err := reg.Open(context.Background(), backend.Config{})
	require.Error(t, err)
	require.True(t, be == nil, "the backend is a true nil, not a typed one")
}

func TestOpenRefusesAConfigurationItCannotHonor(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  backend.Config
		want string
	}{
		"no project":             {backend.Config{}, "SWH_DATABASE"},
		"a DSN instead":          {backend.Config{DSN: "bigquery://p"}, "SWH_DATABASE"},
		"a project with a slash": {backend.Config{Database: "p/jobs"}, "project id"},
		"a project with a space": {backend.Config{Database: "my project"}, "project id"},
		"a DSN beside a project": {backend.Config{Database: "p-1", DSN: "x"}, "SWH_DSN"},
		"a host":                 {backend.Config{Database: "p-1", Host: "h"}, "SWH_HOST"},
		"a port":                 {backend.Config{Database: "p-1", Port: 5432}, "SWH_PORT"},
		"a user":                 {backend.Config{Database: "p-1", User: "u"}, "SWH_USER"},
		"a password":             {backend.Config{Database: "p-1", Password: "pw"}, "SWH_PASSWORD"},
		"an account":             {backend.Config{Database: "p-1", Account: "a"}, "SWH_ACCOUNT"},
		"a bad default dataset":  {backend.Config{Database: "p-1", DefaultDataset: "a/b"}, "dataset"},
		"a bad location":         {backend.Config{Database: "p-1", Location: "eu west"}, "SWH_LOCATION"},
	} {
		t.Run(name, func(t *testing.T) {
			b, err := Open(context.Background(), tc.cfg)
			require.Nil(t, b)
			require.True(t, serr.Is(err, serr.InvalidArgument), "got %v", err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestOpenNamesEverySettingItRejects(t *testing.T) {
	_, err := Open(context.Background(), backend.Config{Database: "p-1", User: "u", Password: "pw", Host: "h"})
	require.ErrorContains(t, err, "SWH_HOST, SWH_USER, SWH_PASSWORD")
	require.NotContains(t, err.Error(), "pw", "a secret is never repeated back")
}

func TestOpenFailsOnAnUnreadableCredentialsFileAndSaysWhy(t *testing.T) {
	_, err := Open(context.Background(), backend.Config{Database: "p-1", CredentialsFile: t.TempDir() + "/missing.json"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing.json", "the operator is told which file")
}

func TestCapabilitiesSayExactlyWhatWorks(t *testing.T) {
	b := newFake(t).open(t, backend.Config{MaxQueryBytes: 1 << 30})
	require.Equal(t, "bigquery", b.Name())
	require.Equal(t, backend.Capabilities{
		Backend:         "bigquery",
		AsyncJobs:       true,
		DryRun:          true,
		Parameters:      true,
		DDL:             true,
		Load:            false,
		Unload:          false,
		StreamingInsert: true,
		ArrowResults:    true,
		MaxQueryBytes:   1 << 30,
	}, b.Capabilities())
}

func TestWhatIsNotOfferedIsUnsupportedAndNeverEmpty(t *testing.T) {
	f := newFake(t)
	b := f.open(t, backend.Config{})
	ctx := context.Background()

	job, err := b.Load(ctx, backend.LoadOptions{Dest: backend.TableRef{Dataset: "d", Table: "t"}, SourceURIs: []string{"gs://b/o"}})
	require.Nil(t, job)
	require.True(t, serr.Is(err, serr.Unsupported))

	job, err = b.Unload(ctx, backend.UnloadOptions{SQL: "SELECT 1", DestURI: "gs://b/o"})
	require.Nil(t, job)
	require.True(t, serr.Is(err, serr.Unsupported))

	values, err := b.Native(ctx, "time-travel", map[string]string{"at": "now"})
	require.Nil(t, values)
	require.True(t, serr.Is(err, serr.Unsupported))

	require.Empty(t, f.requests)
	caps := b.Capabilities()
	require.False(t, caps.Load)
	require.False(t, caps.Unload)
	require.Empty(t, caps.LoadFormats)
	require.Empty(t, caps.NativeVerbs)
}

func TestLimitIsTheTighterOfTheCallersAndTheServers(t *testing.T) {
	require.Equal(t, int64(10), limit(int64(10), int64(100)))
	require.Equal(t, int64(100), limit(int64(1000), int64(100)))
	require.Equal(t, int64(100), limit(int64(0), int64(100)))
	require.Equal(t, int64(10), limit(int64(10), int64(0)))
	require.Equal(t, int64(0), limit(int64(0), int64(0)))
	require.Equal(t, time.Second, limit(time.Second, time.Minute))
}
