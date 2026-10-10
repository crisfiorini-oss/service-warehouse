package bigquery

import (
	"context"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

func TestScanCreatesTheSessionFromTheRequest(t *testing.T) {
	f := newScanFixture(t, backend.Config{ScanStreams: 7})
	f.read.session = newSession(1234, "s0")
	f.read.streams["s0"] = &scriptedStream{responses: responses(1, "n", 3)}

	res, err := f.scan(t, backend.ScanOptions{
		Selected: []string{"id"},
		Filter: []backend.Predicate{
			p("NAME", backend.OpEq, backend.TypeString, `a"b\c`),
			p("id", backend.OpGe, backend.TypeInt64, "2"),
		},
	})
	require.NoError(t, err)
	_ = drainScan(t, res)

	create, _ := f.read.requests()
	require.Len(t, create, 1)
	req := create[0]
	require.Equal(t, "projects/test-project", req.GetParent())
	require.EqualValues(t, 7, req.GetMaxStreamCount())
	rs := req.GetReadSession()
	require.Equal(t, "projects/test-project/datasets/d/tables/t", rs.GetTable())
	require.Equal(t, storagepb.DataFormat_ARROW, rs.GetDataFormat())
	require.Equal(t, []string{"id"}, rs.GetReadOptions().GetSelectedFields())
	require.Equal(t, "`name` = \"a\\\"b\\\\c\" AND `id` >= 2", rs.GetReadOptions().GetRowRestriction())
	// The table's metadata was read through the REST API, and no query ran.
	require.Len(t, f.rest.seen("GET", tablePath), 1)
	require.Empty(t, f.rest.seen("POST", "/jobs"), "a scan runs no query job")
}

func TestScanAsksForTheDefaultNumberOfStreams(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	f.read.session = newSession(0)
	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	_ = drainScan(t, res)
	create, _ := f.read.requests()
	require.EqualValues(t, defaultScanStreams, create[0].GetMaxStreamCount())
	require.Empty(t, create[0].GetReadSession().GetReadOptions().GetSelectedFields(), "no selection reads every column")
	require.Empty(t, create[0].GetReadSession().GetReadOptions().GetRowRestriction())
}

func TestScanMergesEveryStreamOnceAndInOrderWithinAStream(t *testing.T) {
	f := newScanFixture(t, backend.Config{ScanStreams: 3})
	f.read.session = newSession(5000, "a", "b", "c")
	f.read.streams["a"] = &scriptedStream{responses: responses(0, "n", 5, 5, 5)}
	f.read.streams["b"] = &scriptedStream{responses: responses(1000, "n", 7, 3)}
	f.read.streams["c"] = &scriptedStream{responses: responses(2000, "n", 1)}

	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	got := drainScan(t, res)

	require.Len(t, got.ids, 15+10+1)
	// Each stream's rows are in the order the stream sent them; the streams
	// interleave however they like.
	for _, run := range []struct{ from, to int64 }{{0, 15}, {1000, 1010}, {2000, 2001}} {
		var seq []int64
		for _, id := range got.ids {
			if id >= run.from && id < run.from+(run.to-run.from) {
				seq = append(seq, id)
			}
		}
		require.Len(t, seq, int(run.to-run.from))
		require.True(t, sort.SliceIsSorted(seq, func(i, j int) bool { return seq[i] < seq[j] }), "stream from %d is out of order", run.from)
	}
	seen := map[int64]int{}
	for _, id := range got.ids {
		seen[id]++
	}
	for id, n := range seen {
		require.Equal(t, 1, n, "row %d", id)
	}
	for i, id := range got.ids {
		require.Equal(t, "n"+strconv.FormatInt(id, 10), got.names[i], "the columns of a row stay together")
	}
	_, reads := f.read.requests()
	require.Len(t, reads, 3, "every stream is read once, none reopened")
}

func TestScanPutsTheEstimateInTheHeaderBeforeAnyBatch(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	f.read.session = newSession(987654, "s0")
	f.read.streams["s0"] = &scriptedStream{responses: responses(1, "n", 2), hold: true}

	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	defer func() { _ = res.Batches.Close() }()

	// Nothing has been read from the batches yet: the header already has it.
	require.EqualValues(t, 987654, res.Job.Stats.BytesScanned)
	require.Empty(t, res.Job.ID, "a scan has no job to look up")
	require.NotEmpty(t, res.ArrowSchema)
	require.Equal(t, []string{"id", "name"}, []string{res.Schema[0].Name, res.Schema[1].Name})
	require.Equal(t, backend.TypeInt64, res.Schema[0].Type)
	require.Equal(t, backend.TypeString, res.Schema[1].Type)
}

func TestScanHoldsTheEstimateToTheByteCap(t *testing.T) {
	f := newScanFixture(t, backend.Config{MaxQueryBytes: 1000})
	f.read.session = newSession(1001, "s0")
	f.read.streams["s0"] = &scriptedStream{responses: responses(1, "n", 2)}

	_, err := f.scan(t, backend.ScanOptions{})
	require.Equal(t, serr.Throttled, serr.CodeOf(err), "%v", err)
	_, reads := f.read.requests()
	require.Empty(t, reads, "nothing is read of a scan that is over the cap")

	f.read.session = newSession(1000, "s0")
	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	require.Len(t, drainScan(t, res).ids, 2)
}

func TestScanReturnsColumnsInTheOrderSelected(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	f.read.session = newSession(1, "s0")
	f.read.streams["s0"] = &scriptedStream{responses: responses(1, "n", 3)}

	res, err := f.scan(t, backend.ScanOptions{Selected: []string{"NAME", "id"}})
	require.NoError(t, err)
	require.Equal(t, []string{"name", "id"}, []string{res.Schema[0].Name, res.Schema[1].Name})
	got := drainScan(t, res)
	require.Equal(t, []int64{1, 2, 3}, got.ids)
	require.Equal(t, []string{"n1", "n2", "n3"}, got.names)

	create, _ := f.read.requests()
	require.Equal(t, []string{"name", "id"}, create[0].GetReadSession().GetReadOptions().GetSelectedFields(),
		"names go to the session as the table spells them")
}

func TestScanResplitsABigResponseIntoResultSizedBatches(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	f.read.session = newSession(1, "s0")
	f.read.streams["s0"] = &scriptedStream{responses: responses(0, "n", 20000, 5)}

	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	got := drainScan(t, res)

	require.Len(t, got.ids, 20005)
	for i, id := range got.ids {
		require.EqualValues(t, i, id, "no row lost, repeated or reordered")
	}
	for i, n := range got.rowsIn {
		require.LessOrEqual(t, n, int64(batchMaxRows), "message %d", i)
	}
	require.GreaterOrEqual(t, len(got.messages), 4, "20000 rows is more than two batches")
}

func TestScanResplitsByBytesWhenRowsAreWide(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	f.read.session = newSession(1, "s0")
	wide := strings.Repeat("x", 100_000)
	ids, names := make([]int64, 60), make([]string, 60)
	for i := range ids {
		ids[i], names[i] = int64(i), wide
	}
	f.read.streams["s0"] = &scriptedStream{responses: []response{{ids: ids, names: names}}}

	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	got := drainScan(t, res)

	require.Len(t, got.ids, 60)
	require.Greater(t, len(got.messages), 3, "6 MB of text does not travel in a few messages")
	for i, m := range got.messages {
		require.LessOrEqual(t, len(m), 2*batchTargetBytes, "message %d is %d bytes", i, len(m))
	}
}

// A response that holds a few huge rows among small ones cannot be cut well by
// the average row: a batch whose message is over the limit is cut again.
func TestScanCutsAgainWhenAMessageIsOverTheLimit(t *testing.T) {
	f := newScanFixture(t, backend.Config{MaxArrowMessageBytes: 64 << 10})
	f.read.session = newSession(1, "s0")
	ids, names := make([]int64, 200), make([]string, 200)
	for i := range ids {
		ids[i], names[i] = int64(i), "small"
	}
	for i := 0; i < 8; i++ {
		names[i] = strings.Repeat("y", 30_000) // the big rows are all at the front
	}
	f.read.streams["s0"] = &scriptedStream{responses: []response{{ids: ids, names: names}}}

	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	got := drainScan(t, res)

	require.Len(t, got.ids, 200)
	for i, id := range got.ids {
		require.EqualValues(t, i, id, "cutting again keeps the order")
	}
	for i, m := range got.messages {
		require.LessOrEqual(t, len(m), 64<<10, "message %d", i)
	}
}

func TestScanFailsOnARowTooBigToSend(t *testing.T) {
	f := newScanFixture(t, backend.Config{MaxArrowMessageBytes: 64 << 10})
	f.read.session = newSession(1, "s0")
	f.read.streams["s0"] = &scriptedStream{responses: []response{{ids: []int64{1}, names: []string{strings.Repeat("z", 200_000)}}}}

	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	defer func() { _ = res.Batches.Close() }()
	_, err = res.Batches.Next()
	require.Equal(t, serr.Unsupported, serr.CodeOf(err), "%v", err)
}

func TestScanResumesADroppedStreamAtItsRowOffset(t *testing.T) {
	f := newScanFixture(t, backend.Config{ScanStreams: 2})
	f.read.session = newSession(1, "a", "b")
	f.read.streams["a"] = &scriptedStream{
		responses: responses(0, "n", 4, 6, 5, 5),
		dropAfter: 2,
		dropErr:   status.Error(codes.Unavailable, "VENDOR connection reset"),
	}
	f.read.streams["b"] = &scriptedStream{responses: responses(100, "n", 3)}

	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	got := drainScan(t, res)

	seen := map[int64]int{}
	for _, id := range got.ids {
		seen[id]++
	}
	require.Len(t, seen, 20+3)
	for id, n := range seen {
		require.Equal(t, 1, n, "row %d was repeated by the resume", id)
	}

	_, reads := f.read.requests()
	var aOffsets []int64
	for _, r := range reads {
		if r.GetReadStream() == "a" {
			aOffsets = append(aOffsets, r.GetOffset())
		}
	}
	require.Equal(t, []int64{0, 10}, aOffsets, "the second read starts at the rows already delivered (4 + 6)")
}

func TestScanGivesUpOnAStreamThatMakesNoHeadway(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	f.read.session = newSession(1, "a")
	f.read.streams["a"] = &scriptedStream{failAlways: status.Error(codes.Unavailable, "VENDOR down")}

	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	defer func() { _ = res.Batches.Close() }()
	_, err = res.Batches.Next()
	require.Equal(t, serr.Internal, serr.CodeOf(err), "%v", err)
	require.NotContains(t, err.Error(), "VENDOR")
	_, reads := f.read.requests()
	require.Len(t, reads, readAttempts)
}

func TestScanDoesNotRetryAFailureARetryCannotCure(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	f.read.session = newSession(1, "a")
	f.read.streams["a"] = &scriptedStream{failAlways: status.Error(codes.InvalidArgument, "the restriction is not valid")}

	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	defer func() { _ = res.Batches.Close() }()
	_, err = res.Batches.Next()
	require.Equal(t, serr.InvalidArgument, serr.CodeOf(err), "%v", err)
	_, reads := f.read.requests()
	require.Len(t, reads, 1)
}

func TestScanKeepsAPermissionFailureDistinguishable(t *testing.T) {
	t.Run("creating the session", func(t *testing.T) {
		logged := quiet(t)
		f := newScanFixture(t, backend.Config{})
		f.read.createFn = func(*storagepb.CreateReadSessionRequest) (*storagepb.ReadSession, error) {
			return nil, status.Error(codes.PermissionDenied, "Permission 'bigquery.readsessions.create' denied on SECRET-PROJECT for serviceAccount:reader@x")
		}
		_, err := f.scan(t, backend.ScanOptions{})
		require.Equal(t, serr.PermissionDenied, serr.CodeOf(err), "%v", err)
		require.NotContains(t, err.Error(), "SECRET-PROJECT", "the vendor's text is the operator's")
		require.NotContains(t, err.Error(), "reader@x")
		_ = logged
	})
	t.Run("reading a stream", func(t *testing.T) {
		f := newScanFixture(t, backend.Config{})
		f.read.session = newSession(1, "a")
		f.read.streams["a"] = &scriptedStream{failAlways: status.Error(codes.PermissionDenied, "denied")}
		res, err := f.scan(t, backend.ScanOptions{})
		require.NoError(t, err)
		defer func() { _ = res.Batches.Close() }()
		_, err = res.Batches.Next()
		require.Equal(t, serr.PermissionDenied, serr.CodeOf(err), "%v", err)
		_, reads := f.read.requests()
		require.Len(t, reads, 1, "a denial is not retried")
	})
	t.Run("reading the table's metadata", func(t *testing.T) {
		quiet(t)
		f := newScanFixture(t, backend.Config{})
		f.table(403, apiError(403, "accessDenied", "no bigquery.tables.get for SECRET"))
		_, err := f.scan(t, backend.ScanOptions{})
		require.Equal(t, serr.PermissionDenied, serr.CodeOf(err), "%v", err)
		create, _ := f.read.requests()
		require.Empty(t, create)
	})
}

func TestScanNeverCreatesASessionForARequestItRefuses(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	f.read.session = newSession(1, "s0")
	for name, tc := range map[string]struct {
		opts backend.ScanOptions
		want serr.Code
	}{
		"a filter on a column the table lacks": {backend.ScanOptions{Filter: []backend.Predicate{p("nope", backend.OpEq, backend.TypeInt64, "1")}}, serr.InvalidArgument},
		"a selection of a column it lacks":     {backend.ScanOptions{Selected: []string{"nope"}}, serr.InvalidArgument},
		"a value of the wrong type":            {backend.ScanOptions{Filter: []backend.Predicate{p("id", backend.OpEq, backend.TypeString, "1")}}, serr.InvalidArgument},
		"a malformed value":                    {backend.ScanOptions{Filter: []backend.Predicate{p("id", backend.OpEq, backend.TypeInt64, "1; DROP")}}, serr.InvalidArgument},
		"no operator":                          {backend.ScanOptions{Filter: []backend.Predicate{{Field: "id"}}}, serr.InvalidArgument},
		"a column selected twice":              {backend.ScanOptions{Selected: []string{"id", "ID"}}, serr.InvalidArgument},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := f.scan(t, tc.opts)
			require.Equal(t, tc.want, serr.CodeOf(err), "%v", err)
		})
	}
	create, _ := f.read.requests()
	require.Empty(t, create)
}

func TestScanRefusesAViewAndAMissingTable(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	f.table(200, map[string]any{"tableReference": tableRef("d", "t"), "type": "VIEW"})
	_, err := f.scan(t, backend.ScanOptions{})
	require.Equal(t, serr.Unsupported, serr.CodeOf(err), "%v", err)

	f.rest.handle("GET "+tablesPath+"/gone", 404, apiError(404, "notFound", "Not found: table"))
	_, err = f.b.Scan(context.Background(), backend.TableRef{Dataset: "d", Table: "gone"}, backend.ScanOptions{})
	require.Equal(t, serr.NotFound, serr.CodeOf(err), "%v", err)

	_, err = f.b.Scan(context.Background(), backend.TableRef{Dataset: "d/tables/t", Table: "t"}, backend.ScanOptions{})
	require.Equal(t, serr.InvalidArgument, serr.CodeOf(err), "a dataset name cannot address a table")
	create, _ := f.read.requests()
	require.Empty(t, create)
}

func TestScanOfNoRowsIsAHeaderAndNoBatches(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	f.read.session = newSession(0) // a session with no streams
	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, res.ArrowSchema)
	got := drainScan(t, res)
	require.Empty(t, got.messages)
}

// Closing early stops the streams: the read the fake is holding open sees its
// client go, and Close does not wait for the rest of the table.
func TestScanCloseStopsTheStreams(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	f.read.session = newSession(1, "a")
	f.read.streams["a"] = &scriptedStream{responses: responses(0, "n", 5, 5, 5, 5, 5, 5, 5, 5), hold: true}

	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	_, err = res.Batches.Next()
	require.NoError(t, err)

	closed := make(chan struct{})
	go func() { _ = res.Batches.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
	select {
	case <-f.read.gone:
	case <-time.After(10 * time.Second):
		t.Fatal("the stream was not told its client left")
	}
	_, err = res.Batches.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, res.Batches.Close(), "Close may be called twice")
}

func TestScanStopsAtItsTimeLimit(t *testing.T) {
	f := newScanFixture(t, backend.Config{QueryTimeout: 200 * time.Millisecond})
	f.read.session = newSession(1, "a")
	f.read.streams["a"] = &scriptedStream{hold: true}
	res, err := f.scan(t, backend.ScanOptions{})
	require.NoError(t, err)
	defer func() { _ = res.Batches.Close() }()
	_, err = res.Batches.Next()
	require.Equal(t, serr.DeadlineExceeded, serr.CodeOf(err), "a scan that ran out of time is not a complete one: %v", err)
}

func TestScanIsOfferedAndItsStreamSettingIsChecked(t *testing.T) {
	f := newScanFixture(t, backend.Config{})
	require.True(t, f.b.Capabilities().TableScan)

	for _, n := range []int{-1, backend.MaxScanStreams + 1} {
		_, err := Open(context.Background(), backend.Config{Database: "test-project", ScanStreams: n})
		require.Equal(t, serr.InvalidArgument, serr.CodeOf(err), "%d", n)
	}
	b, err := Open(context.Background(), backend.Config{Database: "test-project", ScanStreams: backend.MaxScanStreams})
	require.NoError(t, err)
	require.NoError(t, b.Close(), "Close before any scan has nothing of the read client to close")
}
