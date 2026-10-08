package arrowipc

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// allocated is how many bytes f allocated: what the sizes a message states must
// not be able to choose.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// boundedAlloc is far above what refusing a message costs and far below what any
// message the tests state would cost to read.
const boundedAlloc = 8 << 20

func readAll(header []byte, src Source, opts ...Option) error {
	rd, err := NewReader(header, src, opts...)
	if err != nil {
		return err
	}
	defer rd.Release()
	for rd.Next() {
	}
	return rd.Err()
}

// oversizedBatch is a valid batch message, a few hundred bytes, whose metadata
// states a body of 16 TiB: the body bound refuses it before any of it is read.
func oversizedBatch(t *testing.T) []byte {
	t.Helper()
	enc, err := NewEncoder(testSchema)
	require.NoError(t, err)
	defer enc.Close()
	rec := batch([]int64{1, 2}, []string{"a", "b"})
	defer rec.Release()
	m, err := enc.Encode(rec)
	require.NoError(t, err)

	// The body length is the one 8-byte value in the metadata that equals the
	// bytes after it.
	metaLen := int(binary.LittleEndian.Uint32(m[4:8]))
	meta := m[8 : 8+metaLen]
	bodyLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(bodyLen, uint64(len(m)-8-metaLen))
	at := bytes.Index(meta, bodyLen)
	require.GreaterOrEqual(t, at, 0, "the metadata states the body length")
	require.Equal(t, -1, bytes.Index(meta[at+1:], bodyLen), "only once")

	oversized := append([]byte(nil), m...)
	binary.LittleEndian.PutUint64(oversized[8+at:], 1<<44)
	require.Less(t, len(oversized), 512)
	return oversized
}

func TestAMessageThatClaimsAHugeBodyIsRefusedBeforeItIsAllocated(t *testing.T) {
	header, err := SchemaMessage(testSchema)
	require.NoError(t, err)
	oversized := oversizedBatch(t)

	// A bound that is not positive is the default bound, never no bound.
	for name, opts := range map[string][]Option{
		"by default":              nil,
		"with a bound of zero":    {WithMaxMessageBytes(0)},
		"with a negative bound":   {WithMaxMessageBytes(-1)},
		"with a bound it exceeds": {WithMaxMessageBytes(1 << 20)},
	} {
		t.Run(name, func(t *testing.T) {
			var got error
			grew := allocated(func() { got = readAll(header, &msgs{list: [][]byte{oversized}}, opts...) })
			require.ErrorIs(t, got, ErrMalformed)
			require.Less(t, grew, uint64(boundedAlloc))
		})
	}
}

// The metadata is a schema or a buffer layout, so its size is bounded too, and the
// first four bytes of a stream that is not Arrow at all are read as the length.
func TestMetadataThatClaimsToBeHugeIsRefusedBeforeItIsAllocated(t *testing.T) {
	for name, header := range map[string][]byte{
		"a length of 2 GiB":    {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
		"text, read as length": []byte("not an arrow schema"),
	} {
		t.Run(name, func(t *testing.T) {
			var got error
			grew := allocated(func() { got = readAll(header, &msgs{}) })
			require.ErrorIs(t, got, ErrMalformed)
			require.Less(t, grew, uint64(boundedAlloc))
		})
	}
}

func TestTheBodyBoundIsTheCallersToSet(t *testing.T) {
	enc, err := NewEncoder(testSchema)
	require.NoError(t, err)
	defer enc.Close()
	rec := batch([]int64{1, 2}, []string{"a", "b"})
	defer rec.Release()
	m, err := enc.Encode(rec)
	require.NoError(t, err)
	header, err := SchemaMessage(testSchema)
	require.NoError(t, err)

	require.ErrorIs(t, readAll(header, &msgs{list: [][]byte{m}}, WithMaxMessageBytes(16)), ErrMalformed,
		"a batch whose body is over the bound is refused")
	require.NoError(t, readAll(header, &msgs{list: [][]byte{m}}, WithMaxMessageBytes(1<<10)))
}
