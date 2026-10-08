package arrowipc

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"
)

// nestedSchema has everything the walk follows: struct and list children,
// custom metadata on the schema and on fields, a union with type ids and a
// timestamp with a time zone.
func nestedSchema() *arrow.Schema {
	meta := func(kv ...string) arrow.Metadata {
		var keys, vals []string
		for i := 0; i < len(kv); i += 2 {
			keys, vals = append(keys, kv[i]), append(vals, kv[i+1])
		}
		return arrow.NewMetadata(keys, vals)
	}
	union := arrow.SparseUnionOf(
		[]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int32}, {Name: "s", Type: arrow.BinaryTypes.String}},
		[]arrow.UnionTypeCode{3, 5},
	)
	m := meta("origin", "tests", "ARROW:note", "kept")
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.StructOf(
			arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int32, Metadata: meta("unit", "m")},
			arrow.Field{Name: "y", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		), Metadata: meta("a", "1", "b", "2")},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Europe/Paris"}, Metadata: meta("k", "v")},
		{Name: "u", Type: union},
	}, &m)
}

func schemaOf(t testing.TB, s *arrow.Schema) []byte {
	t.Helper()
	m, err := SchemaMessage(s)
	require.NoError(t, err)
	return m
}

// nav walks a message's flatbuffer to the words a test patches.
type nav struct {
	t   testing.TB
	buf []byte // the flatbuffer, after the 8-byte framing
}

func (n nav) root() flatbuffers.Table {
	return flatbuffers.Table{Bytes: n.buf, Pos: flatbuffers.GetUOffsetT(n.buf)}
}

// union is the table in the union slot of t.
func (n nav) union(t flatbuffers.Table, slot flatbuffers.VOffsetT) flatbuffers.Table {
	o := t.Offset(slot)
	require.NotZero(n.t, o, "slot %d is set", slot)
	var out flatbuffers.Table
	t.Union(&out, flatbuffers.UOffsetT(o))
	return out
}

// target is where the offset stored in slot of t leads.
func (n nav) target(t flatbuffers.Table, slot flatbuffers.VOffsetT) int {
	o := t.Offset(slot)
	require.NotZero(n.t, o, "slot %d is set", slot)
	at := int(t.Pos) + int(o)
	return at + int(flatbuffers.GetUOffsetT(n.buf[at:]))
}

// slotAt is where slot of t is stored.
func (n nav) slotAt(t flatbuffers.Table, slot flatbuffers.VOffsetT) int {
	o := t.Offset(slot)
	require.NotZero(n.t, o, "slot %d is set", slot)
	return int(t.Pos) + int(o)
}

// element is the i-th table of the vector in slot of t.
func (n nav) element(t flatbuffers.Table, slot flatbuffers.VOffsetT, i int) flatbuffers.Table {
	ep := n.target(t, slot) + 4 + 4*i
	return flatbuffers.Table{Bytes: n.buf, Pos: flatbuffers.UOffsetT(ep + int(flatbuffers.GetUOffsetT(n.buf[ep:])))}
}

// schema and field are the tables the cases below reach into.
func (n nav) schema() flatbuffers.Table { return n.union(n.root(), slotMessageHeader) }

func put32(buf []byte, at int, v uint32) { binary.LittleEndian.PutUint32(buf[at:], v) }

// inflatedSchemas are the nested schema's message with one word changed to state
// more than the bytes hold: a count, a length, an offset.
func inflatedSchemas(t testing.TB) map[string][]byte {
	t.Helper()
	cases := map[string]func(n nav, buf []byte){
		"four billion fields":       func(n nav, buf []byte) { put32(buf, n.target(n.schema(), slotSchemaFields), 0xFFFFFFFF) },
		"2^30 fields":               func(n nav, buf []byte) { put32(buf, n.target(n.schema(), slotSchemaFields), 1<<30) },
		"more fields than bytes":    func(n nav, buf []byte) { put32(buf, n.target(n.schema(), slotSchemaFields), 1<<18) },
		"a billion schema metadata": func(n nav, buf []byte) { put32(buf, n.target(n.schema(), slotSchemaMetadata), 1<<30) },
		"a billion children": func(n nav, buf []byte) {
			put32(buf, n.target(n.element(n.schema(), slotSchemaFields, 1), slotFieldChildren), 1<<30)
		},
		"a billion field metadata": func(n nav, buf []byte) {
			put32(buf, n.target(n.element(n.schema(), slotSchemaFields, 1), slotFieldMetadata), 1<<30)
		},
		"a billion union type ids": func(n nav, buf []byte) {
			u := n.union(n.element(n.schema(), slotSchemaFields, 3), slotFieldType)
			put32(buf, n.target(u, slotUnionTypeIDs), 1<<30)
		},
		"a name longer than the message": func(n nav, buf []byte) {
			put32(buf, n.target(n.element(n.schema(), slotSchemaFields, 0), slotFieldName), 1<<30)
		},
		"a time zone longer than the message": func(n nav, buf []byte) {
			ts := n.union(n.element(n.schema(), slotSchemaFields, 2), slotFieldType)
			put32(buf, n.target(ts, slotTimestampZone), 1<<30)
		},
		"a metadata key longer than the message": func(n nav, buf []byte) {
			kv := n.element(n.schema(), slotSchemaMetadata, 0)
			put32(buf, n.target(kv, slotKeyValueKey), 1<<30)
		},
		"a fields vector that starts outside": func(n nav, buf []byte) {
			put32(buf, n.slotAt(n.schema(), slotSchemaFields), 1<<30)
		},
		"a field that is its own offset": func(n nav, buf []byte) {
			ep := n.target(n.schema(), slotSchemaFields) + 4
			put32(buf, ep, 0)
		},
		"a field that points backwards": func(n nav, buf []byte) {
			ep := n.target(n.schema(), slotSchemaFields) + 4
			put32(buf, ep, 0xFFFFFFF0)
		},
	}
	out := map[string][]byte{}
	for name, patch := range cases {
		m := schemaOf(t, nestedSchema())
		patch(nav{t: t, buf: m[8:]}, m[8:])
		out[name] = m
	}
	return out
}

func TestASchemaThatStatesMoreThanItsBytesHoldIsRefusedBeforeItIsDecoded(t *testing.T) {
	for name, m := range inflatedSchemas(t) {
		t.Run(name, func(t *testing.T) {
			var got error
			grew := allocated(func() { got = readAll(m, &msgs{}) })
			require.ErrorIs(t, got, ErrMalformed)
			require.Less(t, grew, uint64(boundedAlloc))
		})
	}
}

// A table that is referenced many times is walked, and paid for, at each
// reference: a message of 400 KB whose 50,000 fields each have 50,000 children,
// all of them one table, is refused.
func TestASchemaThatPointsManyReferencesAtOneTableIsRefused(t *testing.T) {
	const n = 50_000
	b := flatbuffers.NewBuilder(1 << 20)
	name := b.CreateString("x")
	b.StartObject(7)
	b.PrependUOffsetTSlot(0, name, 0)
	leaf := b.EndObject()

	vector := func(of flatbuffers.UOffsetT) flatbuffers.UOffsetT {
		b.StartVector(4, n, 4)
		for i := 0; i < n; i++ {
			b.PrependUOffsetT(of)
		}
		return b.EndVector(n)
	}
	children := vector(leaf)
	b.StartObject(7)
	b.PrependUOffsetTSlot(0, name, 0)
	b.PrependUOffsetTSlot(5, children, 0)
	mid := b.EndObject()
	fields := vector(mid)

	b.StartObject(4)
	b.PrependUOffsetTSlot(1, fields, 0)
	schema := b.EndObject()
	b.StartObject(5)
	b.PrependByteSlot(1, messageHeaderSchema, 0)
	b.PrependUOffsetTSlot(2, schema, 0)
	b.Finish(b.EndObject())
	flat := b.FinishedBytes()
	require.Less(t, len(flat), int(maxMetadataBytes), "it fits the metadata cap, so only the walk can refuse it")

	m := append([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0}, flat...)
	put32(m, 4, uint32(len(flat)))
	var got error
	grew := allocated(func() { got = readAll(m, &msgs{}) })
	require.ErrorIs(t, got, ErrMalformed)
	require.Less(t, grew, uint64(boundedAlloc))
}

// The walk is no stricter than arrow-go on a schema that is what it says it is.
func TestSchemasThatAreWhatTheySayPassTheWalk(t *testing.T) {
	wide := make([]arrow.Field, 4000)
	for i := range wide {
		wide[i] = arrow.Field{Name: fmt.Sprintf("column_%04d", i), Type: arrow.PrimitiveTypes.Int64, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"i"}, []string{fmt.Sprint(i)})}
	}
	for name, schema := range map[string]*arrow.Schema{
		"nested, with every vector the walk follows": nestedSchema(),
		"wide, with metadata on every field":         arrow.NewSchema(wide, nil),
		"empty":                                      arrow.NewSchema(nil, nil),
	} {
		t.Run(name, func(t *testing.T) {
			rd, err := NewReader(schemaOf(t, schema), &msgs{})
			require.NoError(t, err)
			defer rd.Release()
			require.True(t, rd.Schema().Equal(schema), "got %s want %s", rd.Schema(), schema)
			if schema.HasMetadata() {
				require.Equal(t, schema.Metadata().Keys(), rd.Schema().Metadata().Keys())
				require.Equal(t, schema.Metadata().Values(), rd.Schema().Metadata().Values())
			}
			for i, f := range schema.Fields() {
				require.Equal(t, f.Metadata.Keys(), rd.Schema().Field(i).Metadata.Keys())
			}
		})
	}
}

// A schema carried in the first message of a stream with no header is vetted
// like one in the header.
func TestAFirstMessageThatCarriesTheSchemaIsVettedToo(t *testing.T) {
	for name, m := range inflatedSchemas(t) {
		t.Run(name, func(t *testing.T) {
			var got error
			grew := allocated(func() { got = readAll(nil, &msgs{list: [][]byte{m}}) })
			require.ErrorIs(t, got, ErrMalformed)
			require.Less(t, grew, uint64(boundedAlloc))
		})
	}
}

// Whatever one byte of a schema message is made, it is read, or refused, with
// bounded memory.
func TestNoSingleByteMakesASchemaMessageCostMemory(t *testing.T) {
	m := schemaOf(t, nestedSchema())
	for at := 0; at < len(m); at++ {
		for _, v := range []byte{0x00, 0x01, 0x7F, 0x80, 0xFF} {
			if m[at] == v {
				continue
			}
			mutated := append([]byte(nil), m...)
			mutated[at] = v
			grew := allocated(func() { _ = readAll(mutated, &msgs{}) })
			require.Less(t, grew, uint64(boundedAlloc), "byte %d set to %#x", at, v)
		}
	}
}

// FuzzSchemaReader runs its seeds in the ordinary suite, and `go test -fuzz
// FuzzSchemaReader ./internal/arrowipc` explores from them. The body bound is
// small here: a message that states a body is allocated up to the bound before
// it is read, which is the bound's job and not what this looks for.
func FuzzSchemaReader(f *testing.F) {
	for _, s := range []*arrow.Schema{nestedSchema(), testSchema, arrow.NewSchema(nil, nil)} {
		m, err := SchemaMessage(s)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(m)
	}
	for _, m := range inflatedSchemas(f) {
		f.Add(m)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		grew := allocated(func() { _ = readAll(data, &msgs{}, WithMaxMessageBytes(1<<20)) })
		if grew > boundedAlloc {
			t.Fatalf("%d bytes of input allocated %d", len(data), grew)
		}
	})
}
