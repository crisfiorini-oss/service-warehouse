package arrowipc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	flatbuffers "github.com/google/flatbuffers/go"
)

// STOPGAP: a defensive check on a schema message, before it is decoded. It is not
// part of the wire contract, and it can be removed if the decoder validates its own
// input.
//
// A schema message states counts (fields, children, metadata entries, type ids),
// lengths and offsets, and the decoder sizes what it builds from them. They are
// checked here against the message's own bytes first. Before the decoder sees the
// first message of a stream, and only when it is a schema, this walks its
// flatbuffer with the public flatbuffers table API and refuses it, as ErrMalformed,
// when
//
//   - a vector states more elements than the bytes after it can hold, at the
//     smallest element size there is (4 bytes: an offset or an int32);
//   - an offset points outside the message, or to or before itself, which also
//     rules out a cycle;
//   - or the walk would visit more structure than the message has bytes: every
//     table costs 4, every vector 4 plus 4 for each element, every string 4 plus
//     its length, and a table that is referenced more than once is paid for at
//     each reference. A message that is a tree costs no more than its own size,
//     so a producer that writes an ordinary schema never reaches this.
//
// The walk covers what the decoder reads from a schema (fields, their children,
// every custom metadata vector, a union's type ids, a timestamp's time zone), so
// what it admits is bounded by the metadata cap, 1 MiB, and so is what the
// decoder builds from it. The slot numbers are those of Schema.fbs in the Arrow
// format and do not change: a field is only ever added at the end of a table.

// vtable offsets of the fields read below: 4 plus 2 a slot.
const (
	slotMessageHeaderType = 6
	slotMessageHeader     = 8

	slotSchemaFields   = 6
	slotSchemaMetadata = 8

	slotFieldName     = 4
	slotFieldTypeType = 8
	slotFieldType     = 10
	slotFieldChildren = 14
	slotFieldMetadata = 16

	slotKeyValueKey   = 4
	slotKeyValueValue = 6

	slotUnionTypeIDs = 6

	slotTimestampZone = 6

	messageHeaderSchema = 1 // MessageHeader.Schema
	typeTimestamp       = 10
	typeUnion           = 14
)

// shape is a walk of one flatbuffer.
type shape struct {
	buf    []byte
	budget uint64 // bytes of structure the walk may still visit
}

type shapeFault string

func (s *shape) fail(format string, args ...any) { panic(shapeFault(fmt.Sprintf(format, args...))) }

// spend pays for structure the walk visits.
func (s *shape) spend(n uint64) {
	if n > s.budget {
		s.fail("it states more structure than it has bytes")
	}
	s.budget -= n
}

// need checks that n bytes at pos are inside the message. pos and n are below
// 1<<33, so the sum cannot wrap.
func (s *shape) need(pos, n uint64) {
	if pos+n > uint64(len(s.buf)) {
		s.fail("an offset or a length points outside the message")
	}
}

// indirect follows the offset stored at pos, which must lead strictly forward to
// somewhere with at least a word to read.
func (s *shape) indirect(pos uint64) uint64 {
	s.need(pos, 4)
	delta := uint64(flatbuffers.GetUOffsetT(s.buf[pos:]))
	if delta == 0 {
		s.fail("an offset points to itself")
	}
	dest := pos + delta
	s.need(dest, 4)
	return dest
}

// table is the table at pos. Its vtable has to be inside the message too, which
// is all Table.Offset needs to read safely.
func (s *shape) table(pos uint64) flatbuffers.Table {
	s.spend(4)
	s.need(pos, 4)
	t := flatbuffers.Table{Bytes: s.buf, Pos: flatbuffers.UOffsetT(pos)}
	vtable := int64(pos) - int64(t.GetSOffsetT(t.Pos))
	if vtable < 0 {
		s.fail("a table's vtable is before the message")
	}
	s.need(uint64(vtable), 4)
	size := uint64(t.GetVOffsetT(flatbuffers.UOffsetT(vtable)))
	if size < 4 {
		s.fail("a vtable is smaller than its own header")
	}
	s.need(uint64(vtable), size)
	return t
}

// field is where the vtable slot of t is stored in t, and whether it is set.
func (s *shape) field(t flatbuffers.Table, slot flatbuffers.VOffsetT) (uint64, bool) {
	o := t.Offset(slot)
	if o == 0 {
		return 0, false
	}
	return uint64(t.Pos) + uint64(o), true
}

func (s *shape) byteField(t flatbuffers.Table, slot flatbuffers.VOffsetT) byte {
	pos, ok := s.field(t, slot)
	if !ok {
		return 0
	}
	s.need(pos, 1)
	return s.buf[pos]
}

func (s *shape) tableField(t flatbuffers.Table, slot flatbuffers.VOffsetT) (flatbuffers.Table, bool) {
	pos, ok := s.field(t, slot)
	if !ok {
		return flatbuffers.Table{}, false
	}
	return s.table(s.indirect(pos)), true
}

// vector is the position of the first element of the vector in a slot of t and
// how many it states, after checking that the bytes hold that many.
func (s *shape) vector(t flatbuffers.Table, slot flatbuffers.VOffsetT) (first, n uint64) {
	pos, ok := s.field(t, slot)
	if !ok {
		return 0, 0
	}
	at := s.indirect(pos)
	n = uint64(flatbuffers.GetUOffsetT(s.buf[at:]))
	first = at + 4
	s.need(first, 4*n)
	s.spend(4 + 4*n)
	return first, n
}

func (s *shape) str(t flatbuffers.Table, slot flatbuffers.VOffsetT) {
	pos, ok := s.field(t, slot)
	if !ok {
		return
	}
	at := s.indirect(pos)
	n := uint64(flatbuffers.GetUOffsetT(s.buf[at:]))
	s.need(at+4, n)
	s.spend(4 + n)
}

func (s *shape) walkSchema(t flatbuffers.Table) {
	first, n := s.vector(t, slotSchemaFields)
	for i := uint64(0); i < n; i++ {
		s.walkField(s.table(s.indirect(first + 4*i)))
	}
	s.walkKeyValues(t, slotSchemaMetadata)
}

func (s *shape) walkField(t flatbuffers.Table) {
	s.str(t, slotFieldName)
	switch s.byteField(t, slotFieldTypeType) {
	case typeUnion:
		if u, ok := s.tableField(t, slotFieldType); ok {
			s.vector(u, slotUnionTypeIDs)
		}
	case typeTimestamp:
		if ts, ok := s.tableField(t, slotFieldType); ok {
			s.str(ts, slotTimestampZone)
		}
	}
	first, n := s.vector(t, slotFieldChildren)
	for i := uint64(0); i < n; i++ {
		s.walkField(s.table(s.indirect(first + 4*i)))
	}
	s.walkKeyValues(t, slotFieldMetadata)
}

func (s *shape) walkKeyValues(t flatbuffers.Table, slot flatbuffers.VOffsetT) {
	first, n := s.vector(t, slot)
	for i := uint64(0); i < n; i++ {
		kv := s.table(s.indirect(first + 4*i))
		s.str(kv, slotKeyValueKey)
		s.str(kv, slotKeyValueValue)
	}
}

// checkSchemaMetadata refuses a message's metadata, which is its flatbuffer, if it
// is a schema that states more than its bytes can hold; see the note at the top
// of the file. Anything that is not a schema is not its concern: arrow-go refuses
// it, or reads a record batch, whose counts it does not size slices from.
func checkSchemaMetadata(buf []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// A fault of ours, or an out-of-range read of the table API on bytes
			// that were checked less closely than the ones it reads: either way
			// the bytes are not a schema arrow-go can be trusted with.
			err = fmt.Errorf("%w: schema metadata: %v", ErrMalformed, r)
		}
	}()
	s := &shape{buf: buf, budget: uint64(len(buf))}
	s.need(0, 4)
	msg := s.table(uint64(flatbuffers.GetUOffsetT(buf)))
	if s.byteField(msg, slotMessageHeaderType) != messageHeaderSchema {
		return nil
	}
	if schema, ok := s.tableField(msg, slotMessageHeader); ok {
		s.walkSchema(schema)
	}
	return nil
}

// vetFirstMessage reads the first message off stream, framing and metadata, and
// refuses it with checkSchemaMetadata when it is a schema. It returns a reader
// that replays what it read ahead of the rest of the stream, so arrow-go reads
// the same bytes. A message whose framing is not one arrow-go would accept is
// replayed unjudged: arrow-go refuses it itself, by its own limits.
func vetFirstMessage(stream io.Reader) (io.Reader, error) {
	var prefix [8]byte
	if _, err := io.ReadFull(stream, prefix[:4]); err != nil {
		return nil, err
	}
	word := binary.LittleEndian.Uint32(prefix[:4])
	prefixLen, length := 4, int64(int32(word)) // a stream from before the continuation marker
	if word == 0xFFFFFFFF {
		if _, err := io.ReadFull(stream, prefix[4:]); err != nil {
			return nil, err
		}
		prefixLen, length = 8, int64(int32(binary.LittleEndian.Uint32(prefix[4:])))
	}
	replay := bytes.NewReader(prefix[:prefixLen])
	if length < 4 || length > maxMetadataBytes {
		return io.MultiReader(replay, stream), nil
	}
	metadata := make([]byte, length)
	if _, err := io.ReadFull(stream, metadata); err != nil {
		return nil, err
	}
	if err := checkSchemaMetadata(metadata); err != nil {
		return nil, err
	}
	return io.MultiReader(replay, bytes.NewReader(metadata), stream), nil
}
