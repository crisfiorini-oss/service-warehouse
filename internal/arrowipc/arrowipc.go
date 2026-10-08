// Package arrowipc is the one place that knows how Arrow travels on the
// Warehouse wire. A result header carries the schema as an Arrow IPC schema
// message and every following message carries one Arrow IPC record-batch
// message; both are the "encapsulated" form (continuation marker, metadata
// length, flatbuffer, body), so the schema message followed by the batch
// messages is a valid Arrow IPC stream and any Arrow reader consumes it.
//
// The package is backend-neutral: a backend that produces Arrow records uses
// Encoder, and a backend that ingests them uses NewReader, so no backend
// re-derives the framing.
package arrowipc

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// endOfStream is the Arrow IPC stream terminator: the continuation marker
// followed by a zero metadata length.
var endOfStream = [8]byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0}

// messageSink is an ipc.PayloadWriter that keeps each message as its own byte
// slice instead of flattening them into one stream.
type messageSink struct {
	messages [][]byte
}

func (s *messageSink) Start() error { return nil }
func (s *messageSink) Close() error { return nil }

func (s *messageSink) WritePayload(p ipc.Payload) error {
	var buf bytes.Buffer
	if _, err := p.WritePayload(&buf); err != nil {
		return err
	}
	s.messages = append(s.messages, buf.Bytes())
	return nil
}

func (s *messageSink) take() [][]byte {
	out := s.messages
	s.messages = nil
	return out
}

func rejectDictionaries(schema *arrow.Schema) error {
	for _, f := range schema.Fields() {
		if hasDictionary(f.Type) {
			return fmt.Errorf("arrowipc: dictionary-encoded field %q is not part of the wire contract", f.Name)
		}
	}
	return nil
}

func hasDictionary(t arrow.DataType) bool {
	switch v := t.(type) {
	case *arrow.DictionaryType:
		return true
	case *arrow.ListType:
		return hasDictionary(v.Elem())
	case *arrow.LargeListType:
		return hasDictionary(v.Elem())
	case *arrow.FixedSizeListType:
		return hasDictionary(v.Elem())
	case *arrow.StructType:
		for _, f := range v.Fields() {
			if hasDictionary(f.Type) {
				return true
			}
		}
	}
	return false
}

// SchemaMessage returns the Arrow IPC schema message for schema.
func SchemaMessage(schema *arrow.Schema) ([]byte, error) {
	if err := rejectDictionaries(schema); err != nil {
		return nil, err
	}
	sink := &messageSink{}
	w := ipc.NewWriterWithPayloadWriter(sink, ipc.WithSchema(schema), ipc.WithAllocator(memory.DefaultAllocator))
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("arrowipc: encode schema: %w", err)
	}
	msgs := sink.take()
	if len(msgs) != 1 {
		return nil, fmt.Errorf("arrowipc: expected one schema message, got %d", len(msgs))
	}
	return msgs[0], nil
}

// Encoder serializes record batches of one schema as record-batch messages.
type Encoder struct {
	sink *messageSink
	w    *ipc.Writer
	// schemaSeen is false until the writer has emitted the schema message that
	// precedes the first batch; that message belongs in the header, not here.
	schemaSeen bool
}

// NewEncoder returns an Encoder for batches of schema.
func NewEncoder(schema *arrow.Schema) (*Encoder, error) {
	if err := rejectDictionaries(schema); err != nil {
		return nil, err
	}
	sink := &messageSink{}
	return &Encoder{
		sink: sink,
		w:    ipc.NewWriterWithPayloadWriter(sink, ipc.WithSchema(schema), ipc.WithAllocator(memory.DefaultAllocator)),
	}, nil
}

// Encode returns the record-batch message for rec.
func (e *Encoder) Encode(rec arrow.RecordBatch) ([]byte, error) {
	if err := e.w.Write(rec); err != nil {
		return nil, fmt.Errorf("arrowipc: encode batch: %w", err)
	}
	msgs := e.sink.take()
	if !e.schemaSeen {
		// The writer emits the schema ahead of the first batch.
		if len(msgs) == 0 {
			return nil, errors.New("arrowipc: writer produced no messages")
		}
		msgs = msgs[1:]
		e.schemaSeen = true
	}
	if len(msgs) != 1 {
		return nil, fmt.Errorf("arrowipc: expected one batch message, got %d", len(msgs))
	}
	return msgs[0], nil
}

// Close releases the encoder.
func (e *Encoder) Close() error { return e.w.Close() }

// Source yields the messages that follow a schema, one per Next call, and
// io.EOF when there are no more. backend.BatchReader satisfies it.
type Source interface {
	Next() ([]byte, error)
}

// NewReader reads the records of a stream whose schema message is schema and
// whose record-batch messages come from src. schema may be empty when src's
// first message is self-describing (a schema message followed by batches).
func NewReader(schema []byte, src Source) (*ipc.Reader, error) {
	return ipc.NewReader(&messageStream{pending: schema, src: src}, ipc.WithAllocator(memory.DefaultAllocator))
}

// messageStream presents a header schema plus a Source of messages as one
// contiguous Arrow IPC stream, closed with the end-of-stream marker.
type messageStream struct {
	pending []byte
	src     Source
	done    bool
}

func (m *messageStream) Read(p []byte) (int, error) {
	for len(m.pending) == 0 {
		if m.done {
			return 0, io.EOF
		}
		next, err := m.src.Next()
		switch {
		case errors.Is(err, io.EOF):
			m.done = true
			m.pending = endOfStream[:]
		case err != nil:
			return 0, err
		default:
			m.pending = next
		}
	}
	n := copy(p, m.pending)
	m.pending = m.pending[n:]
	return n, nil
}
