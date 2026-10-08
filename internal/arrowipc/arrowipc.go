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

// ErrMalformed marks a failure to read the bytes as an Arrow IPC stream: not a
// schema where one belongs, a batch that does not match it, a message cut short,
// an empty message. It is the client's data that is wrong. A Source that fails to
// deliver the bytes at all is not malformed data, and its error comes back as
// itself, so a caller maps ErrMalformed to an invalid-argument and returns
// anything else unchanged.
var ErrMalformed = errors.New("malformed Arrow IPC stream")

// errEmptyMessage is what an empty message is: not a batch, and not skippable.
var errEmptyMessage = errors.New("an empty message is not an Arrow IPC message")

// Reader reads the records of an Arrow IPC stream that arrives as wire messages.
// It has the read side of an arrow-go ipc.Reader, with errors classified: see
// ErrMalformed.
type Reader struct {
	r   *ipc.Reader
	src *watchedSource
	err error
}

const (
	// DefaultMaxMessageBytes is the largest message body a reader accepts when
	// the caller names no limit: what SWH_MAX_ARROW_MESSAGE_BYTES defaults to.
	DefaultMaxMessageBytes int64 = 64 << 20

	// maxMetadataBytes is the largest message metadata a reader accepts. The
	// metadata is a schema or a batch's buffer layout, a few bytes per column; a
	// megabyte is thousands of columns.
	maxMetadataBytes int64 = 1 << 20
)

// Option configures a Reader.
type Option func(*readerConfig)

type readerConfig struct{ maxMessageBytes int64 }

// WithMaxMessageBytes bounds the body of one message. A message over the bound is
// refused as ErrMalformed before any of it is allocated; a value that is not
// positive leaves the default, never "no bound".
func WithMaxMessageBytes(n int64) Option {
	return func(c *readerConfig) {
		if n > 0 {
			c.maxMessageBytes = n
		}
	}
}

// NewReader reads the records of a stream whose schema message is schema and
// whose record-batch messages come from src. schema may be empty when src's
// first message is self-describing (a schema message followed by batches). A
// schema that arrives terminated by the end-of-stream marker is read as the
// schema it is: left on, the marker would end the stream before the first batch
// and every row would be silently dropped.
//
// The bytes come from a client, so the sizes a message states are not taken on
// trust. arrow-go's ipc.NewReader does not apply the size limits that its message
// reader has, so every reader is built here, from a message reader that does, and
// nowhere else (a test holds the repository to that).
//
// It reads the schema before it returns, so a first message that is not one is
// reported here.
func NewReader(schema []byte, src Source, opts ...Option) (*Reader, error) {
	cfg := readerConfig{maxMessageBytes: DefaultMaxMessageBytes}
	for _, opt := range opts {
		opt(&cfg)
	}
	w := &watchedSource{src: src}
	mem := memory.DefaultAllocator
	messages := ipc.NewMessageReader(
		&messageStream{pending: bytes.TrimSuffix(schema, endOfStream[:]), src: w},
		ipc.WithAllocator(mem),
		ipc.WithMetadataSizeLimit(maxMetadataBytes),
		ipc.WithBodySizeLimit(cfg.maxMessageBytes),
	)
	r, err := ipc.NewReaderFromMessageReader(messages, ipc.WithAllocator(mem))
	if err != nil {
		return nil, w.classify(err)
	}
	return &Reader{r: r, src: w}, nil
}

// Schema is the schema every record has.
func (r *Reader) Schema() *arrow.Schema { return r.r.Schema() }

// Next reads the next record, and reports false at the end of the stream or on
// an error; Err says which. The record is valid until the next call to Next.
func (r *Reader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.r.Next() {
		return true
	}
	if err := r.r.Err(); err != nil {
		r.err = r.src.classify(err)
	}
	return false
}

// RecordBatch is the record Next read.
func (r *Reader) RecordBatch() arrow.RecordBatch { return r.r.RecordBatch() }

// Err is nil at a clean end of stream.
func (r *Reader) Err() error { return r.err }

// Release releases the reader and its current record.
func (r *Reader) Release() { r.r.Release() }

// watchedSource remembers why its Source failed, so the failure is returned as
// itself and not as a complaint about Arrow.
type watchedSource struct {
	src Source
	err error
}

func (w *watchedSource) Next() ([]byte, error) {
	msg, err := w.src.Next()
	if err != nil && !errors.Is(err, io.EOF) && w.err == nil {
		w.err = err
	}
	return msg, err
}

// classify is the source's own error when the source failed, and ErrMalformed
// around err when the bytes it delivered are what failed to read.
func (w *watchedSource) classify(err error) error {
	if w.err != nil {
		return w.err
	}
	return fmt.Errorf("%w: %w", ErrMalformed, err)
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
		case len(next) == 0:
			return 0, errEmptyMessage
		default:
			m.pending = next
		}
	}
	n := copy(p, m.pending)
	m.pending = m.pending[n:]
	return n, nil
}
