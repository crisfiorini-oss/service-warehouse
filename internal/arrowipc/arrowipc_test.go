package arrowipc

import (
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

type msgs struct {
	list [][]byte
	err  error
}

func (m *msgs) Next() ([]byte, error) {
	if len(m.list) == 0 {
		if m.err != nil {
			return nil, m.err
		}
		return nil, io.EOF
	}
	next := m.list[0]
	m.list = m.list[1:]
	return next, nil
}

var testSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

func batch(ids []int64, names []string) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, testSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	b.Field(1).(*array.StringBuilder).AppendValues(names, nil)
	return b.NewRecordBatch()
}

func TestSchemaThenBatchesIsAStream(t *testing.T) {
	enc, err := NewEncoder(testSchema)
	require.NoError(t, err)
	defer enc.Close()

	first, second := batch([]int64{1, 2}, []string{"a", "b"}), batch([]int64{3}, []string{"c"})
	defer first.Release()
	defer second.Release()
	m1, err := enc.Encode(first)
	require.NoError(t, err)
	m2, err := enc.Encode(second)
	require.NoError(t, err)

	header, err := SchemaMessage(testSchema)
	require.NoError(t, err)

	rd, err := NewReader(header, &msgs{list: [][]byte{m1, m2}})
	require.NoError(t, err)
	defer rd.Release()
	require.True(t, rd.Schema().Equal(testSchema))

	var ids []int64
	for rd.Next() {
		col := rd.RecordBatch().Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			ids = append(ids, col.Value(i))
		}
	}
	require.NoError(t, rd.Err())
	require.Equal(t, []int64{1, 2, 3}, ids)
}

// The first batch message must not carry the schema: it is in the header, and a
// reader that is handed it twice would see a second schema where a batch belongs.
func TestBatchMessagesDoNotCarryTheSchema(t *testing.T) {
	enc, err := NewEncoder(testSchema)
	require.NoError(t, err)
	defer enc.Close()
	rec := batch([]int64{1}, []string{"a"})
	defer rec.Release()
	m, err := enc.Encode(rec)
	require.NoError(t, err)

	// A batch message alone is not a stream: with no schema ahead of it, it
	// cannot be read.
	_, err = NewReader(nil, &msgs{list: [][]byte{m}})
	require.ErrorIs(t, err, ErrMalformed)
}

func TestSelfDescribingStream(t *testing.T) {
	enc, err := NewEncoder(testSchema)
	require.NoError(t, err)
	defer enc.Close()
	rec := batch([]int64{7}, []string{"x"})
	defer rec.Release()
	m, err := enc.Encode(rec)
	require.NoError(t, err)
	header, err := SchemaMessage(testSchema)
	require.NoError(t, err)

	// With no header schema, the first message carries it.
	rd, err := NewReader(nil, &msgs{list: [][]byte{append(append([]byte(nil), header...), m...)}})
	require.NoError(t, err)
	defer rd.Release()
	require.True(t, rd.Next())
	require.Equal(t, int64(7), rd.RecordBatch().Column(0).(*array.Int64).Value(0))
}

func TestNoBatchesIsAnEmptyStream(t *testing.T) {
	header, err := SchemaMessage(testSchema)
	require.NoError(t, err)
	rd, err := NewReader(header, &msgs{})
	require.NoError(t, err)
	defer rd.Release()
	require.False(t, rd.Next())
	require.NoError(t, rd.Err())
}

func TestSourceErrorSurfaces(t *testing.T) {
	boom := errors.New("transport failed")
	header, err := SchemaMessage(testSchema)
	require.NoError(t, err)
	rd, err := NewReader(header, &msgs{err: boom})
	if err == nil {
		defer rd.Release()
		for rd.Next() {
		}
		err = rd.Err()
	}
	require.ErrorIs(t, err, boom)
	require.NotErrorIs(t, err, ErrMalformed, "a source that failed is not data that is wrong")
}

func TestDictionaryEncodingIsRefused(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "c",
		Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String},
	}}, nil)
	_, err := SchemaMessage(schema)
	require.Error(t, err)
	_, err = NewEncoder(schema)
	require.Error(t, err)
}

// A header schema that is terminated by the end-of-stream marker is still the
// schema: read as a stream, the marker would end it before the first batch and
// the rows would vanish without an error.
func TestHeaderSchemaThatEndsTheStreamIsStillTheSchema(t *testing.T) {
	enc, err := NewEncoder(testSchema)
	require.NoError(t, err)
	defer enc.Close()
	rec := batch([]int64{1, 2}, []string{"a", "b"})
	defer rec.Release()
	m, err := enc.Encode(rec)
	require.NoError(t, err)
	header, err := SchemaMessage(testSchema)
	require.NoError(t, err)

	rd, err := NewReader(append(append([]byte(nil), header...), endOfStream[:]...), &msgs{list: [][]byte{m}})
	require.NoError(t, err)
	defer rd.Release()
	var rows int64
	for rd.Next() {
		rows += rd.RecordBatch().NumRows()
	}
	require.NoError(t, rd.Err())
	require.EqualValues(t, 2, rows)
}

func TestMalformedBytesAreErrMalformed(t *testing.T) {
	enc, err := NewEncoder(testSchema)
	require.NoError(t, err)
	defer enc.Close()
	rec := batch([]int64{1}, []string{"a"})
	defer rec.Release()
	m, err := enc.Encode(rec)
	require.NoError(t, err)
	header, err := SchemaMessage(testSchema)
	require.NoError(t, err)

	for name, tc := range map[string]struct {
		header []byte
		list   [][]byte
	}{
		"a header that is not a schema":         {[]byte{0xff, 0xff, 0xff, 0xff, 4, 0, 0, 0, 1, 2, 3, 4}, [][]byte{m}},
		"a batch cut short":                     {header, [][]byte{m[:len(m)/2]}},
		"a message that is not a message":       {header, [][]byte{{0xff, 0xff, 0xff, 0xff, 0x10, 0, 0, 0, 1, 2, 3}}},
		"a second schema where a batch belongs": {header, [][]byte{append(append([]byte(nil), header...), m...)}},
		"an empty message":                      {header, [][]byte{{}}},
		"no schema and no message":              {nil, nil},
	} {
		t.Run(name, func(t *testing.T) {
			rd, err := NewReader(tc.header, &msgs{list: tc.list})
			if err == nil {
				defer rd.Release()
				for rd.Next() {
				}
				err = rd.Err()
			}
			require.ErrorIs(t, err, ErrMalformed)
		})
	}
}

// A message that follows an empty one is not read past it: skipping would hide a
// client that sent a batch it never filled in.
func TestEmptyMessageAfterBatchesStopsTheStream(t *testing.T) {
	enc, err := NewEncoder(testSchema)
	require.NoError(t, err)
	defer enc.Close()
	rec := batch([]int64{1}, []string{"a"})
	defer rec.Release()
	m, err := enc.Encode(rec)
	require.NoError(t, err)
	header, err := SchemaMessage(testSchema)
	require.NoError(t, err)

	rd, err := NewReader(header, &msgs{list: [][]byte{m, {}, m}})
	require.NoError(t, err)
	defer rd.Release()
	var rows int64
	for rd.Next() {
		rows += rd.RecordBatch().NumRows()
	}
	require.EqualValues(t, 1, rows)
	require.ErrorIs(t, rd.Err(), ErrMalformed)
}
