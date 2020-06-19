package pgmodel

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

type mockPgxRows struct {
	closeCalled  bool
	firstRowRead bool
	idx          int
	results      [][][]byte
}

// Close closes the rows, making the connection ready for use again. It is safe
// to call Close after rows is already closed.
func (m *mockPgxRows) Close() {
	m.closeCalled = true
}

// Err returns any error that occurred while reading.
func (m *mockPgxRows) Err() error {
	panic("not implemented")
}

// CommandTag returns the command tag from this query. It is only available after Rows is closed.
func (m *mockPgxRows) CommandTag() pgconn.CommandTag {
	panic("not implemented")
}

func (m *mockPgxRows) FieldDescriptions() []pgproto3.FieldDescription {
	panic("not implemented")
}

// Next prepares the next row for reading. It returns true if there is another
// row and false if no more rows are available. It automatically closes rows
// when all rows are read.
func (m *mockPgxRows) Next() bool {
	if m.firstRowRead {
		m.idx++
	}
	m.firstRowRead = true

	return m.idx < len(m.results)
}

// Scan reads the values from the current row into dest values positionally.
// dest can include pointers to core types, values implementing the Scanner
// interface, []byte, and nil. []byte will skip the decoding process and directly
// copy the raw bytes received from PostgreSQL. nil will skip the value entirely.
func (m *mockPgxRows) Scan(dest ...interface{}) error {
	if len(dest) != 4 {
		return fmt.Errorf("incorrect number of destinations to scan in the results")
	}
	if len(m.results[m.idx]) != 4 {
		return fmt.Errorf("incorrect number of results being scanned in")
	}

	if ln, ok := dest[0].(*pgtype.TextArray); !ok {
		return fmt.Errorf("label names incorrect type")
	} else {
		if err := ln.DecodeBinary(nil, m.results[m.idx][0]); err != nil {
			return err
		}
	}
	if lv, ok := dest[1].(*pgtype.TextArray); !ok {
		return fmt.Errorf("label values incorrect type")
	} else {
		if err := lv.DecodeBinary(nil, m.results[m.idx][1]); err != nil {
			return err
		}
	}
	if ts, ok := dest[2].(*pgtype.TimestamptzArray); !ok {
		return fmt.Errorf("sample timestamps incorrect type")
	} else {
		if err := ts.DecodeBinary(nil, m.results[m.idx][2]); err != nil {
			return err
		}
	}
	if vs, ok := dest[3].(*pgtype.Float8Array); !ok {
		return fmt.Errorf("sample values incorrect type")
	} else {
		if err := vs.DecodeBinary(nil, m.results[m.idx][3]); err != nil {
			return err
		}
	}

	return nil
}

// Values returns the decoded row values.
func (m *mockPgxRows) Values() ([]interface{}, error) {
	panic("not implemented")
}

// RawValues returns the unparsed bytes of the row values. The returned [][]byte is only valid until the next Next
// call or the Rows is closed. However, the underlying byte data is safe to retain a reference to and mutate.
func (m *mockPgxRows) RawValues() [][]byte {
	return m.results[m.idx]
}

func generateArrayHeader(numDim, containsNull, elemOID, arrayLength uint32, addData []byte) []byte {
	result := make([]byte, 20)

	binary.BigEndian.PutUint32(result, numDim)
	binary.BigEndian.PutUint32(result[4:], containsNull)
	binary.BigEndian.PutUint32(result[8:], elemOID)
	binary.BigEndian.PutUint32(result[12:], arrayLength)
	binary.BigEndian.PutUint32(result[16:], 1) // filler for upper bound

	return append(result, addData...)
}

func TestPgxSeriesSet(t *testing.T) {
	testCases := []struct {
		name     string
		input    [][][][]byte
		labels   map[string]string
		ts       []pgtype.Timestamptz
		vs       []pgtype.Float8
		rowCount int
		err      error
	}{
		{
			name:  "empty rows",
			input: make([][][][]byte, 0),
		},
		{
			name:     "invalid row",
			input:    append([][][][]byte{}, genRows(1)),
			rowCount: 1,
			err:      errInvalidData,
		},
		{
			name:     "invalid rows",
			input:    append([][][][]byte{}, genRows(4)),
			rowCount: 4,
			err:      errInvalidData,
		},
		{
			name: "invalid row field count",
			input: append([][][][]byte{},
				append([][][]byte{},
					genSeries([]string{"one"}, nil, nil, nil)[:3],
				),
			),
			rowCount: 1,
			err:      errInvalidData,
		},
		{
			name: "invalid label names",
			input: append([][][][]byte{},
				append([][][]byte{},
					append([][]byte{[]byte{}},
						genSeries([]string{"one"}, nil, nil, nil)[1:4]...,
					),
				),
			),
			rowCount: 1,
			err:      errInvalidData,
		},
		{
			name: "invalid label values",
			input: append([][][][]byte{},
				append([][][]byte{},
					append([][]byte{},
						genSeries([]string{"one"}, nil, nil, nil)[0],
						[]byte{},
						genSeries([]string{"one"}, nil, nil, nil)[2],
						genSeries([]string{"one"}, nil, nil, nil)[3],
					),
				),
			),
			rowCount: 1,
			err:      errInvalidData,
		},
		{
			name: "invalid times",
			input: append([][][][]byte{},
				append([][][]byte{},
					append([][]byte{},
						genSeries([]string{"one"}, nil, nil, nil)[0],
						genSeries([]string{"one"}, []string{"one"}, nil, nil)[1],
						[]byte{},
						genSeries([]string{"one"}, nil, nil, nil)[3],
					),
				),
			),
			rowCount: 1,
			err:      errInvalidData,
		},
		{
			name: "invalid values",
			input: append([][][][]byte{},
				append([][][]byte{},
					append([][]byte{},
						genSeries([]string{"one"}, nil, nil, nil)[0],
						genSeries([]string{"one"}, []string{"one"}, nil, nil)[1],
						genSeries([]string{"one"}, nil, nil, nil)[2],
						[]byte{},
					),
				),
			),
			rowCount: 1,
			err:      errInvalidData,
		},
		{
			name: "label name/value count mismatch",
			input: append([][][][]byte{},
				append([][][]byte{},
					genSeries([]string{"one"}, nil, nil, nil),
				),
			),
			rowCount: 1,
			err:      errInvalidData,
		},
		{
			name: "timestamp/value count mismatch",
			input: append([][][][]byte{},
				append([][][]byte{},
					genSeries(
						[]string{"one"},
						[]string{"one"},
						[]pgtype.Timestamptz{},
						[]pgtype.Float8{{Float: 1.0}}),
				),
			),
			rowCount: 1,
			err:      errInvalidData,
		},
		{
			name:     "happy path 1",
			labels:   map[string]string{"one": "one"},
			ts:       []pgtype.Timestamptz{{Time: time.Now()}},
			vs:       []pgtype.Float8{{Float: 1}},
			rowCount: 1,
		},
		{
			name:   "happy path 2",
			labels: map[string]string{"foo": "bar", "name": "value"},
			ts: []pgtype.Timestamptz{
				{Time: time.Unix(0, 500000)},
				{Time: time.Unix(0, 6000000)},
			},
			vs: []pgtype.Float8{
				{Float: 30000},
				{Float: 40000},
			},
			rowCount: 1,
		},
		{
			name:   "check nulls (ts and vs negative values are encoded as null)",
			labels: map[string]string{"foo": "bar", "name": "value"},
			ts: []pgtype.Timestamptz{
				{Status: pgtype.Null},
				{Time: time.Unix(0, 0)},
				{Time: time.Unix(0, 6000000)},
			},
			vs: []pgtype.Float8{
				{Float: 30000},
				{Float: 40000},
				{Status: pgtype.Null},
			},
			rowCount: 1,
		},
		{
			name:   "check all nulls",
			labels: map[string]string{"foo": "bar", "name": "value"},
			ts: []pgtype.Timestamptz{
				{Status: pgtype.Null},
				{Time: time.Unix(0, 0)},
				{Time: time.Unix(0, 6000000)},
			},
			vs: []pgtype.Float8{
				{Float: 30000},
				{Status: pgtype.Null},
				{Status: pgtype.Null},
			},
			rowCount: 1,
		},
		{
			name:   "check infinity",
			labels: map[string]string{"foo": "bar", "name": "value"},
			ts: []pgtype.Timestamptz{
				{InfinityModifier: pgtype.NegativeInfinity},
				{InfinityModifier: pgtype.Infinity},
			},
			vs: []pgtype.Float8{
				{Float: 30000},
				{Float: 100},
			},
			rowCount: 1,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			if c.input == nil {
				labelNames := make([]string, 0, len(c.labels))
				labelValues := make([]string, 0, len(c.labels))
				for k, v := range c.labels {
					labelNames = append(labelNames, k)
					labelValues = append(labelValues, v)
				}
				c.input = append([][][][]byte{},
					append([][][]byte{},
						genSeries(labelNames, labelValues, c.ts, c.vs),
					),
				)
			}
			p := pgxSeriesSet{rows: genPgxRows(c.input)}

			for c.rowCount > 0 {
				c.rowCount--
				if !p.Next() {
					t.Fatal("unexpected end of series set")
				}

				s := p.At()

				if err := p.Err(); err != c.err {
					t.Fatalf("unexpected error returned: got %s, wanted %s", err, c.err)
				}

				if p.Err() != nil {
					continue
				}

				var ss *pgxSeries
				var ok bool

				if ss, ok = s.(*pgxSeries); !ok {
					t.Fatal("unexpected type for storage.Series")
				}

				if !reflect.DeepEqual(ss.Labels().Map(), c.labels) {
					t.Fatalf("unexpected labels values: got %+v, wanted %+v\n", ss.Labels().Map(), c.labels)
				}

				iter := ss.Iterator()
				var (
					i      int
					ts     pgtype.Timestamptz
					lastTs int64   = -1
					lastVs float64 = -1
				)

				for i, ts = range c.ts {
					// Skipping 0/NULL values for ts and vs.
					if ts.Status == pgtype.Null || c.vs[i].Status == pgtype.Null {
						continue
					}
					if !iter.Next() {
						t.Fatal("unexpected end of series iterator")
					}
					gotTs, gotVs := iter.At()
					wanted := ts.Time.UnixNano() / 1e6

					if ts.InfinityModifier == pgtype.NegativeInfinity {
						wanted = math.MinInt64
					}
					if ts.InfinityModifier == pgtype.Infinity {
						wanted = math.MaxInt64
					}

					if gotTs != wanted {
						t.Errorf("unexpected time value: got %d, wanted %d", gotTs, wanted)
					}

					if gotVs != c.vs[i].Float {
						t.Errorf("unexpected value: got %f, wanted %f", gotVs, c.vs[i].Float)
					}

					lastTs = gotTs
					lastVs = gotVs
				}

				// At this point, iterator should be exhausted but if we seek to the last time, it will reset.
				// Unless there are no items to iterate on.
				if lastTs < 0 {
					continue
				}

				if !iter.Seek(lastTs) {
					t.Fatalf("unexpected seek result, item should have been found: seeking %d", lastTs)
				}
				gotTs, gotVs := iter.At()

				if gotTs != lastTs {
					t.Errorf("unexpected time value: got %d, wanted %d", gotTs, lastTs)
				}
				if gotVs != lastVs {
					t.Errorf("unexpected value: got %f, wanted %f", gotVs, lastVs)
				}

				if iter.Next() {
					t.Fatal("unexpected presence of next value after end")
				}
				if zeroTs, zeroVal := iter.At(); zeroTs != 0 || zeroVal != 0 {
					t.Fatal("unexpected presence of values after end")
				}

				if iter.Err() != nil {
					t.Fatal("unexpected error from iterator")
				}

				// Seek a timestamp more current than existing ones.
				// That's only possible if the last value is not MaxInt64.
				if lastTs != math.MaxInt64 && iter.Seek(lastTs+1000) {
					t.Fatalf("Found a sample that should not exist in the iterator")
				}

			}

			if p.Next() {
				t.Fatal("unexpected presence of next row after all rows were iterated on")
			}

			if p.At() != nil {
				t.Fatal("unexpected at value after all rows were iterated on")
			}

			if c.err != p.Err() {
				t.Fatalf("unexpected err: got %s, wanted %s", p.Err(), c.err)
			}
		})
	}
}

func genRows(count int) [][][]byte {
	result := make([][][]byte, count)

	for i := range result {
		result[i] = make([][]byte, 4)

		for j := range result[i] {
			result[i][j] = []byte(fmt.Sprintf("payload %d %d", i, j))
		}
	}

	return result
}

func genPgxRows(m [][][][]byte) []pgx.Rows {
	result := make([]pgx.Rows, len(m))

	for i := range result {
		result[i] = &mockPgxRows{
			results: m[i],
		}
	}

	return result
}

func genSeries(labelNames []string, labelValues []string, ts []pgtype.Timestamptz, vs []pgtype.Float8) [][]byte {
	result := make([][]byte, 4)
	keys := make([]byte, 0, 100)
	values := make([]byte, 0, 100)
	nulls := make([]uint32, 4)
	if len(labelNames) == 0 {
		nulls[0] = 1
	}
	if len(labelValues) == 0 {
		nulls[1] = 1
	}
	if len(ts) == 0 {
		nulls[2] = 1
	}
	if len(vs) == 0 {
		nulls[3] = 1
	}
	var err error
	kIdx, vIdx := 0, 0
	for _, v := range labelNames {
		keyLen := len(v)
		keys = append(keys, []byte("1234")...)
		binary.BigEndian.PutUint32(keys[kIdx:], uint32(keyLen))
		kIdx += 4
		keys = append(keys, v...)
		kIdx += len(v)
	}
	for _, v := range labelValues {
		valLen := len(v)
		values = append(values, []byte("1234")...)
		binary.BigEndian.PutUint32(values[vIdx:], uint32(valLen))
		vIdx += 4
		values = append(values, v...)
		vIdx += len(v)
	}

	tts := make([]byte, 0, len(ts)*12)
	vvs := make([]byte, 0, len(vs)*12)
	tIdx, vIdx := 0, 0
	for _, t := range ts {
		tIdx = len(tts)
		if t.Status == pgtype.Null {
			tts = append(tts, make([]byte, 4)...)
			binary.BigEndian.PutUint32(tts[tIdx:], math.MaxUint32)
			nulls[2] = 1
			continue
		}
		nulls[2] = 1
		t.Status = pgtype.Present
		tts = append(tts, make([]byte, 4)...)
		binary.BigEndian.PutUint32(tts[tIdx:], 8)
		tts, err = t.EncodeBinary(nil, tts)
		if err != nil {
			panic(err)
		}
	}
	for _, v := range vs {
		vIdx = len(vvs)
		if v.Status == pgtype.Null {
			vvs = append(vvs, make([]byte, 4)...)
			binary.BigEndian.PutUint32(vvs[vIdx:], math.MaxUint32)
			nulls[3] = 1
			continue
		}
		v.Status = pgtype.Present
		vvs = append(vvs, make([]byte, 4)...)
		binary.BigEndian.PutUint32(vvs[vIdx:], 8)
		vvs, err = v.EncodeBinary(nil, vvs)
		if err != nil {
			panic(err)
		}
	}
	result[0] = generateArrayHeader(1, nulls[0], 0, uint32(len(labelNames)), keys)
	result[1] = generateArrayHeader(1, nulls[1], 0, uint32(len(labelValues)), values)
	result[2] = generateArrayHeader(1, nulls[2], 0, uint32(len(ts)), tts)
	result[3] = generateArrayHeader(1, nulls[3], 0, uint32(len(vs)), vvs)
	return result
}
