// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

//nolint:govet
package querier

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/prometheus/prometheus/model/labels"
	pgmodelErrs "github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

//nolint
type mockPgxRows struct {
	closeCalled  bool
	firstRowRead bool
	idx          int
	results      []seriesSetRow
	err          error
}

//nolint
// Close closes the rows, making the connection ready for use again. It is safe
// to call Close after rows is already closed.
func (m *mockPgxRows) Close() {
	m.closeCalled = true
}

//nolint
// Err returns any error that occurred while reading.
func (m *mockPgxRows) Err() error {
	return nil
}

//nolint
// CommandTag returns the command tag from this query. It is only available after Rows is closed.
func (m *mockPgxRows) CommandTag() pgconn.CommandTag {
	panic("not implemented")
}

//nolint
func (m *mockPgxRows) FieldDescriptions() []pgproto3.FieldDescription {
	panic("not implemented")
}

//nolint
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

//nolint
// Scan reads the values from the current row into dest values positionally.
// dest can include pointers to core types, values implementing the Scanner
// interface, []byte, and nil. []byte will skip the decoding process and directly
// copy the raw bytes received from PostgreSQL. nil will skip the value entirely.
func (m *mockPgxRows) Scan(dest ...interface{}) error {
	if m.err != nil {
		return m.err
	}
	if len(dest) != 3 {
		return fmt.Errorf("incorrect number of destinations to scan in the results")
	}

	ln, ok := dest[0].(*[]int64)
	if !ok {
		panic("label names incorrect type, expected int64")
	}
	*ln = m.results[m.idx].labels
	ts, ok := dest[1].(*pgtype.TimestamptzArray)
	if !ok {
		panic("sample timestamps incorrect type")
	}
	ts.Elements = m.results[m.idx].timestamps
	//TODO dims?
	vs, ok := dest[2].(*pgtype.Float8Array)
	if !ok {
		return fmt.Errorf("sample values incorrect type")
	}
	vs.Elements = m.results[m.idx].values
	//TODO dims?

	return nil
}

//nolint
// Values returns the decoded row values.
func (m *mockPgxRows) Values() ([]interface{}, error) {
	panic("not implemented")
}

//nolint
// RawValues returns the unparsed bytes of the row values. The returned [][]byte is only valid until the next Next
// call or the Rows is closed. However, the underlying byte data is safe to retain a reference to and mutate.
func (m *mockPgxRows) RawValues() [][]byte {
	panic("not implemented")
}

//nolint
func generateArrayHeader(numDim, containsNull, elemOID, arrayLength uint32, addData []byte) []byte {
	result := make([]byte, 20)

	binary.BigEndian.PutUint32(result, numDim)
	binary.BigEndian.PutUint32(result[4:], containsNull)
	binary.BigEndian.PutUint32(result[8:], elemOID)
	binary.BigEndian.PutUint32(result[12:], arrayLength)
	binary.BigEndian.PutUint32(result[16:], 1) // filler for upper bound

	return append(result, addData...)
}

type seriesSetRow struct {
	labels     []int64
	timestamps []pgtype.Timestamptz
	values     []pgtype.Float8
	schema     string
	column     string
}

var arbitraryErr = fmt.Errorf("arbitrary err")

func TestPgxSeriesSet(t *testing.T) {
	testCases := []struct {
		name         string
		input        [][]seriesSetRow
		labels       []int64
		ts           []pgtype.Timestamptz
		vs           []pgtype.Float8
		metricSchema string
		columnName   string
		rowCount     int
		err          error
		rowErr       error
	}{
		{
			name:     "invalid row",
			rowErr:   arbitraryErr,
			input:    [][]seriesSetRow{{seriesSetRow{}}},
			err:      arbitraryErr,
			rowCount: 1,
		},
		{
			name:  "empty rows",
			input: [][]seriesSetRow{},
		},
		{
			name: "timestamp/value count mismatch",
			input: [][]seriesSetRow{{
				genSeries(
					[]int64{1},
					[]pgtype.Timestamptz{},
					[]pgtype.Float8{{Float: 1.0}},
					"",
					""),
			}},
			rowCount: 1,
			err:      pgmodelErrs.ErrInvalidRowData,
		},
		{
			name:     "happy path 1",
			labels:   []int64{1},
			ts:       []pgtype.Timestamptz{{Time: time.Now()}},
			vs:       []pgtype.Float8{{Float: 1}},
			rowCount: 1,
		},
		{
			name:   "happy path 2",
			labels: []int64{2, 3},
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
			labels: []int64{2, 3},
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
			labels: []int64{2, 3},
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
			labels: []int64{2, 3},
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
		{
			name:   "check default metric schema",
			labels: []int64{2, 3},
			ts: []pgtype.Timestamptz{
				{Time: time.Unix(0, 500000)},
				{Time: time.Unix(0, 6000000)},
			},
			vs: []pgtype.Float8{
				{Float: 30000},
				{Float: 100},
			},
			metricSchema: schema.PromData,
			rowCount:     1,
		},
		{
			name:   "check custom metric schema",
			labels: []int64{2, 3},
			ts: []pgtype.Timestamptz{
				{Time: time.Unix(0, 500000)},
				{Time: time.Unix(0, 6000000)},
			},
			vs: []pgtype.Float8{
				{Float: 30000},
				{Float: 100},
			},
			metricSchema: "customSchema",
			rowCount:     1,
		},
		{
			name:   "check custom column name",
			labels: []int64{2, 3},
			ts: []pgtype.Timestamptz{
				{Time: time.Unix(0, 500000)},
				{Time: time.Unix(0, 6000000)},
			},
			vs: []pgtype.Float8{
				{Float: 30000},
				{Float: 100},
			},
			columnName: "max",
			rowCount:   1,
		},
	}

	labelMapping := make(map[int64]struct {
		k string
		v string
	})
	for i := int64(0); i < 4; i++ {
		labelMapping[i] = struct {
			k string
			v string
		}{
			k: fmt.Sprintf("k%d", i),
			v: fmt.Sprintf("v%d", i),
		}
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			if c.input == nil {
				if c.columnName == "" {
					c.columnName = defaultColumnName
				}
				labels := make([]int64, len(c.labels))
				copy(labels, c.labels)
				c.input = [][]seriesSetRow{{
					genSeries(labels, c.ts, c.vs, c.metricSchema, c.columnName)}}
			}
			p := buildSeriesSet(genPgxRows(c.input, c.rowErr), mapQuerier{labelMapping})
			if p.Err() != nil {
				t.Fatal(p.Err())
			}

			for c.rowCount > 0 {
				c.rowCount--
				if !p.Next() {
					t.Fatal("unexpected end of series set")
				}

				s := p.At()

				if err := p.Err(); !errors.Is(err, c.err) {
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

				expectedLabels := make([]labels.Label, 0, len(c.labels))
				for _, v := range c.labels {
					expectedLabels = append(expectedLabels, labels.Label{Name: labelMapping[v].k, Value: labelMapping[v].v})
				}

				if c.metricSchema != "" && c.metricSchema != schema.PromData {
					expectedLabels = append(expectedLabels, labels.Label{Name: model.SchemaNameLabelName, Value: c.metricSchema})
				}
				if c.columnName != "" && c.columnName != defaultColumnName {
					expectedLabels = append(expectedLabels, labels.Label{Name: model.ColumnNameLabelName, Value: c.columnName})
				}

				expectedMap := labels.Labels(expectedLabels).Map()
				if !reflect.DeepEqual(ss.Labels().Map(), expectedMap) {
					t.Fatalf("unexpected labels values: got %+v, wanted %+v\n", ss.Labels().Map(), expectedMap)
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

			if !errors.Is(p.Err(), c.err) {
				t.Fatalf("unexpected err: got %s, wanted %s", p.Err(), c.err)
			}
		})
	}
}

type mapQuerier struct {
	mapping map[int64]struct {
		k string
		v string
	}
}

func (m mapQuerier) LabelsForIdMap(idMap map[int64]labels.Label) (err error) {
	for id := range idMap {
		kv, ok := m.mapping[id]
		if !ok {
			return pgmodelErrs.ErrInvalidRowData
		}
		idMap[id] = labels.Label{Name: kv.k, Value: kv.v}
	}
	return nil
}

//nolint
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

func genPgxRows(m [][]seriesSetRow, err error) []sampleRow {
	var result []sampleRow

	for _, mm := range m {
		for _, r := range mm {
			result = append(result, sampleRow{
				labelIds: r.labels,
				times:    newRowTimestampSeries(toTimestampTzArray(r.timestamps)),
				values:   toFloat8Array(r.values),
				schema:   r.schema,
				column:   r.column,
				err:      err,
			})
		}
	}

	return result
}

func toTimestampTzArray(times []pgtype.Timestamptz) *pgtype.TimestamptzArray {
	return &pgtype.TimestamptzArray{
		Elements:   times,
		Dimensions: nil,
		Status:     pgtype.Present,
	}
}

func toFloat8Array(values []pgtype.Float8) *pgtype.Float8Array {
	return &pgtype.Float8Array{
		Elements:   values,
		Dimensions: nil,
		Status:     pgtype.Present,
	}
}

func genSeries(labels []int64, ts []pgtype.Timestamptz, vs []pgtype.Float8, schema, column string) seriesSetRow {

	for i := range ts {
		if ts[i].Status == pgtype.Undefined {
			ts[i].Status = pgtype.Present
		}
	}

	for i := range vs {
		if vs[i].Status == pgtype.Undefined {
			vs[i].Status = pgtype.Present
		}
	}

	return seriesSetRow{
		labels:     labels,
		timestamps: ts,
		values:     vs,
		schema:     schema,
		column:     column,
	}
}
