package pgmodel

import (
	"fmt"
	"math"
	"sort"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const (
	// Postgres time zero is Sat Jan 01 00:00:00 2000 UTC.
	// This is the offset of the Unix epoch in milliseconds from the Postgres zero.
	PostgresUnixEpoch = -946684800000
)

var (
	errInvalidData = fmt.Errorf("invalid row data")
)

// pgxSeriesSet implements storage.SeriesSet.
type pgxSeriesSet struct {
	rowIdx int
	rows   []pgx.Rows
	err    error
}

// Next forwards the internal cursor to next storage.Series
func (p *pgxSeriesSet) Next() bool {
	if p.rowIdx >= len(p.rows) {
		return false
	}
	for !p.rows[p.rowIdx].Next() {
		p.rows[p.rowIdx].Close()
		p.rowIdx++
		if p.rowIdx >= len(p.rows) {
			return false
		}
	}
	return true
}

// At returns the current storage.Series. It expects to get rows to contain
// four arrays in binary format which it attempts to deserialize into specific types.
// It also expects that the first two and second two arrays are the same length.
func (p *pgxSeriesSet) At() storage.Series {
	if p.rowIdx >= len(p.rows) {
		return nil
	}

	// Setting invalid data until we confirm that all data is valid.
	p.err = errInvalidData

	ps := &pgxSeries{}
	if err := p.rows[p.rowIdx].Scan(&ps.labelNames, &ps.labelValues, &ps.times, &ps.values); err != nil {
		return nil
	}

	if len(ps.labelNames.Elements) != len(ps.labelValues.Elements) {
		return nil
	}

	if len(ps.times.Elements) != len(ps.values.Elements) {
		return nil
	}

	p.err = nil
	return ps
}

// Err implements storage.SeriesSet.
func (p *pgxSeriesSet) Err() error {
	return p.err
}

// pgxSeries implements storage.Series.
type pgxSeries struct {
	labelNames  pgtype.TextArray
	labelValues pgtype.TextArray
	times       pgtype.TimestamptzArray
	values      pgtype.Float8Array
}

// Labels returns the label names and values for the series.
func (p *pgxSeries) Labels() labels.Labels {
	ll := make(labels.Labels, len(p.labelNames.Elements))

	for i := range ll {
		ll[i].Name = p.labelNames.Elements[i].String
		ll[i].Value = p.labelValues.Elements[i].String
	}

	sort.Sort(ll)

	return ll
}

// Iterator returns a chunkenc.Iterator for iterating over series data.
func (p *pgxSeries) Iterator() chunkenc.Iterator {
	return newIterator(p.times, p.values)
}

// pgxSeriesIterator implements storage.SeriesIterator.
type pgxSeriesIterator struct {
	cur          int
	totalSamples int
	times        pgtype.TimestamptzArray
	values       pgtype.Float8Array
}

// newIterator returns an iterator over the samples. It expects times and values to be the same length.
func newIterator(times pgtype.TimestamptzArray, values pgtype.Float8Array) *pgxSeriesIterator {
	return &pgxSeriesIterator{
		cur:          -1,
		totalSamples: len(times.Elements),
		times:        times,
		values:       values,
	}
}

// Seek implements storage.SeriesIterator.
func (p *pgxSeriesIterator) Seek(t int64) bool {
	p.cur = -1

	for p.Next() {
		if p.getTs() >= t {
			return true
		}
	}

	return false
}

// getTs returns a Unix timestamp in milliseconds.
func (p *pgxSeriesIterator) getTs() int64 {
	v := p.times.Elements[p.cur]

	switch v.InfinityModifier {
	case pgtype.NegativeInfinity:
		return math.MinInt64
	case pgtype.Infinity:
		return math.MaxInt64
	default:
		return v.Time.UnixNano() / 1e6
	}
}

func (p *pgxSeriesIterator) getVal() float64 {
	return p.values.Elements[p.cur].Float
}

// At returns a Unix timestamp in milliseconds and value of the sample.
func (p *pgxSeriesIterator) At() (t int64, v float64) {
	if p.cur >= p.totalSamples || p.cur < 0 {
		return 0, 0
	}
	return p.getTs(), p.getVal()
}

// Next implements storage.SeriesIterator.
func (p *pgxSeriesIterator) Next() bool {
	for {
		p.cur++
		if p.cur >= p.totalSamples {
			return false
		}
		if p.times.Elements[p.cur].Status == pgtype.Present &&
			p.values.Elements[p.cur].Status == pgtype.Present {
			return true
		}
	}
}

// Err implements storage.SeriesIterator.
func (p *pgxSeriesIterator) Err() error {
	return nil
}
