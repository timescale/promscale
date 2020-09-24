package pgmodel

import (
	"fmt"
	"sort"

	"github.com/jackc/pgtype"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/timescale/promscale/pkg/log"
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
	rowIdx  int
	rows    []timescaleRow
	err     error
	querier labelQuerier
}

// pgxSeriesSet must implement storage.SeriesSet
var _ storage.SeriesSet = (*pgxSeriesSet)(nil)

func buildSeriesSet(rows []timescaleRow, querier labelQuerier) storage.SeriesSet {
	return &pgxSeriesSet{
		rows:    rows,
		querier: querier,
		rowIdx:  -1,
	}
}

// Next forwards the internal cursor to next storage.Series
func (p *pgxSeriesSet) Next() bool {
	if p.rowIdx >= len(p.rows) {
		return false
	}
	p.rowIdx += 1
	if p.rowIdx >= len(p.rows) {
		return false
	}
	if p.err == nil {
		p.err = p.rows[p.rowIdx].err
	}
	return true
}

// At returns the current storage.Series.
func (p *pgxSeriesSet) At() storage.Series {
	if p.rowIdx >= len(p.rows) {
		return nil
	}

	row := &p.rows[p.rowIdx]

	if row.err != nil {
		return nil
	}
	if len(row.times.Elements) != len(row.values.Elements) {
		p.err = errInvalidData
		return nil
	}

	ps := &pgxSeries{
		times:  row.times,
		values: row.values,
	}

	// this should pretty much always be non-empty due to __name__, but it
	// costs little to check here
	if len(row.labelIds) != 0 {
		lls, err := p.querier.getLabelsForIds(row.labelIds)
		if err != nil {
			log.Error("err", err)
			return nil
		}
		sort.Sort(lls)
		ps.labels = lls
	}

	return ps
}

// Err implements storage.SeriesSet.
func (p *pgxSeriesSet) Err() error {
	if p.err != nil {
		return fmt.Errorf("Error retrieving series set: %w", p.err)
	}
	return nil
}

func (p *pgxSeriesSet) Warnings() storage.Warnings { return nil }

// pgxSeries implements storage.Series.
type pgxSeries struct {
	labels labels.Labels
	times  pgtype.TimestamptzArray
	values pgtype.Float8Array
}

// Labels returns the label names and values for the series.
func (p *pgxSeries) Labels() labels.Labels {
	return p.labels
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
	return timestamptzToMs(p.times.Elements[p.cur])
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
