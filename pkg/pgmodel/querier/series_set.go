// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package querier

import (
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

const (
	// Postgres time zero is Sat Jan 01 00:00:00 2000 UTC.
	// This is the offset of the Unix epoch in milliseconds from the Postgres zero.
	PostgresUnixEpoch = -946684800000
)

// pgxSamplesSeriesSet implements storage.SeriesSet.
type pgxSamplesSeriesSet struct {
	rowIdx     int
	rows       []sampleRow
	labelIDMap map[int64]labels.Label
	err        error
	querier    labelQuerier
}

// pgxSamplesSeriesSet must implement storage.SeriesSet
var _ storage.SeriesSet = (*pgxSamplesSeriesSet)(nil)

func buildSeriesSet(rows []sampleRow, querier labelQuerier) SeriesSet {
	labelIDMap := make(map[int64]labels.Label)
	initLabelIdIndexForSamples(labelIDMap, rows)

	err := querier.LabelsForIdMap(labelIDMap)
	if err != nil {
		return &errorSeriesSet{err}
	}

	return &pgxSamplesSeriesSet{
		rows:       rows,
		querier:    querier,
		rowIdx:     -1,
		labelIDMap: labelIDMap,
	}
}

// Next forwards the internal cursor to next storage.Series
func (p *pgxSamplesSeriesSet) Next() bool {
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
func (p *pgxSamplesSeriesSet) At() storage.Series {
	if p.rowIdx >= len(p.rows) {
		return nil
	}

	row := &p.rows[p.rowIdx]

	if row.err != nil {
		return nil
	}
	if row.times.Len() != len(row.values.FlatArray) {
		p.err = errors.ErrInvalidRowData
		return nil
	}

	ps := &pgxSeries{
		times:  row.times,
		values: row.values,
	}

	// this should pretty much always be non-empty due to __name__, but it
	// costs little to check here
	if len(row.labelIds) == 0 {
		return ps
	}

	lls, err := getLabelsFromLabelIds(row.labelIds, p.labelIDMap)
	if err != nil {
		p.err = err
	}

	if row.metricOverride != "" {
		for i := range lls {
			if lls[i].Name == model.MetricNameLabelName {
				lls[i].Value = row.metricOverride
				break
			}
		}
	}
	lls = append(lls, row.GetAdditionalLabels()...)

	sort.Sort(lls)
	ps.labels = lls

	return ps
}

func getLabelsFromLabelIds(labelIds []*int64, index map[int64]labels.Label) (labels.Labels, error) {
	lls := make([]labels.Label, 0, len(labelIds))
	for _, id := range labelIds {
		if id == nil || *id == 0 {
			continue
		}
		label, ok := index[*id]
		if !ok {
			return nil, fmt.Errorf("missing label for id %v", *id)
		}
		if label == (labels.Label{}) {
			return nil, fmt.Errorf("missing label for id %v", *id)
		}
		lls = append(lls, label)
	}
	return lls, nil
}

// Err implements storage.SeriesSet.
func (p *pgxSamplesSeriesSet) Err() error {
	if p.err != nil {
		return fmt.Errorf("error retrieving series set: %w", p.err)
	}
	return nil
}

func (p *pgxSamplesSeriesSet) Warnings() storage.Warnings { return nil }

func (p *pgxSamplesSeriesSet) Close() {
	for _, row := range p.rows {
		row.Close()
	}
}

// pgxSeries implements storage.Series.
type pgxSeries struct {
	labels labels.Labels
	times  TimestampSeries
	values *model.ReusableArray[pgtype.Float8]
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
	times        TimestampSeries
	values       *model.ReusableArray[pgtype.Float8]
}

// newIterator returns an iterator over the samples. It expects times and values to be the same length.
func newIterator(times TimestampSeries, values *model.ReusableArray[pgtype.Float8]) *pgxSeriesIterator {
	return &pgxSeriesIterator{
		cur:          -1,
		totalSamples: times.Len(),
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
	ts, _ := p.times.At(p.cur)
	return ts
}

func (p *pgxSeriesIterator) getVal() float64 {
	return p.values.FlatArray[p.cur].Float64
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
		_, ok := p.times.At(p.cur)
		if ok && p.values.FlatArray[p.cur].Valid {
			return true
		}
	}
}

// Err implements storage.SeriesIterator.
func (p *pgxSeriesIterator) Err() error {
	return nil
}
