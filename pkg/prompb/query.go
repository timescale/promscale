package prompb

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"sort"
)

// ToQuery builds a Query proto.

// FromQueryResult unpacks and sorts a QueryResult proto.

// validateLabelsAndMetricName validates the label names/values and metric names returned from remote read,
// also making sure that there are no labels with duplicate names

// concreteSeries implements storage.Series.
type concreteSeries struct {
	labels  labels.Labels
	samples []Sample
}

func (c *concreteSeries) Labels() labels.Labels {
	return labels.New(c.labels...)
}

func (c *concreteSeries) Iterator() chunkenc.Iterator {
	return newConcreteSeriersIterator(c)
}

// concreteSeriesIterator implements storage.SeriesIterator.
type concreteSeriesIterator struct {
	cur    int
	series *concreteSeries
}

func newConcreteSeriersIterator(series *concreteSeries) chunkenc.Iterator {
	return &concreteSeriesIterator{
		cur:    -1,
		series: series,
	}
}

// Seek implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Seek(t int64) bool {
	c.cur = sort.Search(len(c.series.samples), func(n int) bool {
		return c.series.samples[n].Timestamp >= t
	})
	return c.cur < len(c.series.samples)
}

// At implements storage.SeriesIterator.
func (c *concreteSeriesIterator) At() (t int64, v float64) {
	s := c.series.samples[c.cur]
	return s.Timestamp, s.Value
}

// Next implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Next() bool {
	c.cur++
	return c.cur < len(c.series.samples)
}

// Err implements storage.SeriesIterator.
func (c *concreteSeriesIterator) Err() error {
	return nil
}
