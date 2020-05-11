package prompb

import (
	"errors"
	"fmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"sort"
)

// ToQuery builds a Query proto.
func ToQuery(from, to int64, matchers []*labels.Matcher, hints *storage.SelectHints) (*Query, error) {
	ms, err := toLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}

	var rp *ReadHints
	if hints != nil {
		rp = &ReadHints{
			StartMs:  hints.Start,
			EndMs:    hints.End,
			StepMs:   hints.Step,
			Func:     hints.Func,
			Grouping: hints.Grouping,
			By:       hints.By,
			RangeMs:  hints.Range,
		}
	}

	return &Query{
		StartTimestampMs: from,
		EndTimestampMs:   to,
		Matchers:         ms,
		Hints:            rp,
	}, nil
}

// FromQueryResult unpacks and sorts a QueryResult proto.
func FromQueryResult(sortSeries bool, res *QueryResult) storage.SeriesSet {
	series := make([]storage.Series, 0, len(res.Timeseries))
	for _, ts := range res.Timeseries {
		lbls := labelProtosToLabels(ts.Labels)
		if err := validateLabelsAndMetricName(lbls); err != nil {
			return errSeriesSet{err: err}
		}

		series = append(series, &concreteSeries{
			labels:  lbls,
			samples: ts.Samples,
		})
	}

	if sortSeries {
		sort.Sort(byLabel(series))
	}
	return &concreteSeriesSet{
		series: series,
	}
}

func toLabelMatchers(matchers []*labels.Matcher) ([]*LabelMatcher, error) {
	pbMatchers := make([]*LabelMatcher, 0, len(matchers))
	for _, m := range matchers {
		var mType LabelMatcher_Type
		switch m.Type {
		case labels.MatchEqual:
			mType = LabelMatcher_EQ
		case labels.MatchNotEqual:
			mType = LabelMatcher_NEQ
		case labels.MatchRegexp:
			mType = LabelMatcher_RE
		case labels.MatchNotRegexp:
			mType = LabelMatcher_NRE
		default:
			return nil, errors.New("invalid matcher type")
		}
		pbMatchers = append(pbMatchers, &LabelMatcher{
			Type:  mType,
			Name:  m.Name,
			Value: m.Value,
		})
	}
	return pbMatchers, nil
}

func labelProtosToLabels(labelPairs []Label) labels.Labels {
	result := make(labels.Labels, 0, len(labelPairs))
	for _, l := range labelPairs {
		result = append(result, labels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	sort.Sort(result)
	return result
}

// validateLabelsAndMetricName validates the label names/values and metric names returned from remote read,
// also making sure that there are no labels with duplicate names
func validateLabelsAndMetricName(ls labels.Labels) error {
	for i, l := range ls {
		if l.Name == labels.MetricName && !model.IsValidMetricName(model.LabelValue(l.Value)) {
			return fmt.Errorf("invalid metric name: %v", l.Value)
		}
		if !model.LabelName(l.Name).IsValid() {
			return fmt.Errorf("invalid label name: %v", l.Name)
		}
		if !model.LabelValue(l.Value).IsValid() {
			return fmt.Errorf("invalid label value: %v", l.Value)
		}
		if i > 0 && l.Name == ls[i-1].Name {
			return fmt.Errorf("duplicate label with name: %v", l.Name)
		}
	}
	return nil
}

// errSeriesSet implements storage.SeriesSet, just returning an error.
type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool {
	return false
}

func (errSeriesSet) At() storage.Series {
	return nil
}

func (e errSeriesSet) Err() error {
	return e.err
}

// concreteSeriesSet implements storage.SeriesSet.
type concreteSeriesSet struct {
	cur    int
	series []storage.Series
}

func (c *concreteSeriesSet) Next() bool {
	c.cur++
	return c.cur-1 < len(c.series)
}

func (c *concreteSeriesSet) At() storage.Series {
	return c.series[c.cur-1]
}

func (c *concreteSeriesSet) Err() error {
	return nil
}

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

type byLabel []storage.Series

func (a byLabel) Len() int           { return len(a) }
func (a byLabel) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byLabel) Less(i, j int) bool { return labels.Compare(a[i].Labels(), a[j].Labels()) < 0 }
