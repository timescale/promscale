// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"sync"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/prompb"
)

type InsertableExemplar interface {
	Insertable
	AllExemplarLabelKeys() []string
	OrderExemplarLabels(index map[string]int) (positionExists bool)
}

var promExemplarsPool = sync.Pool{New: func() interface{} { return new(promExemplars) }}

func putExemplars(s *promExemplars) {
	s.series = nil
	s.exemplars = s.exemplars[:0]
	promExemplarsPool.Put(s)
}

type promExemplars struct {
	series    *Series
	exemplars []prompb.Exemplar
}

func NewPromExemplars(series *Series, exemplarSet []prompb.Exemplar) InsertableExemplar {
	s := promExemplarsPool.Get().(*promExemplars)
	s.series = series
	if cap(s.exemplars) < len(exemplarSet) {
		s.exemplars = make([]prompb.Exemplar, len(exemplarSet))
	}
	s.exemplars = exemplarSet[:]
	return s
}

func (t *promExemplars) Series() *Series {
	return t.series
}

func (t *promExemplars) Count() int {
	return len(t.exemplars)
}

func (t *promExemplars) MaxTs() int64 {
	numSamples := len(t.exemplars)
	if numSamples == 0 {
		// If no samples exist, return a -ve int, so that the stats
		// caller does not capture this value.
		return -1
	}
	return t.exemplars[numSamples-1].Timestamp
}

func (t *promExemplars) At(index int) sampleFields {
	return t.exemplars[index]
}

// todo: pool
type exemplarsIterator struct {
	curr  int
	total int
	data  []prompb.Exemplar
}

var exemplarsIteratorPool = sync.Pool{New: func() interface{} { return new(exemplarsIterator) }}

func (i *exemplarsIterator) HasNext() bool {
	return i.curr < i.total
}

func (i *exemplarsIterator) Value() (labels []prompb.Label, timestamp int64, value float64) {
	datapoint := i.data[i.curr]
	labels, timestamp, value = datapoint.Labels, datapoint.Timestamp, datapoint.Value
	i.curr++
	return
}

func (i *exemplarsIterator) Close() {
	i.data = i.data[:0]
	i.curr = 0
	exemplarsIteratorPool.Put(i)
}

func (t *promExemplars) Iterator() Iterator {
	return &exemplarsIterator{data: t.exemplars, total: len(t.exemplars)}
}

// OrderExemplarLabels orders the existing labels in each exemplar, based on the index of the label key
// in _prom_catalog.exemplar_label_key_position table. The index received is the positions for each
// label key which is used to re-format the labels slice in exemplars.
//
// During ingestion, we need the label's value part only, as the label's key part is already handled
// by the index received from the _prom_catalog.exemplar_label_key_position table.
//
// OrderExemplarLabels returns positionNotExists as true if the index does not contain the position of some key. This happens
// when for same metric, two different series have exemplars with different labels_set, which will require the cache
// to have positions as union of the different labels_set. For this to happen, we need to re-fetch the positions with
// the missing keys and update the underlying cache (which happens in the calling function).
func (t *promExemplars) OrderExemplarLabels(index map[string]int) (positionExists bool) {
	for i := range t.exemplars {
		orderedLabels := make([]prompb.Label, len(index))
		fillEmptyValues(orderedLabels)
		labels := t.exemplars[i].Labels
		for _, l := range labels {
			key := l.Name
			value := l.Value
			position, exists := index[key]
			if !exists {
				return false
			}
			orderedLabels[position-1].Value = value
		}
		t.exemplars[i].Labels = orderedLabels
	}
	return true
}

const EmptyExemplarValues = ""

func fillEmptyValues(s []prompb.Label) []prompb.Label {
	for i := range s {
		s[i].Name = "" // Label keys are no more required during ingestion of exemplars.
		s[i].Value = EmptyExemplarValues
	}
	return s
}

func (t *promExemplars) AllExemplarLabelKeys() []string {
	uniqueKeys := make(map[string]struct{})
	for i := range t.exemplars {
		l := t.exemplars[i].Labels
		for _, k := range l {
			uniqueKeys[k.Name] = struct{}{}
		}
	}
	return getSlice(uniqueKeys)
}

func getSlice(m map[string]struct{}) []string {
	s := make([]string, 0, len(m))
	for k := range m {
		s = append(s, k)
	}
	return s
}

func (t *promExemplars) Type() InsertableType {
	return Exemplar
}

func (t *promExemplars) IsOfType(typ InsertableType) bool {
	return typ == Exemplar
}

func (t *promExemplars) Close() {
	putExemplars(t)
}

// ExemplarData is additional information associated with a time series.
type ExemplarData struct {
	Labels labels.Labels `json:"labels"`
	Value  float64       `json:"value"`
	Ts     int64         `json:"timestamp"` // This is int64 in Prometheus, but we do this to avoid later conversions to decimal.
}

type ExemplarQueryResult struct {
	SeriesLabels labels.Labels  `json:"seriesLabels"`
	Exemplars    []ExemplarData `json:"exemplars"`
}
