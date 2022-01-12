// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/timescale/promscale/pkg/prompb"
)

type InsertableExemplar interface {
	Insertable
	AllExemplarLabelKeys() []string
	OrderExemplarLabels(index map[string]int) (positionExists bool)
}

type PromExemplars struct {
	series    *Series
	exemplars []prompb.Exemplar
}

func NewPromExemplars(series *Series, exemplarSet []prompb.Exemplar) InsertableExemplar {
	return &PromExemplars{series, exemplarSet}
}

func (t *PromExemplars) Series() *Series {
	return t.series
}

func (t *PromExemplars) Count() int {
	return len(t.exemplars)
}

func (t *PromExemplars) MaxTs() int64 {
	numSamples := len(t.exemplars)
	if numSamples == 0 {
		// If no samples exist, return a -ve int, so that the stats
		// caller does not capture this value.
		return -1
	}
	return t.exemplars[numSamples-1].Timestamp
}

type exemplarsIterator struct {
	curr  int
	total int
	data  []prompb.Exemplar
}

func (i *exemplarsIterator) HasNext() bool {
	return i.curr < i.total
}

func (i *exemplarsIterator) Value() (labels []prompb.Label, timestamp int64, value float64) {
	datapoint := i.data[i.curr]
	labels, timestamp, value = datapoint.Labels, datapoint.Timestamp, datapoint.Value
	i.curr++
	return
}

func (t *PromExemplars) Iterator() Iterator {
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
func (t *PromExemplars) OrderExemplarLabels(index map[string]int) (positionExists bool) {
	for i := range t.exemplars {
		// We fetch the highest position in the index because we need to fill all the positions, even if they are empty.
		// Example: the index can be
		// {"some_a": 1, "some_b": 2, "job": 3, "TraceID": 4, "some_c": 5, "random_label": 6}
		// This means that when the position was created, there were already some indexes with position 1 and 2, but they
		// have not been asked while fetching the positions.
		//
		// If exemplar labels are [{"job": "promscale"}, {"TraceID": "some_id"}, {"random_label": "some_value"}]
		// When we write the values here, we have to write like
		// "", "", promscale, "some_id", "", "some_value" (Note: the index is index-1, as array in postgres starts from 1).
		// Hence, we need to allocate the highest possible index position present in the index.
		orderedLabels := make([]prompb.Label, highestPos(index))
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

func highestPos(index map[string]int) (highest int) {
	highest = -1
	for _, v := range index {
		if v > highest {
			highest = v
		}
	}
	return
}

const EmptyExemplarValues = ""

func fillEmptyValues(s []prompb.Label) []prompb.Label {
	for i := range s {
		s[i].Name = "" // Label keys are no more required during ingestion of exemplars.
		s[i].Value = EmptyExemplarValues
	}
	return s
}

func (t *PromExemplars) AllExemplarLabelKeys() []string {
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

func (t *PromExemplars) Type() InsertableType {
	return Exemplar
}

func (t *PromExemplars) IsOfType(typ InsertableType) bool {
	return typ == Exemplar
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
