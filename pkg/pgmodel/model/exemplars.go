package model

import (
	"sync"

	"github.com/timescale/promscale/pkg/prompb"
)

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

func newExemplarSamples(series *Series, exemplarSet []prompb.Exemplar) Insertable {
	s := promExemplarsPool.Get().(*promExemplars)
	s.series = series
	s.exemplars = exemplarSet
	return s
}

func (t *promExemplars) GetSeries() *Series {
	return t.series
}

func (t *promExemplars) Count() int {
	return len(t.exemplars)
}

func (t *promExemplars) At(index int) sampleFields {
	return t.exemplars[index]
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

const emptyExemplarValues = "__promscale_no_value__"

func fillEmptyValues(s []prompb.Label) []prompb.Label {
	for i := range s {
		s[i].Name = "" // Label keys are no more required during ingestion of exemplars.
		s[i].Value = emptyExemplarValues
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
	for k, _ := range m {
		s = append(s, k)
	}
	return s
}

func (t *promExemplars) Type() InsertableType {
	return Exemplar
}

func (t *promExemplars) data() interface{} {
	return t
}
