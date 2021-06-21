// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/prompb"
)

type Metadata struct {
	MetricFamily string `json:"metric,omitempty"`
	Unit         string `json:"unit"`
	Type         string `json:"type"`
	Help         string `json:"help"`
}

type InsertableType uint8

const (
	Invalid InsertableType = iota
	Sample
	Exemplar
)

type Insertable interface {
	GetSeries() *Series
	Count() int
	// MaxTs returns the approximate maxiumum timestamp of datapoints in the insertable.
	// In most cases, this will be timestamp of the last datapoint, since they are sorted
	// by default when Prometheus dispatches them.
	MaxTs() int64
	At(index int) sampleFields
	Type() InsertableType
	data() interface{}
	AllExemplarLabelKeys() []string
	OrderExemplarLabels(index map[string]int) (positionNotExists bool)
}

type sampleFields interface {
	// GetTs returns timestamp of the field (sample or exemplar).
	// It is based on non-pointer declaration to save allocs (see types.pb.go)
	// and also saves from type checks due to interface (as At() will then return interface{}).
	GetTs() int64
	// GetVal similar to GetTs, but returns value.
	GetVal() float64
	// ExemplarLabels returns labels of exemplars.
	ExemplarLabels() []prompb.Label
}

func NewInsertable(series *Series, data interface{}) Insertable {
	if data == nil {
		return newNoopInsertable(series)
	}
	switch n := data.(type) {
	case []prompb.Sample:
		return newPromSamples(series, n)
	case []prompb.Exemplar:
		return newExemplarSamples(series, n)
	default:
		panic("invalid insertableType")
	}
}

// Data wraps incoming data with its in-timestamp. It is used to warn if the rate
// of incoming samples vs outgoing samples is too low, based on time.
type Data struct {
	Rows         map[string][]Insertable
	ReceivedTime time.Time
}

var promSamplesPool = sync.Pool{New: func() interface{} { return new(promSamples) }}

// putSamples adds samples to the pool.
func putSamples(s *promSamples) {
	s.series = nil
	s.samples = s.samples[:0]
	promSamplesPool.Put(s)
}

type promSamples struct {
	series  *Series
	samples []prompb.Sample
}

func newPromSamples(series *Series, sampleSet []prompb.Sample) Insertable {
	s := promSamplesPool.Get().(*promSamples)
	s.series = series
	s.samples = sampleSet
	return s
}

func (t *promSamples) GetSeries() *Series {
	return t.series
}

func (t *promSamples) Count() int {
	return len(t.samples)
}

func (t *promSamples) At(index int) sampleFields {
	return t.samples[index]
}

func (t *promSamples) MaxTs() int64 {
	numSamples := len(t.samples)
	if numSamples == 0 {
		// If no samples exist, return a -ve int, so that the stats
		// caller does not capture this value.
		return math.MinInt64
	}
	return t.samples[numSamples-1].Timestamp
}

func (t *promSamples) AllExemplarLabelKeys() []string {
	return nil
}

func (t *promSamples) OrderExemplarLabels(_ map[string]int) bool { return false }

func (t *promSamples) Type() InsertableType {
	return Sample
}

func (t *promSamples) data() interface{} {
	return t
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

func (t *promExemplars) MaxTs() int64 {
	numExemplars := len(t.exemplars)
	if numExemplars == 0 {
		// If no exemplars exist, return a -ve int, so that the stats
		// caller does not capture this value.
		return math.MinInt64
	}
	return t.exemplars[numExemplars-1].Timestamp
}

func (t *promExemplars) ExemplarLabels(index int) []prompb.Label {
	return t.exemplars[index].Labels
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
func (t *promExemplars) OrderExemplarLabels(index map[string]int) (positionNotExists bool) {
	for i := range t.exemplars {
		orderedLabels := make([]prompb.Label, len(index))
		orderedLabels = fillEmptyValues(orderedLabels)
		labels := t.exemplars[i].Labels
		for labelIndex := range labels {
			key := labels[labelIndex].Name
			value := labels[labelIndex].Value
			position, exists := index[key] // We always expect the position to be present. A fatal case for data loss would
			if !exists {
				return true
			}
			orderedLabels[position-1].Value = value
		}
		for j := range orderedLabels {
			if orderedLabels[j].Value == emptyExemplarValues {
				panic(orderedLabels)
			}
		}
		t.exemplars[i].Labels = orderedLabels
	}
	return false
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

// Batch is an iterator over a collection of sampleInfos that returns
// data in the format expected for the data table row.
type Batch struct {
	data        []Insertable
	seriesIndex int
	dataIndex   int
	MinSeen     int64
	err         error
}

// NewBatch returns a new batch that can hold samples and exemplars.
func NewBatch() Batch {
	si := Batch{data: make([]Insertable, 0)}
	si.ResetPosition()
	return si
}

func (t *Batch) Reset() {
	for i := 0; i < len(t.data); i++ {
		// nil all pointers to prevent memory leaks
		t.data[i] = nil
	}
	*t = Batch{data: t.data[:0]}
	t.ResetPosition()
}

func (t *Batch) CountSeries() int {
	return len(t.data)
}

func (t *Batch) Data() []Insertable {
	return t.data
}

func (t *Batch) Count() (numSamples, numExemplars int) {
	for i := range t.data {
		switch t.data[i].Type() {
		case Sample:
			numSamples += t.data[i].Count()
		case Exemplar:
			numExemplars += t.data[i].Count()
		default:
			panic(fmt.Sprintf("invalid type %d", t.data[i].Type()))
		}
	}
	return
}

// Append adds a sample info to the back of the iterator
func (t *Batch) Append(s Insertable) {
	t.data = append(t.data, s)
}

func (t *Batch) AppendSlice(s []Insertable) {
	t.data = append(t.data, s...)
}

//ResetPosition resets the iteration position to the beginning
func (t *Batch) ResetPosition() {
	t.dataIndex = -1
	t.seriesIndex = 0
	t.MinSeen = math.MaxInt64
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (t *Batch) Next() bool {
	t.dataIndex++
	if t.seriesIndex < len(t.data) && t.dataIndex >= t.data[t.seriesIndex].Count() {
		t.seriesIndex++
		t.dataIndex = 0
	}
	return t.seriesIndex < len(t.data)
}

func (t *Batch) dataType() InsertableType {
	return t.data[t.seriesIndex].Type()
}

// Values returns the values for the current row
func (t *Batch) Values() (time.Time, float64, SeriesID, SeriesEpoch, InsertableType) {
	set := t.data[t.seriesIndex]
	sample := set.At(t.dataIndex)
	if t.MinSeen > sample.GetTs() {
		t.MinSeen = sample.GetTs()
	}
	sid, eid, err := set.GetSeries().GetSeriesID()
	if t.err == nil {
		t.err = err
	}
	return model.Time(sample.GetTs()).Time(), sample.GetVal(), sid, eid, set.Type()
}

// GetCorrespondingLabelValues returns the label values of the underlying data (exemplar) in the iterator.
func (t *Batch) GetCorrespondingLabelValues() []string {
	set := t.data[t.seriesIndex]
	exemplar := set.At(t.dataIndex)
	lbls := exemplar.ExemplarLabels()
	s := make([]string, len(lbls))
	for i := range lbls {
		s[i] = lbls[i].Value
	}
	return s
}

func (t *Batch) Absorb(other Batch) {
	t.AppendSlice(other.data)
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *Batch) Err() error {
	return t.err
}

// Release puts the underlying promSamples back into its pool so that they can be reused.
func (t *Batch) Release() {
	for i := range t.data {
		data := t.data[i].data()
		switch n := data.(type) {
		case *promSamples:
			putSamples(n)
		case *promExemplars:
			putExemplars(n)
		}
	}
}

type noopInsertable struct {
	series *Series
}

func newNoopInsertable(s *Series) Insertable {
	return &noopInsertable{
		series: s,
	}
}

func (t *noopInsertable) GetSeries() *Series {
	return t.series
}

func (t *noopInsertable) Count() int {
	return 0
}

func (t *noopInsertable) At(_ int) sampleFields {
	return nil
}

func (t *noopInsertable) MaxTs() int64 {
	return -1
}

func (t *noopInsertable) Type() InsertableType {
	return Invalid
}

func (t *noopInsertable) AllExemplarLabelKeys() []string {
	return nil
}

func (t *noopInsertable) OrderExemplarLabels(_ map[string]int) bool { return false }

func (t *noopInsertable) data() interface{} {
	return nil
}
