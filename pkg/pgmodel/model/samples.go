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
	ExemplarLabels(index int) []prompb.Label
}

type sampleFields interface {
	// GetTs returns timestamp of the field (sample or exemplar).
	// It is based on non-pointer declaration to save allocs (see types.pb.go)
	// and also saves from type checks due to interface (as At() will then return interface{}).
	GetTs() int64
	// GetVal similar to GetTs, but returns value.
	GetVal() float64
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

// todo: rename promSample to sample.
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

func (t *promSamples) ExemplarLabels(_ int) []prompb.Label {
	return nil
}

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
	s := promSamplesPool.Get().(*promExemplars)
	s.series = series
	s.exemplars = exemplarSet[:0]
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
			numSamples++
		case Exemplar:
			numExemplars++
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

//func (t *Batch) Query(batch pgxconn.PgxBatch) (int, int64, error) {
//	numSamples, numExemplars := t.CountSamples()
//	times := make([]time.Time, numRows)
//	vals := make([]float64, numRows)
//	series := make([]int64, numRows)
//	lowestEpoch := SeriesEpoch(math.MaxInt64)
//
//	i := 0
//	for t.next() {
//		ts, val, seriesId, epoch, typ := t.values()
//		if epoch < lowestEpoch {
//			lowestEpoch = epoch
//		}
//		times[i] = ts
//		vals[i] = val
//		series[i] = int64(seriesId)
//		if t.dataType() == exemplar {
//			// exemplar
//		}
//	}
//	if t.Err() != nil {
//		return 0, 0, t.Err()
//	}
//}

// Values returns the values for the current row
func (t *Batch) Values() (time.Time, float64, SeriesID, SeriesEpoch, InsertableType) {
	sampleSet := t.data[t.seriesIndex]
	sample := sampleSet.At(t.dataIndex)
	if t.MinSeen > sample.GetTs() {
		t.MinSeen = sample.GetTs()
	}
	sid, eid, err := sampleSet.GetSeries().GetSeriesID()
	if t.err == nil {
		t.err = err
	}
	return model.Time(sample.GetTs()).Time(), sample.GetVal(), sid, eid, sampleSet.Type()
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

func (t *noopInsertable) ExemplarLabels(_ int) []prompb.Label {
	return nil
}

func (t *noopInsertable) data() interface{} {
	return nil
}
