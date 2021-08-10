// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"sync"

	"github.com/timescale/promscale/pkg/prompb"
)

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

func NewPromSamples(series *Series, sampleSet []prompb.Sample) Insertable {
	s := promSamplesPool.Get().(*promSamples)
	s.series = series
	if cap(s.samples) < len(sampleSet) {
		s.samples = make([]prompb.Sample, len(sampleSet))
	}
	s.samples = sampleSet[:] // todo: is this right too?
	return s
}

func (t *promSamples) Series() *Series {
	return t.series
}

func (t *promSamples) Count() int {
	return len(t.samples)
}

type samplesIterator struct {
	curr  int
	total int
	data  []prompb.Sample
}

func (t *promSamples) MaxTs() int64 {
	numSamples := len(t.samples)
	if numSamples == 0 {
		// If no samples exist, return a -ve int, so that the stats
		// caller does not capture this value.
		return -1
	}
	return t.samples[numSamples-1].Timestamp
}

var samplesIteratorPool = sync.Pool{New: func() interface{} { return new(samplesIterator) }}

func (i *samplesIterator) HasNext() bool {
	return i.curr < i.total
}

// Value in samplesIterator does not return labels, since samples do not have labels.
// Its the series that have th labels in samples.
func (i *samplesIterator) Value() (timestamp int64, value float64) {
	timestamp, value = i.data[i.curr].Timestamp, i.data[i.curr].Value
	i.curr++
	return
}

func (i *samplesIterator) Close() {
	i.data = i.data[:0]
	i.curr = 0
	samplesIteratorPool.Put(i)
}

func (t *promSamples) Iterator() Iterator {
	return &samplesIterator{data: t.samples, total: len(t.samples)}
}

func (t *promSamples) Type() InsertableType {
	return Sample
}

func (t *promSamples) IsOfType(typ InsertableType) bool {
	return Sample == typ
}

func (t *promSamples) Close() {
	putSamples(t)
}
