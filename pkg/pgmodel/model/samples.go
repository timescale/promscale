// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"github.com/timescale/promscale/pkg/prompb"
	"sync"
)

type SeriesCacheKey struct {
	str string
}

func NewSeriesCacheKey(str string) *SeriesCacheKey {
	return &SeriesCacheKey{str}
}

func (k *SeriesCacheKey) String() string {
	return k.str
}

// Series represents a
type Series struct {
	lock             sync.RWMutex
	key              *SeriesCacheKey
	storedSeries     *StoredSeries
	unresolvedSeries *UnresolvedSeries
}

func NewSeries(key *SeriesCacheKey, unresolvedSeries *UnresolvedSeries, storedSeries *StoredSeries) *Series {
	return &Series{
		key:              key,
		storedSeries:     storedSeries,
		unresolvedSeries: unresolvedSeries,
	}
}

func (s *Series) StoredSeries() *StoredSeries {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.storedSeries
}

func (s *Series) UnresolvedSeries() *UnresolvedSeries {
	return s.unresolvedSeries
}

func (s *Series) SetSeries(series *StoredSeries) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.storedSeries = series
}

func (s *Series) CacheKey() *SeriesCacheKey {
	return s.key
}

type promSamples struct {
	series  *Series
	samples []prompb.Sample
}

func NewPromSamples(series *Series, sampleSet []prompb.Sample) Insertable {
	return &promSamples{series, sampleSet}
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

func (t *promSamples) Iterator() Iterator {
	return &samplesIterator{data: t.samples, total: len(t.samples)}
}

func (t *promSamples) Type() InsertableType {
	return Sample
}

func (t *promSamples) IsOfType(typ InsertableType) bool {
	return Sample == typ
}
