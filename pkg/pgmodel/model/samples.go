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
