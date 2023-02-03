// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"github.com/timescale/promscale/pkg/prompb"
)

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
func (i *samplesIterator) Value() (int64, map[string]interface{}) {
	defer func() {
		i.curr++
	}()
	timestamp := i.data[i.curr].Timestamp
	isMultiValued := len(i.data[i.curr].MultiValue) > 0
	if isMultiValued {
		return timestamp, i.data[i.curr].MultiValue
	}

	singleValue := map[string]interface{}{"": i.data[i.curr].Value}
	return timestamp, singleValue
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
