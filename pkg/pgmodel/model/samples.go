// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"math"
	"time"

	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/prompb"
)

type Samples interface {
	GetSeries() *Series
	CountSamples() int
	getSample(int) *prompb.Sample
}

type promSample struct {
	series  *Series
	samples []prompb.Sample
}

func NewPromSample(series *Series, samples []prompb.Sample) *promSample {
	return &promSample{series, samples}
}

func (t *promSample) GetSeries() *Series {
	return t.series
}

func (t *promSample) CountSamples() int {
	return len(t.samples)
}

func (t *promSample) getSample(index int) *prompb.Sample {
	return &t.samples[index]
}

// SampleInfoIterator is an iterator over a collection of sampleInfos that returns
// data in the format expected for the data table row.
type SampleInfoIterator struct {
	SampleInfos     []Samples
	SampleInfoIndex int
	SampleIndex     int
	MinSeen         int64
	err             error
}

// NewSampleInfoIterator is the constructor
func NewSampleInfoIterator() SampleInfoIterator {
	si := SampleInfoIterator{SampleInfos: make([]Samples, 0)}
	si.ResetPosition()
	return si
}

//Append adds a sample info to the back of the iterator
func (t *SampleInfoIterator) Append(s Samples) {
	t.SampleInfos = append(t.SampleInfos, s)
}

//ResetPosition resets the iteration position to the beginning
func (t *SampleInfoIterator) ResetPosition() {
	t.SampleIndex = -1
	t.SampleInfoIndex = 0
	t.MinSeen = math.MaxInt64
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (t *SampleInfoIterator) Next() bool {
	t.SampleIndex++
	if t.SampleInfoIndex < len(t.SampleInfos) && t.SampleIndex >= t.SampleInfos[t.SampleInfoIndex].CountSamples() {
		t.SampleInfoIndex++
		t.SampleIndex = 0
	}
	return t.SampleInfoIndex < len(t.SampleInfos)
}

// Values returns the values for the current row
func (t *SampleInfoIterator) Values() (time.Time, float64, SeriesID, SeriesEpoch) {
	info := t.SampleInfos[t.SampleInfoIndex]
	sample := info.getSample(t.SampleIndex)
	if t.MinSeen > sample.Timestamp {
		t.MinSeen = sample.Timestamp
	}
	sid, eid, err := info.GetSeries().GetSeriesID()
	if t.err == nil {
		t.err = err
	}
	return model.Time(sample.Timestamp).Time(), sample.Value, sid, eid
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *SampleInfoIterator) Err() error {
	return t.err
}
