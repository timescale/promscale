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

// Data wraps incoming data with its in-timestamp. It is used to warn if the rate
// of incoming samples vs outgoing samples is too low, based on time.
type Data struct {
	Rows   map[string][]Samples
	InTime time.Time
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

// SamplesBatch is an iterator over a collection of sampleInfos that returns
// data in the format expected for the data table row.
type SamplesBatch struct {
	seriesSamples []Samples
	seriesIndex   int
	sampleIndex   int
	MinSeen       int64
	err           error
}

// NewSamplesBatch is the constructor
func NewSamplesBatch() SamplesBatch {
	si := SamplesBatch{seriesSamples: make([]Samples, 0)}
	si.ResetPosition()
	return si
}

func (t *SamplesBatch) Reset() {
	for i := 0; i < len(t.seriesSamples); i++ {
		// nil all pointers to prevent memory leaks
		t.seriesSamples[i] = nil
	}
	*t = SamplesBatch{seriesSamples: t.seriesSamples[:0]}
	t.ResetPosition()
}

func (t *SamplesBatch) CountSeries() int {
	return len(t.seriesSamples)
}

func (t *SamplesBatch) GetSeriesSamples() []Samples {
	return t.seriesSamples
}

func (t *SamplesBatch) CountSamples() int {
	c := 0
	for i := range t.seriesSamples {
		c += t.seriesSamples[i].CountSamples()
	}
	return c
}

//Append adds a sample info to the back of the iterator
func (t *SamplesBatch) Append(s Samples) {
	t.seriesSamples = append(t.seriesSamples, s)
}

func (t *SamplesBatch) AppendSlice(s []Samples) {
	t.seriesSamples = append(t.seriesSamples, s...)
}

//ResetPosition resets the iteration position to the beginning
func (t *SamplesBatch) ResetPosition() {
	t.sampleIndex = -1
	t.seriesIndex = 0
	t.MinSeen = math.MaxInt64
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (t *SamplesBatch) Next() bool {
	t.sampleIndex++
	if t.seriesIndex < len(t.seriesSamples) && t.sampleIndex >= t.seriesSamples[t.seriesIndex].CountSamples() {
		t.seriesIndex++
		t.sampleIndex = 0
	}
	return t.seriesIndex < len(t.seriesSamples)
}

// Values returns the values for the current row
func (t *SamplesBatch) Values() (time.Time, float64, SeriesID, SeriesEpoch) {
	info := t.seriesSamples[t.seriesIndex]
	sample := info.getSample(t.sampleIndex)
	if t.MinSeen > sample.Timestamp {
		t.MinSeen = sample.Timestamp
	}
	sid, eid, err := info.GetSeries().GetSeriesID()
	if t.err == nil {
		t.err = err
	}
	return model.Time(sample.Timestamp).Time(), sample.Value, sid, eid
}

func (t *SamplesBatch) Absorb(other SamplesBatch) {
	t.AppendSlice(other.seriesSamples)
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *SamplesBatch) Err() error {
	return t.err
}
