// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"fmt"
	"time"

	"github.com/timescale/promscale/pkg/log"
)

// Data wraps incoming data with its in-timestamp. It is used to warn if the rate
// of incoming samples vs outgoing samples is too low, based on time.
type Data struct {
	Rows         map[string][]Insertable
	ReceivedTime time.Time
}

// Batch is an iterator over a collection of Insertables that returns
// data in the format expected for the data table row.
type Batch struct {
	data         []Insertable
	numSamples   int
	numExemplars int
}

// NewBatch returns a new batch that can hold samples and exemplars.
func NewBatch() Batch {
	si := Batch{data: make([]Insertable, 0)}
	return si
}

func (t *Batch) Reset() {
	for i := 0; i < len(t.data); i++ {
		// nil all pointers to prevent memory leaks
		t.data[i] = nil
	}
	*t = Batch{data: t.data[:0], numSamples: 0, numExemplars: 0}
}

func (t *Batch) CountSeries() int {
	return len(t.data)
}

func (t *Batch) Data() []Insertable {
	return t.data
}

func (t *Batch) Count() (numSamples, numExemplars int) {
	return t.numSamples, t.numExemplars
}

func (t *Batch) AppendSlice(s []Insertable) {
	t.data = append(t.data, s...)
	for _, d := range s {
		if d.IsOfType(Sample) {
			t.numSamples += d.Count()
		} else if d.IsOfType(Exemplar) {
			t.numExemplars += d.Count()
		} else {
			panic(fmt.Sprintf("invalid type %T. Valid options: ['Sample', 'Exemplar']", d))
		}
	}
}

// ResizeTo resizes the batch to the specified size and returns the
// array of the overflown data.
func (t *Batch) ResizeTo(size int) []Insertable {
	if len(t.data) <= size {
		return nil
	}

	overflow := t.data[size:]

	for _, d := range overflow {
		switch d.Type() {
		case Sample:
			t.numSamples -= 1
		case Exemplar:
			t.numExemplars -= 1
		}
	}

	t.data = t.data[:size]
	return overflow
}

func (t *Batch) Visitor() *batchVisitor {
	return getBatchVisitor(t)
}

func (t *Batch) Absorb(other Batch) {
	t.AppendSlice(other.data)
}

func (t *Batch) Len() int {
	return t.CountSeries()
}

func (t *Batch) Swap(i, j int) {
	t.data[i], t.data[j] = t.data[j], t.data[i]
}

func (t *Batch) Less(i, j int) bool {
	s1, _, err := t.data[i].Series().GetSeriesID()
	if err != nil {
		log.Warn("seriesID", "not set but being sorted on")
	}
	s2, _, err := t.data[j].Series().GetSeriesID()
	if err != nil {
		log.Warn("seriesID", "not set but being sorted on")
	}
	return s1 < s2
}
