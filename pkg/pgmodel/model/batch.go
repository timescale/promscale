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
	data []Insertable
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
	*t = Batch{data: t.data[:0]}
}

func (t *Batch) CountSeries() int {
	return len(t.data)
}

func (t *Batch) Data() []Insertable {
	return t.data
}

func (t *Batch) Count() (numSamples, numExemplars int) {
	for _, d := range t.data {
		if d.IsOfType(Sample) {
			numSamples += d.Count()
		} else if d.IsOfType(Exemplar) {
			numExemplars += d.Count()
		} else {
			panic(fmt.Sprintf("invalid type %T. Valid options: ['Sample', 'Exemplar']", d))
		}
	}
	return
}

func (t *Batch) AppendSlice(s []Insertable) {
	t.data = append(t.data, s...)
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
