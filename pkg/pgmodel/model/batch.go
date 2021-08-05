// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"fmt"
	"math"
	"time"
)

// Data wraps incoming data with its in-timestamp. It is used to warn if the rate
// of incoming samples vs outgoing samples is too low, based on time.
type Data struct {
	Rows              map[string][]Insertable
	ReceivedTime      time.Time
	ContainsExemplars bool
}

// Batch is an iterator over a collection of Insertables that returns
// data in the format expected for the data table row.
type Batch struct {
	data        []Insertable
	seriesIndex int
	dataIndex   int

	MinSeen           int64
	ContainsExemplars bool
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

//ResetPosition resets the iteration position to the beginning
func (t *Batch) ResetPosition() {
	t.dataIndex = -1
	t.seriesIndex = 0
	t.MinSeen = math.MaxInt64
}

func (t *Batch) Visitor() *batchVisitor {
	vtr := batchVisitorPool.Get().(*batchVisitor)
	if cap(vtr.data) < len(t.data) {
		vtr.data = make([]Insertable, 0, len(t.data))
	}
	vtr.data = t.data[:]
	vtr.batchCopy = t
	return vtr
}

func (t *Batch) Absorb(other Batch) {
	t.AppendSlice(other.data)
}

// Close closes the batch and puts the underlying insertables back into their respective pools,
// allowing them to be re-used.
//
// Batch should never be used once Close() is called.
func (t *Batch) Close() {
	for _, insertable := range t.data {
		insertable.Close()
	}
}
