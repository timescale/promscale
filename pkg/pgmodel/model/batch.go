// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"fmt"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"math"
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/prompb"
)

type sampleFields interface {
	// GetTs returns timestamp of the field (sample or exemplar).
	// It is based on non-pointer declaration to save allocs (see types.pb.go)
	// and also saves from type checks due to interface (as At() will then return interface{}).
	GetTs() int64
	// GetVal similar to GetTs, but returns value.
	GetVal() float64
	// ExemplarLabels returns labels of exemplars.
	ExemplarLabels() []prompb.Label
}

// Data wraps incoming data with its in-timestamp. It is used to warn if the rate
// of incoming samples vs outgoing samples is too low, based on time.
type Data struct {
	Rows         map[string][]Insertable
	ReceivedTime time.Time
}

// Batch is an iterator over a collection of Insertables that returns
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

type BatchIterator struct {
	currInsertableIndex    int
	currInsertableIterator Iterator
	totalInsertables       int
	data                   []Insertable
	batchCopy              *Batch // Used to update stats in Batch, like MinSeen.
	err                    error
}

var batchIteratorPool = sync.Pool{New: func() interface{} { return new(BatchIterator) }}

func putBatchIterator(itr *BatchIterator) {
	itr.data = itr.data[:0]
	itr.totalInsertables = 0
	itr.currInsertableIndex = 0
	itr.currInsertableIterator = nil
	itr.batchCopy = nil
	itr.err = nil
	batchIteratorPool.Put(itr)
}

func (b *BatchIterator) insertablesPending() bool {
	return b.currInsertableIndex < b.totalInsertables
}

func (b *BatchIterator) HasNext() bool {
	if b.currInsertableIterator == nil {
		// Set the first iterator.
		if len(b.data) == 0 {
			return false
		}
		b.currInsertableIterator = b.data[b.currInsertableIndex].Iterator()
	}
	insertablesPending := b.insertablesPending()            // Whether list of insertables in the batch have not been covered.
	datapointsPending := b.currInsertableIterator.HasNext() // Whether datapoints in an insertable are yet to be read.
	if !datapointsPending && insertablesPending {
		// All datapoints in the current insertable have been read.
		// Let's move to the next insertable if there exists one.
		var newItr Iterator
		for {
			b.currInsertableIndex++
			if !b.insertablesPending() {
				return false
			}
			newItr = b.data[b.currInsertableIndex].Iterator()
			if newItr.HasNext() {
				break
			}
		}
		b.currInsertableIterator = newItr
	}
	// Return true if there are more insertables to be read or if the current insertable has pending datapoints.
	return insertablesPending || datapointsPending
}

// Value returns the current value of the insertable iterator and increments the count of insertable
// iterator by calling its Value().
func (b *BatchIterator) Value() ([]prompb.Label, time.Time, float64, SeriesID, SeriesEpoch, InsertableType) {
	datapointsIterator := b.currInsertableIterator
	lbls, t, v := datapointsIterator.Value()

	// Let's fetch metadata of current insertable.
	currentInsertable := b.data[b.currInsertableIndex]
	seriesId, seriesEpoch, err := currentInsertable.Series().GetSeriesID()
	if err != nil {
		b.err = err
	}

	if b.batchCopy.MinSeen > t {
		b.batchCopy.MinSeen = t
	}

	return lbls, timestamp.Time(t), v, seriesId, seriesEpoch, currentInsertable.Type()
}

// Error returns the most recent error, that is encountered while iterating over insertables.
func (b *BatchIterator) Error() error {
	return b.err
}

// Close puts the batchIterator back into the pool.
func (b *BatchIterator) Close() {
	putBatchIterator(b)
}

// Iterator iterates over []model.Insertable that belong to this batch.
func (t *Batch) Iterator() *BatchIterator {
	itr := batchIteratorPool.Get().(*BatchIterator)
	if cap(itr.data) < len(t.data) {
		// Allocate memory if the existing buffer cannot satisfy the demand.
		itr.data = make([]Insertable, 0, len(t.data))
	}
	itr.data = t.data[:]
	// Its slightly faster to store length in a var, that everytime calling it in HasNext().
	// The importance increases further, since HasNext() falls in a hot path.
	// See https://stackoverflow.com/a/26635279/14562953
	itr.totalInsertables = len(t.data)
	itr.batchCopy = t
	return itr
}

func (t *Batch) Absorb(other Batch) {
	t.AppendSlice(other.data)
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *Batch) Err() error {
	return t.err
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
