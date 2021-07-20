// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"fmt"

	"github.com/timescale/promscale/pkg/prompb"
)

type InsertableType uint8

const (
	Noop InsertableType = iota
	Sample
	Exemplar
)

type Insertable interface {
	// Series returns the reference of the series, the insertable belongs to.
	Series() *Series
	// Count returns the number data points in the current insertable.
	Count() int
	// MaxTs returns the max timestamp among the datapoints in the insertable.
	// In most cases, this will be the timestamp from the last sample, since
	// Prometheus dispatches data in sorted order of time.
	MaxTs() int64
	// Iterator returns an iterator that iterates over underlying datapoints.
	Iterator() Iterator
	// Type returns type of underlying insertable.
	Type() InsertableType
	// IsOfType returns true if the provided type matches with the underlying insertable datatype.
	IsOfType(InsertableType) bool
	// Close puts the underlying insertable into the pool, allowing them to be reused.
	Close()
}

// Iterator iterates over data values.
type Iterator interface {
	// HasNext returns true if there is any datapoint that is yet to be read.
	HasNext() bool
	// Value returns the current datapoint's value and also increments the counter of the iterator.
	// The labels returned are labels of the exemplars, since samples do not have labels.
	// Hence, expect a nil in case of samples.
	Value() (labels []prompb.Label, timestamp int64, value float64)
	// Close puts the iterator alloc back into the pool, allowing it to be reused.
	Close()
}

func NewInsertable(series *Series, data interface{}) Insertable {
	if data == nil {
		// Used during tests.
		return newNoopInsertable(series)
	}
	switch n := data.(type) {
	case []prompb.Sample:
		return newPromSamples(series, n)
	case []prompb.Exemplar:
		return newPromExemplars(series, n)
	default:
		panic(fmt.Sprintf("invalid insertableType: %T", data))
	}
}

type noopInsertable struct {
	series *Series
}

func newNoopInsertable(s *Series) Insertable {
	return &noopInsertable{
		series: s,
	}
}

func (t *noopInsertable) Series() *Series {
	return t.series
}

func (t *noopInsertable) Count() int {
	return 0
}

func (t *noopInsertable) MaxTs() int64 {
	return -1
}

func (t *noopInsertable) Iterator() Iterator {
	return nil
}

func (t *noopInsertable) Type() InsertableType {
	return Noop
}

func (t *noopInsertable) IsOfType(typ InsertableType) bool {
	return typ == Noop
}

func (t *noopInsertable) AllExemplarLabelKeys() []string {
	return nil
}

func (t *noopInsertable) OrderExemplarLabels(_ map[string]int) bool { return false }

func (t *noopInsertable) Close() {}
