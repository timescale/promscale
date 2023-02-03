// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import "github.com/timescale/promscale/pkg/prompb"

type InsertableType uint8

const (
	Sample InsertableType = iota
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
}

// Iterator iterates over datapoints.
type Iterator interface {
	// HasNext returns true if there is any datapoint that is yet to be read.
	HasNext() bool
}

// SamplesIterator iterates over samples.
type SamplesIterator interface {
	Iterator
	// Value returns current samples timestamp and value.
	Value() (timestamp int64, values map[string]interface{})
}

// ExemplarsIterator iterates over exemplars.
type ExemplarsIterator interface {
	Iterator
	// Value returns the current exemplar's value array, timestamp and value.
	Value() (labels []prompb.Label, timestamp int64, value float64)
}
