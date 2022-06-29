// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/timescale/promscale/pkg/prompb"
)

// SeriesID represents a globally unique id for the series. This should be equivalent
// to the PostgreSQL type in the series table (currently BIGINT).
type SeriesID int64

const (
	invalidSeriesID    = -1
	InvalidSeriesEpoch = -1
)

func (s SeriesID) String() string {
	return strconv.FormatInt(int64(s), 10)
}

// SeriesEpoch represents the series epoch
type SeriesEpoch int64

// Series stores a Prometheus labels.Labels in its canonical string representation
type Series struct {
	//protects names, values, seriesID, epoch
	//str and metricName are immutable and doesn't need a lock
	lock     sync.RWMutex
	names    []string
	values   []string
	seriesID SeriesID
	epoch    SeriesEpoch

	metricName string
	str        string
}

func NewSeries(key string, labelPairs []prompb.Label) *Series {
	series := &Series{
		names:    make([]string, len(labelPairs)),
		values:   make([]string, len(labelPairs)),
		str:      key,
		seriesID: invalidSeriesID,
		epoch:    InvalidSeriesEpoch,
	}
	for i, l := range labelPairs {
		series.names[i] = l.Name
		series.values[i] = l.Value
		if l.Name == MetricNameLabelName {
			series.metricName = l.Value
		}
	}
	return series
}

//NameValues returns the names and values, only valid if the seriesIDIsNotSet
func (l *Series) NameValues() (names []string, values []string, ok bool) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.names, l.values, !l.isSeriesIDSetNoLock()
}

func (l *Series) MetricName() string {
	return l.metricName
}

// Get a string representation for hashing and comparison
// This representation is guaranteed to uniquely represent the underlying label
// set, though need not human-readable, or indeed, valid utf-8
func (l *Series) String() string {
	return l.str
}

// Compare returns a comparison int between two Labels
func (l *Series) Compare(b *Series) int {
	return strings.Compare(l.str, b.str)
}

// Equal returns true if two Labels are equal
func (l *Series) Equal(b *Series) bool {
	return l.str == b.str
}

func (l *Series) isSeriesIDSetNoLock() bool {
	return l.seriesID != invalidSeriesID
}

func (l *Series) IsSeriesIDSet() bool {
	l.lock.RLock()
	defer l.lock.RUnlock()

	return l.isSeriesIDSetNoLock()
}

//FinalSizeBytes returns the size in bytes /after/ the seriesID is set
func (l *Series) FinalSizeBytes() uint64 {
	//size is the base size of the struct + the str and metricName strings
	//names and values are not counted since they will be nilled out
	return uint64(unsafe.Sizeof(*l)) + uint64(len(l.str)+len(l.metricName)) // #nosec
}

func (l *Series) GetSeriesID() (SeriesID, SeriesEpoch, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()

	switch l.seriesID {
	case invalidSeriesID:
		return 0, 0, fmt.Errorf("Series id not set")
	case 0:
		return 0, 0, fmt.Errorf("Series id invalid")
	default:
		return l.seriesID, l.epoch, nil
	}
}

//note this has to be idempotent
func (l *Series) SetSeriesID(sid SeriesID, eid SeriesEpoch) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.seriesID = sid
	l.epoch = eid
	l.names = nil
	l.values = nil
}
