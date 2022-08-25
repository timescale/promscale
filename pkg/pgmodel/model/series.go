// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"github.com/timescale/promscale/pkg/prompb"
	"strconv"
	"sync"
)

// SeriesID represents a globally unique id for the series. This should be equivalent
// to the PostgreSQL type in the series table (currently BIGINT).
type SeriesID int64

func (s SeriesID) String() string {
	return strconv.FormatInt(int64(s), 10)
}

// SeriesEpoch represents the series epoch
type SeriesEpoch struct {
	time int64
}

func NewSeriesEpoch(epochTime int64) *SeriesEpoch {
	return &SeriesEpoch{
		time: epochTime,
	}
}

func (s *SeriesEpoch) After(o *SeriesEpoch) bool {
	return s.time > o.time
}

func (s *SeriesEpoch) Time() int64 {
	return s.time
}

// StoredSeries represents a series which has been stored in the database
type StoredSeries struct {
	seriesId SeriesID
	epoch    *SeriesEpoch
}

func NewStoredSeries(seriesId SeriesID, epoch *SeriesEpoch) *StoredSeries {
	return &StoredSeries{seriesId, epoch}
}

func (s *StoredSeries) SeriesID() SeriesID {
	return s.seriesId
}

func (s *StoredSeries) Epoch() *SeriesEpoch {
	return s.epoch
}

// UnresolvedSeries represents a series which we do not have a DB id for. This
// could be either because the series doesn't exist yet, or because we haven't
// cached the ID yet.
type UnresolvedSeries struct {
	names  []string
	values []string

	metricName string
	lock       sync.RWMutex
	resolved   bool
}

func NewUnresolvedSeries(labelPairs []prompb.Label) *UnresolvedSeries {
	series := &UnresolvedSeries{
		names:  make([]string, len(labelPairs)),
		values: make([]string, len(labelPairs)),
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

func (l *UnresolvedSeries) MetricName() string {
	return l.metricName
}

// NameValues returns the names and values
func (l *UnresolvedSeries) NameValues() (names []string, values []string) {
	return l.names, l.values
}

func (l *UnresolvedSeries) Resolve() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.resolved = true
}
func (l *UnresolvedSeries) IsResolved() bool {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.resolved
}
