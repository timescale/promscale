// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/timescale/promscale/pkg/prompb"
)

// SeriesID represents a globally unique id for the series. This should be equivalent
// to the PostgreSQL type in the series table (currently BIGINT).
type SeriesID int64

const unsetSeriesID = -1

func (s SeriesID) String() string {
	return strconv.FormatInt(int64(s), 10)
}

//Epoch represents the series epoch
type SeriesEpoch int64

const UnsetSeriesEpoch = -1

// Series stores a labels.Series in its canonical string representation
type Series struct {
	Names      []string
	Values     []string
	MetricName string
	str        string
	seriesID   SeriesID
	epoch      SeriesEpoch
}

func NewSeries(key string, labelPairs []prompb.Label) *Series {
	series := &Series{
		Names:    make([]string, len(labelPairs)),
		Values:   make([]string, len(labelPairs)),
		str:      key,
		seriesID: unsetSeriesID,
		epoch:    UnsetSeriesEpoch,
	}
	for i, l := range labelPairs {
		series.Names[i] = l.Name
		series.Values[i] = l.Value
		if l.Name == MetricNameLabelName {
			series.MetricName = l.Value
		}
	}
	return series
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

func (l *Series) IsSeriesIDSet() bool {
	return l.seriesID != unsetSeriesID
}

func (l *Series) GetSeriesID() (SeriesID, SeriesEpoch, error) {
	switch l.seriesID {
	case unsetSeriesID:
		return 0, 0, fmt.Errorf("Series id not set")
	case 0:
		return 0, 0, fmt.Errorf("Series id invalid")
	default:
		return l.seriesID, l.epoch, nil
	}
}

//note this has to be idempotent
func (l *Series) SetSeriesID(sid SeriesID, eid SeriesEpoch) {
	//TODO: Unset l.Names and l.Values, no longer used
	l.seriesID = sid
	l.epoch = eid
}
