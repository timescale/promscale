// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"fmt"
	"strconv"

	"github.com/timescale/promscale/pkg/prompb"
)

const (
	MetricNameLabelName = "__name__"
)

var (
	ErrNoMetricName = fmt.Errorf("metric name missing")
)

// SeriesID represents a globally unique id for the series. This should be equivalent
// to the PostgreSQL type in the series table (currently BIGINT).
type SeriesID int64

func (s SeriesID) String() string {
	return strconv.FormatInt(int64(s), 10)
}

// inserter is responsible for inserting label, series and data into the storage.
type inserter interface {
	InsertNewData(rows map[string][]samplesInfo) (uint64, error)
	CompleteMetricCreation() error
	Close()
}

// SeriesCache provides a caching mechanism for labels and series.
type SeriesCache interface {
	GetSeries(lset Labels) (SeriesID, error)
	SetSeries(lset Labels, id SeriesID) error
	NumElements() int
	Capacity() int
}

type samplesInfo struct {
	labels   *Labels
	seriesID SeriesID
	samples  []prompb.Sample
}
