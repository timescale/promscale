// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/prompb"
)

// Reader reads the data based on the provided read request.
type Reader interface {
	Read(*prompb.ReadRequest) (*prompb.ReadResponse, error)
}

// Querier queries the data using the provided query data and returns the
// matching timeseries.
type Querier interface {
	// Query returns resulting timeseries for a query.
	Query(*prompb.Query) ([]*prompb.TimeSeries, error)
	// Select returns a series set that matches the supplied query parameters.
	Select(mint int64, maxt int64, sortSeries bool, hints *storage.SelectHints, path []parser.Node, ms ...*labels.Matcher) (storage.SeriesSet, parser.Node)
	// LabelNames returns all the distinct label names in the system.
	LabelNames() ([]string, error)
	// LabelValues returns all the distinct values for a given label name.
	LabelValues(labelName string) ([]string, error)
}
