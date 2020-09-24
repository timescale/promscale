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
	Query(*prompb.Query) ([]*prompb.TimeSeries, error)
	Select(mint int64, maxt int64, sortSeries bool, hints *storage.SelectHints, path []parser.Node, ms ...*labels.Matcher) (storage.SeriesSet, parser.Node)
	LabelNames() ([]string, error)
	LabelValues(labelName string) ([]string, error)
	NumCachedLabels() int
	LabelsCacheCapacity() int
}

//HealthChecker allows checking for proper operations.
type HealthChecker interface {
	HealthCheck() error
}

// QueryHealthChecker can query and check its own health.
type QueryHealthChecker interface {
	Querier
	HealthChecker
}

// DBReader reads data from the database.
type DBReader struct {
	db QueryHealthChecker
}

func (r *DBReader) GetQuerier() QueryHealthChecker {
	return r.db
}

func (r *DBReader) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	if req == nil {
		return nil, nil
	}

	resp := prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(req.Queries)),
	}

	for i, q := range req.Queries {
		tts, err := r.db.Query(q)
		if err != nil {
			return nil, err
		}
		resp.Results[i] = &prompb.QueryResult{
			Timeseries: tts,
		}
	}

	return &resp, nil
}

// HealthCheck checks that the reader is properly connected
func (r *DBReader) HealthCheck() error {
	return r.db.HealthCheck()
}
