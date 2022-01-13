// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package querier

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/tenancy"
)

type pgxQuerier struct {
	tools *queryTools
}

var _ Querier = (*pgxQuerier)(nil)

// NewQuerier returns a new pgxQuerier that reads from PostgreSQL using PGX
// and caches metric table names and label sets using the supplied caches.
func NewQuerier(
	conn pgxconn.PgxConn,
	metricCache cache.MetricCache,
	labelsReader lreader.LabelsReader,
	exemplarCache cache.PositionCache,
	rAuth tenancy.ReadAuthorizer,
) Querier {
	querier := &pgxQuerier{
		tools: &queryTools{
			conn:             conn,
			labelsReader:     labelsReader,
			metricTableNames: metricCache,
			exemplarPosCache: exemplarCache,
			rAuth:            rAuth,
		},
	}
	return querier
}

func (q *pgxQuerier) SamplesQuerier() SamplesQuerier {
	return newQuerySamples(q)
}

func (q *pgxQuerier) ExemplarsQuerier(ctx context.Context) ExemplarQuerier {
	return newQueryExemplars(q)
}

// Query implements the Querier interface. It is the entry point for
// remote-storage queries.
func (q *pgxQuerier) Query(query *prompb.Query) ([]*prompb.TimeSeries, error) {
	if query == nil {
		return []*prompb.TimeSeries{}, nil
	}

	matchers, err := fromLabelMatchers(query.Matchers)
	if err != nil {
		return nil, err
	}

	qrySamples := newQuerySamples(q)
	sampleRows, _, err := qrySamples.fetchSamplesRows(query.StartTimestampMs, query.EndTimestampMs, nil, nil, nil, matchers)
	if err != nil {
		return nil, err
	}
	results, err := buildTimeSeries(sampleRows, q.tools.labelsReader)
	if err != nil {
		return nil, fmt.Errorf("building time-series: %w", err)
	}
	return results, nil
}

// errorSeriesSet represents an error result in a form of a series set.
// This behavior is inherited from Prometheus codebase.
type errorSeriesSet struct {
	err error
}

func (errorSeriesSet) Next() bool                   { return false }
func (errorSeriesSet) At() storage.Series           { return nil }
func (e errorSeriesSet) Err() error                 { return e.err }
func (e errorSeriesSet) Warnings() storage.Warnings { return nil }
func (e errorSeriesSet) Close()                     {}

type labelQuerier interface {
	LabelsForIdMap(idMap map[int64]labels.Label) (err error)
}
