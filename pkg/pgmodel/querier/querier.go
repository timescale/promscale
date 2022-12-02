// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package querier

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/rollup"
	"github.com/timescale/promscale/pkg/tenancy"
)

type pgxQuerier struct {
	tools  *queryTools
	schema *rollup.Decider
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
	scrapeInterval time.Duration,
	useRollups bool,
) (Querier, error) {
	querier := &pgxQuerier{
		tools: &queryTools{
			conn:             conn,
			labelsReader:     labelsReader,
			metricTableNames: metricCache,
			exemplarPosCache: exemplarCache,
			rAuth:            rAuth,
		},
	}
	if useRollups {
		decider, err := rollup.NewDecider(context.Background(), conn, scrapeInterval)
		if err != nil {
			return nil, fmt.Errorf("error creating rollups schema decider: %w", err)
		}
		querier.schema = decider
	}
	return querier, nil
}

func (q *pgxQuerier) RemoteReadQuerier(ctx context.Context) RemoteReadQuerier {
	return newQueryRemoteRead(ctx, q)
}

func (q *pgxQuerier) SamplesQuerier(ctx context.Context) SamplesQuerier {
	return newQuerySamples(ctx, q, q.schema)
}

func (q *pgxQuerier) ExemplarsQuerier(ctx context.Context) ExemplarQuerier {
	return newQueryExemplars(ctx, q)
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
