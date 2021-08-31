// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/tenancy"
	"github.com/timescale/promscale/pkg/tests/common"
)

type testQuery struct {
	name       string
	expression string
	start, end int64
}

var startTimeOffset = common.StartTime + 24*30*time.Hour.Milliseconds() // start time + 1 month.

var benchmarkableInstantQueries = []testQuery{
	// Real-world inspired PromQL queries.
	// References:
	// 1. https://www.robustperception.io/common-query-patterns-in-promql
	// 2. https://github.com/infinityworks/prometheus-example-queries
	{
		name:       "vs: simple gauge 1",
		expression: "one_gauge_metric",
		start:      startTimeOffset,
		end:        startTimeOffset,
	},
	{
		name:       "vs: simple gauge 2",
		expression: "two_gauge_metric",
		start:      startTimeOffset,
		end:        startTimeOffset,
	},
	{
		name:       "utilization percentage",
		expression: "100 * (1 - avg by(instance)(irate(one_gauge_metric{foo='bar'}[5m])))",
		start:      startTimeOffset,
		end:        startTimeOffset,
	},
	{
		name: "event occurange percentage (like rate of errors)",
		expression: `
	rate(one_counter_total[5m]) * 50
> on(job, instance)
	rate(two_counter_total[5m])`,
		start: startTimeOffset,
		end:   startTimeOffset,
	},
	{
		name:       "percentile calculation 1",
		expression: `histogram_quantile(0.9, one_histogram_bucket)`,
		start:      startTimeOffset,
		end:        startTimeOffset,
	},
	{
		name: "percentile calculation 2",
		expression: `
	histogram_quantile(0.9, rate(one_histogram_bucket{job="benchmark"}[10m])) > 0.05
and
	rate(one_histogram_count[10m]) > 1`,
		start: startTimeOffset,
		end:   startTimeOffset,
	},
}

func BenchmarkQuerier(b *testing.B) {
	startContainer()
	defer terminateContainer()

	conf := &pgclient.Config{
		CacheConfig:             cache.DefaultConfig,
		WriteConnectionsPerProc: 4,
		MaxConnections:          -1,
	}

	queryEngine := getQueryEngine(b)

	withDB(b, benchDatabase, func(db *pgxpool.Pool, t testing.TB) {
		prepareContainer(b)

		// Using a role (prom_modifier or prom_reader) here leads to permission error. Investigate.

		client, err := pgclient.NewClientWithPool(conf, 1, db, tenancy.NewNoopAuthorizer(), false)
		require.NoError(t, err)

		queryable := client.Queryable()

		for _, q := range benchmarkableInstantQueries {
			executable := getQuery(t, q, queryEngine, queryable)
			require.NoError(t, err, q.name)

			var result *promql.Result
			b.Run(q.name, func(b *testing.B) {
				result = executable.Exec(context.Background())
			})
			fmt.Println("result", result.String())
		}
	})
}

func getQuery(t testing.TB, q testQuery, engine *promql.Engine, queryable promql.Queryable) promql.Query {
	var (
		err        error
		executable promql.Query
	)

	step, err := api.ParseDuration("10000")
	require.NoError(t, err)

	if q.start == q.end {
		// Instant query.
		executable, err = engine.NewInstantQuery(queryable, q.expression, timestamp.Time(q.start))
	} else {
		executable, err = engine.NewRangeQuery(queryable, q.expression, timestamp.Time(q.start), timestamp.Time(q.end), step)
	}
	require.NoError(t, err)
	return executable
}
