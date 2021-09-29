// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/tenancy"
)

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
		ingestBenchData(t, db)

		// Using a role (prom_modifier or prom_reader) here leads to permission error. Investigate.

		client, err := pgclient.NewClientWithPool(conf, 1, db, tenancy.NewNoopAuthorizer(), false)
		require.NoError(t, err)

		queryable := client.Queryable()

		log.Info("msg", "benchmarking custom queries...")

		for _, q := range customQueries {
			executable := getQuery(t, queryEngine, queryable, q)

			b.Run(fmt.Sprintf("custom query: %s", q.name), func(b *testing.B) {
				b.ReportAllocs()
				_ = executable.Exec(context.Background())
			})
		}

		log.Info("msg", "benchmarking real queries...")

		for _, q := range realQueries {
			executable := getQuery(t, queryEngine, queryable, q)

			b.Run(fmt.Sprintf("real query: %s", q.name), func(b *testing.B) {
				b.ReportAllocs()
				// todo: should we check result.String() form?
				_ = executable.Exec(context.Background())
			})
		}
	})
}

func getQuery(t testing.TB, engine *promql.Engine, queryable promql.Queryable, q testQuery) promql.Query {
	var (
		err        error
		executable promql.Query
	)

	step, err := api.ParseDuration("1000")
	require.NoError(t, err, q.name)

	start := realStart
	end := realEnd
	if q.isRule {
		start = realStartRule
		end = realEndRule
	}

	executable, err = engine.NewRangeQuery(queryable, q.expression, timestamp.Time(start), timestamp.Time(end), step)
	require.NoError(t, err, q.name)
	return executable
}
