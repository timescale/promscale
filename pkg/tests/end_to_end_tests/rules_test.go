// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	prom_rules "github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/prompb"

	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/query"
	"github.com/timescale/promscale/pkg/rules"
	"github.com/timescale/promscale/pkg/tenancy"
)

const (
	RecordingRulesEvalConfigPath  = "../testdata/rules/config.recording_rules_eval.yaml"
	EmptyRecordingRulesConfigPath = "../testdata/rules/config.empty_rules.yaml"
)

func TestRecordingRulesEval(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		conf := &pgclient.Config{
			CacheConfig:    cache.DefaultConfig,
			MaxConnections: -1,
		}

		pgClient, err := pgclient.NewClientWithPool(prometheus.NewRegistry(), conf, 1, db, db, nil, tenancy.NewNoopAuthorizer(), false)
		require.NoError(t, err)
		defer pgClient.Close()
		err = pgClient.InitPromQLEngine(&query.Config{
			MaxQueryTimeout:      query.DefaultQueryTimeout,
			EnabledFeatureMap:    map[string]struct{}{"promql-at-modifier": {}},
			SubQueryStepInterval: query.DefaultSubqueryStepInterval,
			LookBackDelta:        query.DefaultLookBackDelta,
			MaxSamples:           query.DefaultMaxSamples,
			MaxPointsPerTs:       11000,
		})
		require.NoError(t, err)

		ingestor := pgClient.Inserter()
		ts := tsToSeconds(generateSmallTimeseries(), time.Second) // Converts ts of samples into seconds.
		_, _, err = ingestor.IngestMetrics(context.Background(), newWriteRequestWithTs(ts))
		require.NoError(t, err)

		rulesCfg := rules.DefaultConfig
		rulesCfg.PrometheusConfigAddress = EmptyRecordingRulesConfigPath // Start with empty rules.

		require.NoError(t, rules.Validate(&rulesCfg))
		require.False(t, rulesCfg.ContainsRules())

		ruleCtx, stopRuler := context.WithCancel(context.Background())
		defer stopRuler()

		manager, reloadRules, err := rules.NewManager(ruleCtx, prometheus.NewRegistry(), pgClient, &rulesCfg)
		require.NoError(t, err)

		require.NotNil(t, rulesCfg.PrometheusConfig)
		require.NoError(t, reloadRules())
		require.False(t, rulesCfg.ContainsRules())

		ruleGroups := manager.RuleGroups()
		require.Equal(t, 0, len(ruleGroups))

		manager.WithPostRulesProcess(func(*prom_rules.Group, time.Time, log.Logger) error {
			defer func() {
				stopRuler() // Shuts down the manager.Run() as soon as the test completes.
			}()
			// Check if recording rule as a metric exists in metric catalog table.
			var exists bool
			err := db.QueryRow(context.Background(), "select count(*)>0 from _prom_catalog.metric where metric_name = 'test_rule'").Scan(&exists)
			require.NoError(t, err)
			require.True(t, exists)

			// Check if the sum is right.
			expected := 0.9
			var value float64
			err = db.QueryRow(context.Background(), "select value from prom_data.test_rule order by time limit 1").Scan(&value)
			require.NoError(t, err)
			require.Equal(t, expected, value)

			return nil
		})
		// Reload with configuration file that contains some rules.
		rulesCfg.PrometheusConfigAddress = RecordingRulesEvalConfigPath
		require.NoError(t, reloadRules())
		require.True(t, rulesCfg.ContainsRules())
		require.Equal(t, 1, len(manager.RuleGroups()))

		require.NoError(t, manager.Run(), "error running rules manager")
	})
}

func tsToSeconds(ts []prompb.TimeSeries, multiplier time.Duration) []prompb.TimeSeries {
	for i := range ts {
		for j := range ts[i].Samples {
			ts[i].Samples[j].Timestamp *= multiplier.Milliseconds()
		}
	}
	return ts
}
