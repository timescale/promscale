package database

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/tests/testsupport"
	"github.com/timescale/promscale/pkg/util"
)

func TestCustomPollConfig(t *testing.T) {
	engine := &metricsEngineImpl{
		conn: testsupport.MockPgxConn{},
		ctx:  context.Background(),
		metrics: []metricQueryWrap{
			{
				metrics: counters(
					prometheus.CounterOpts{
						Namespace: util.PromNamespace,
						Subsystem: "sql_database",
						Name:      "test",
						Help:      "test",
					},
				),
				customPollConfig: updateAtMostEvery(1 * time.Second),
				query:            "SELECT 1",
			},
		},
	}

	testStart := time.Now()
	testMetricPollConfig := &engine.metrics[0].customPollConfig

	if err := engine.Update(); err != nil {
		t.Fail()
	}
	require.False(t, testMetricPollConfig.lastUpdate.After(testStart))

	require.Eventually(t, func() bool {
		if err := engine.Update(); err != nil {
			t.Fail()
		}
		return testMetricPollConfig.lastUpdate.After(testStart)
	}, 5*time.Second, 1*time.Second)

}
