// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

func TestExtractMetricValue(t *testing.T) {
	metric := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "test", Name: "extraction"})

	metric.Set(164)

	value, err := extractMetricValue(metric)
	require.NoError(t, err)
	require.Equal(t, float64(164), value)

	wrongMetric := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "test", Name: "wrong", Buckets: prometheus.DefBuckets})

	wrongMetric.Observe(164)
	_, err = extractMetricValue(wrongMetric)
	require.Error(t, err)
}

func TestRegisterMetric(t *testing.T) {
	metric := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "test", Name: "extraction"})

	engine := &telemetryEngine{}
	require.NoError(t, engine.RegisterMetric("some_stats", metric))
	require.Len(t, engine.metrics, 1)

	wrongMetric := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "test", Name: "wrong", Buckets: prometheus.DefBuckets})
	wrongMetric.Observe(164)

	require.Error(t, engine.RegisterMetric("some_wrong_stats", wrongMetric))
	require.Len(t, engine.metrics, 1)
}

func TestEngineClosePlain(t *testing.T) {
	engine := &telemetryEngine{}
	require.NoError(t, engine.Close())

	engine.StartRoutineAsync()
	require.NoError(t, engine.Close())

	engine.isHouseKeeper = true
	lock, err := util.NewPgAdvisoryLock(0, "")
	require.NoError(t, err)
	engine.housekeeperLock = lock
	require.NoError(t, engine.DoHouseKeepingAsync())
	require.NoError(t, engine.Close())
}
