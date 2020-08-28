package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/timescale-prometheus/pkg/util"
)

type Metrics struct {
	// Using the first word in struct to ensure proper alignment in 32-bit systems.
	// Reference: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	LastRequestUnixNano int64
	LeaderGauge         prometheus.Gauge
	ReceivedSamples     prometheus.Counter
	FailedSamples       prometheus.Counter
	SentSamples         prometheus.Counter
	SentBatchDuration   prometheus.Histogram
	WriteThroughput     *util.ThroughputCalc
	ReceivedQueries     prometheus.Counter
	FailedQueries       prometheus.Counter
	QueryBatchDuration  prometheus.Histogram
	CachedMetricNames   prometheus.CounterFunc
	CachedLabels        prometheus.CounterFunc
	InvalidReadReqs     prometheus.Counter
	InvalidWriteReqs    prometheus.Counter
}
