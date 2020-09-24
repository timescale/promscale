package api

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

const tickInterval = time.Second

var metrics *Metrics

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
	InvalidReadReqs     prometheus.Counter
	InvalidWriteReqs    prometheus.Counter
	HTTPRequestDuration *prometheus.HistogramVec
}

func InitMetrics() *Metrics {
	if metrics != nil {
		return metrics
	}
	metrics = &Metrics{
		LeaderGauge: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Name:      "current_leader",
				Help:      "Shows current election leader status",
			},
		),
		ReceivedSamples: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: util.PromNamespace,
				Name:      "received_samples_total",
				Help:      "Total number of received samples.",
			},
		),
		FailedSamples: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: util.PromNamespace,
				Name:      "failed_samples_total",
				Help:      "Total number of processed samples which failed on send to remote storage.",
			},
		),
		SentSamples: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: util.PromNamespace,
				Name:      "sent_samples_total",
				Help:      "Total number of processed samples sent to remote storage.",
			},
		),
		SentBatchDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: util.PromNamespace,
				Name:      "sent_batch_duration_seconds",
				Help:      "Duration of sample batch send calls to the remote storage.",
				Buckets:   prometheus.DefBuckets,
			},
		),
		WriteThroughput:     util.NewThroughputCalc(tickInterval),
		LastRequestUnixNano: time.Now().UnixNano(),
		QueryBatchDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: util.PromNamespace,
				Name:      "query_batch_duration_seconds",
				Help:      "Duration of query batch read calls to the remote storage.",
				Buckets:   prometheus.DefBuckets,
			},
		),
		FailedQueries: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: util.PromNamespace,
				Name:      "failed_queries_total",
				Help:      "Total number of queries which failed on send to remote storage.",
			},
		),
		InvalidReadReqs: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: util.PromNamespace,
				Name:      "invalid_read_requests",
				Help:      "Total number of remote read requests with invalid metadata.",
			},
		),
		InvalidWriteReqs: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: util.PromNamespace,
				Name:      "invalid_write_requests",
				Help:      "Total number of remote write requests with invalid metadata.",
			},
		),
		ReceivedQueries: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: util.PromNamespace,
				Name:      "received_queries_total",
				Help:      "Total number of received queries.",
			},
		),
		HTTPRequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: util.PromNamespace,
				Name:      "http_request_duration_ms",
				Help:      "Duration of HTTP request in milliseconds",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"path"},
		),
	}
	prometheus.MustRegister(
		metrics.LeaderGauge,
		metrics.ReceivedSamples,
		metrics.ReceivedQueries,
		metrics.SentSamples,
		metrics.FailedSamples,
		metrics.FailedQueries,
		metrics.InvalidReadReqs,
		metrics.InvalidWriteReqs,
		metrics.SentBatchDuration,
		metrics.QueryBatchDuration,
		metrics.HTTPRequestDuration,
	)
	metrics.WriteThroughput.Start()

	return metrics
}
