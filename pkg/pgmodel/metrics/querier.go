package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

var (
	QueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "query",
			Name:      "duration_seconds",
			Help:      "Time taken to respond to the query/query batch.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 50, 100, 250, 500, 1000, 2500},
		}, []string{"type", "handler"},
	)
	Query = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "query",
			Name:      "requests_total",
			Help:      "Number of query requests to Promscale.",
		}, []string{"type", "handler", "code"},
	)
	RemoteReadReceivedQueries = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "query",
			Name:        "remote_read",
			Help:        "Number of remote-read queries received.",
			ConstLabels: map[string]string{"type": "metric", "handler": "/read"},
		},
	)
)

func init() {
	prometheus.MustRegister(QueryDuration, Query, RemoteReadReceivedQueries)
}
