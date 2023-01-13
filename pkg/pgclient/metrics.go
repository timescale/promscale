// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgclient

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/timescale/promscale/pkg/util"
)

func initMetrics(r prometheus.Registerer, writerPool, readerPool, maintPool *pgxpool.Pool) {
	writerAcquired := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "sql_database",
			Name:        "active_connections",
			Help:        "Number of connections currently acquired from the pool.",
			ConstLabels: map[string]string{"pool": "writer"},
		}, func() float64 {
			if writerPool == nil {
				// readonly mode.
				return 0
			}
			return float64(writerPool.Stat().AcquiredConns())
		},
	)
	writerActive := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "sql_database",
			Name:        "total_connections",
			Help:        "Number of connections currently active in the pool.",
			ConstLabels: map[string]string{"pool": "writer"},
		}, func() float64 {
			if writerPool == nil {
				// readonly mode.
				return 0
			}
			return float64(writerPool.Stat().TotalConns())
		},
	)
	readerAcquired := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "sql_database",
			Name:        "active_connections",
			Help:        "Number of connections currently acquired from the pool.",
			ConstLabels: map[string]string{"pool": "reader"},
		}, func() float64 {
			return float64(readerPool.Stat().AcquiredConns())
		},
	)
	readerActive := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "sql_database",
			Name:        "total_connections",
			Help:        "Number of connections currently active in the pool.",
			ConstLabels: map[string]string{"pool": "reader"},
		}, func() float64 {
			return float64(readerPool.Stat().TotalConns())
		},
	)
	maintAcquired := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "sql_database",
			Name:        "active_connections",
			Help:        "Number of connections currently acquired from the pool.",
			ConstLabels: map[string]string{"pool": "maint"},
		}, func() float64 {
			if maintPool == nil {
				// readonly mode.
				return 0
			}
			return float64(maintPool.Stat().AcquiredConns())
		},
	)
	maintActive := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "sql_database",
			Name:        "total_connections",
			Help:        "Number of connections currently active in the pool.",
			ConstLabels: map[string]string{"pool": "maint"},
		}, func() float64 {
			if maintPool == nil {
				// readonly mode.
				return 0
			}
			return float64(maintPool.Stat().TotalConns())
		},
	)

	r.MustRegister(writerAcquired, writerActive, readerAcquired, readerActive, maintAcquired, maintActive)
}
