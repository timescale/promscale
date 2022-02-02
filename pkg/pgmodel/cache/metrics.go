// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
	"sync/atomic"
	"time"
)

var (
	Enabled = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "cache",
			Name:      "enabled",
			Help:      "Cache is enalbed or not.",
		},
		[]string{"subsystem", "name"}, // type => ["trace" or "metric"] and name => name of the cache i.e., metric cache, series cache, etc.
	)
	capacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "cache",
			Name:      "capacity",
			Help:      "Cache is enabled or not.",
		},
		[]string{"subsystem", "name"},
	)
	sizeBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "cache",
			Name:      "size_bytes",
			Help:      "Cache size in bytes.",
		},
		[]string{"subsystem", "name"},
	)
	evictionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "cache",
			Name:      "evictions_total",
			Help:      "Total evictions in a clockcache.",
		},
		[]string{"subsystem", "name"},
	)
)

func init() {
	prometheus.MustRegister(
		Enabled,
		capacity,
		sizeBytes,
		evictionsTotal,
	)
	funcs.Store([]updateFunc{})
	go metricsUpdater()
}

const (
	Cap = iota
	Size
	Evict
)

type MetricKind uint8

type updateFunc struct {
	typ    MetricKind
	update func(metric prometheus.Collector)
}

var funcs atomic.Value

// RegisterUpdateFunc updates some metrics like SizeBytes and Capacity every 30 secs.
// Earlier these were done via supplying a func to NewGaugeFunc that called that func
// when prometheus scraped. But now we have labels, and we have to use NewGaugeVec
// which does not allow to implement a func. Hence, we have to choose the routine way
// in order to update these metrics.
func RegisterUpdateFunc(kind MetricKind, update func(metric prometheus.Collector)) {
	l := funcs.Load().([]updateFunc)
	switch kind {
	case Cap:
		l = append(l, updateFunc{kind, update})
	case Size:
		l = append(l, updateFunc{kind, update})
	case Evict:
		l = append(l, updateFunc{kind, update})
	default:
		panic(fmt.Sprintf("invalid kind %d", kind))
	}
	funcs.Store(l)
}

func metricsUpdater() {
	update := time.NewTicker(time.Second * 10)
	defer update.Stop()
	for range update.C {
		if len(funcs.Load().([]updateFunc)) == 0 {
			continue
		}
		needUpdate := funcs.Load().([]updateFunc)
		for _, f := range needUpdate {
			switch f.typ {
			case Cap:
				f.update(capacity)
			case Size:
				f.update(sizeBytes)
			case Evict:
				f.update(evictionsTotal)
			}
		}
	}
}
