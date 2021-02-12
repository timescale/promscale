// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/timescale/promscale/pkg/log"
)

const (
	// reportThreshold is a value above which the warn would be logged. This value is
	// the value of the ratio between incoming batches vs outgoing batches.
	reportRatioThreshold = 3
	refreshIteration     = 10 // This means that values of watcher.incomingBatches and watcher.outgoingBatches gets renewed every (refreshIteration * checkThroughputInterval).
	checkRatioInterval   = time.Minute

	// Duration warnings constants.
	reportDurationThreshold        = time.Minute
	checkIngestionDurationInterval = time.Minute
)

// throughtputWatcher is a light weight samples batch watching type, that serves as a watching
// routine and keeps the track of incoming write batches. It keeps a ratio of
// incoming samples vs outgoing samples and warn about low throughput to the user
// which might be due to external causes like network latency, resources allocated, etc.
//
// We watch the batches than the samples in total, since watching batching is fast
// and does the same purpose of watching samples, but on a higher-level.
type throughtputWatcher struct {
	// Warn based on ratio.
	incomingBatches uint64
	outgoingBatches uint64

	// Warn based on ingestion duration.
	shouldReportLastDuration atomic.Value
	lastIngestionDuration    atomic.Value

	stop chan struct{}
}

var watcher *throughtputWatcher

func runThroughputWatcher(stop chan struct{}) {
	watcher = &throughtputWatcher{
		incomingBatches: 0,
		outgoingBatches: 0,
		stop:            stop,
	}
	watcher.shouldReportLastDuration.Store(false)
	go watcher.watch()
	go watcher.watchTime()
}

// watch watches the ratio between incomingBatches and outgoingBatches every checkThroughputInterval.
func (w *throughtputWatcher) watch() {
	var (
		itr uint8
		t   = time.NewTicker(checkRatioInterval)
	)
	for range t.C {
		select {
		case <-w.stop:
			return
		default:
		}
		itr++
		if w.outgoingBatches > 1 {
			if ratio := float64(atomic.LoadUint64(&w.incomingBatches)) / float64(atomic.LoadUint64(&w.outgoingBatches)); ratio > reportRatioThreshold {
				w.warn(ratio)
			}
		}
		if itr > refreshIteration {
			// refresh every 'refreshIteration' of outgoing ratio checks.
			atomic.StoreUint64(&watcher.incomingBatches, 0)
			atomic.StoreUint64(&watcher.outgoingBatches, 0)
		}
	}
}

func (w *throughtputWatcher) warn(ratio float64) {
	log.Warn("msg", "[WARNING] Incoming samples rate higher than outgoing samples. This may happen due to poor network latency, "+
		"less resources allocated, or few concurrent writes (shards) from Prometheus. Please tune your resource to improve performance. ",
		"Throughput ratio", fmt.Sprintf("%.2f", ratio), "Ratio threshold", reportRatioThreshold)
}

func (w *throughtputWatcher) watchTime() {
	t := time.NewTicker(checkIngestionDurationInterval)
	for range t.C {
		select {
		case <-w.stop:
			return
		default:
		}
		if w.shouldReportLastDuration.Load().(bool) {
			log.Warn("msg", "[WARNING] Ingestion is taking too long", "ingest duration",
				w.lastIngestionDuration.Load().(time.Duration).String(), "threshold", reportDurationThreshold.String())
			w.shouldReportLastDuration.Store(false)
		}
	}
}

func incBatch(size uint64) {
	atomic.AddUint64(&watcher.incomingBatches, size)
}

func decBatch(size uint64) {
	atomic.AddUint64(&watcher.outgoingBatches, size)
}

func registerIngestionDuration(inTime time.Time) {
	d := time.Since(inTime)
	if d.Seconds() > reportDurationThreshold.Seconds() {
		watcher.shouldReportLastDuration.Store(true)
		watcher.lastIngestionDuration.Store(d)
	}
}
