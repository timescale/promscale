// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/timescale/promscale/pkg/ewma"
	"github.com/timescale/promscale/pkg/log"
)

const (
	// reportThreshold is a value above which the warn would be logged. This value is
	// the value of the ratio between incoming batches vs outgoing batches.
	reportRatioThreshold = 3
	checkRatioInterval   = time.Minute

	// Duration warnings constants.
	reportDurationThreshold = time.Minute
)

// throughtputWatcher is a light weight samples batch watching type, that serves as a watching
// routine and keeps the track of incoming write batches. It keeps a ratio of
// incoming samples vs outgoing samples and warn about low throughput to the user
// which might be due to external causes like network latency, resources allocated, etc.
type batchWatcher struct {
	// Warn based on ratio.
	incomingBatches *ewma.Rate
	outgoingBatches *ewma.Rate

	// Warn based on ingestion duration.
	shouldReportLastDuration atomic.Value
	lastIngestionDuration    atomic.Value

	stop chan struct{}
}

var (
	watcher  *batchWatcher
	tWatcher = new(sync.Once)
)

func runBatchWatcher(stop chan struct{}) {
	tWatcher.Do(func() {
		watcher := newThroughputWatcher(stop)
		go watcher.watch()
	})
}

func newThroughputWatcher(stop chan struct{}) *batchWatcher {
	watcher = &batchWatcher{
		incomingBatches: ewma.NewEWMARate(1, checkRatioInterval),
		outgoingBatches: ewma.NewEWMARate(1, checkRatioInterval),
		stop:            stop,
	}
	watcher.shouldReportLastDuration.Store(false)
	return watcher
}

// watchBatches watches the ratio between incomingBatches and outgoingBatches every checkRatioInterval.
func (w *batchWatcher) watch() {
	t := time.NewTicker(checkRatioInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-w.stop:
			return
		}
		w.outgoingBatches.Tick()
		w.incomingBatches.Tick()
		outgoingRate := w.outgoingBatches.Rate()
		incomingRate := w.incomingBatches.Rate()
		if outgoingRate > 0 {
			r := incomingRate / outgoingRate
			if r > reportRatioThreshold {
				warnHighRatio(r, incomingRate, outgoingRate)
			}
		}
		if w.shouldReportLastDuration.Load().(bool) {
			warnSlowIngestion(w.lastIngestionDuration.Load().(time.Duration))
			w.shouldReportLastDuration.Store(false)
		}
	}
}

func warnHighRatio(ratio float64, inRate, outRate float64) {
	log.Warn("msg", "[WARNING] Incoming samples rate much higher than the rate of samples being saved. "+
		"This may happen due to poor network latency, not enough resources allocated to Promscale, or some other performance-related reason. "+
		"Please tune your system or reach out to the Promscale team.",
		"incoming-rate", inRate, "outgoing-rate", outRate,
		"throughput-ratio", fmt.Sprintf("%.2f", ratio), "threshold", reportRatioThreshold)
}

func warnSlowIngestion(duration time.Duration) {
	log.Warn("msg", "[WARNING] Ingestion is a very long time", "duration",
		duration.String(), "threshold", reportDurationThreshold.String())
}

func reportIncomingBatch(size uint64) {
	watcher.incomingBatches.Incr(int64(size))
}

func reportOutgoingBatch(size uint64) {
	watcher.outgoingBatches.Incr(int64(size))
}

func reportBatchProcessingTime(inTime time.Time) {
	d := time.Since(inTime)
	if d.Seconds() > reportDurationThreshold.Seconds() {
		watcher.shouldReportLastDuration.Store(true)
		watcher.lastIngestionDuration.Store(d)
	}
}
