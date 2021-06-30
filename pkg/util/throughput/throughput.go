// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package throughput

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/timescale/promscale/pkg/ewma"
	"github.com/timescale/promscale/pkg/log"
)

var (
	watcher           sync.Once
	throughputWatcher *throughputCalc
)

// InitWatcher initializes the watcher that watches the throughput of incoming data.
func InitWatcher(calculateEvery time.Duration) {
	if calculateEvery == 0 {
		// Watching throughput is disabled.
		return
	}
	watcher.Do(func() {
		throughputWatcher = newThroughputCal(calculateEvery)
		go throughputWatcher.run()
	})
}

type throughputCalc struct {
	every    time.Duration
	maxTs    int64
	samples  *ewma.Rate
	metadata *ewma.Rate
}

func newThroughputCal(every time.Duration) *throughputCalc {
	return &throughputCalc{
		every:    every,
		maxTs:    0,
		samples:  ewma.NewEWMARate(1, every),
		metadata: ewma.NewEWMARate(1, every),
	}
}

func (tc *throughputCalc) run() {
	t := time.NewTicker(tc.every)
	for range t.C {
		tc.samples.Tick()
		tc.metadata.Tick()
		samplesRate := tc.samples.Rate()
		metadataRate := tc.metadata.Rate()
		if samplesRate+metadataRate == 0 {
			continue
		}
		ts := timestamp.Time(atomic.LoadInt64(&tc.maxTs))
		log.Info("msg", "Samples write throughput", "samples/sec", int(samplesRate), "metadata/sec", int(metadataRate), "max-sent-ts", ts)
	}
}

func ReportDataProcessed(maxTs int64, numSamples, numMetadata uint64) {
	if throughputWatcher == nil {
		// Throughput watcher is disabled.
		return
	}
	throughputWatcher.samples.Incr(int64(numSamples))
	throughputWatcher.metadata.Incr(int64(numMetadata))
	if maxTs != 0 && atomic.LoadInt64(&throughputWatcher.maxTs) < maxTs {
		atomic.StoreInt64(&throughputWatcher.maxTs, maxTs)
	}
}
