// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package throughput

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
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
	every time.Duration

	// Metrics telemetry.
	samples          *ewma.Rate
	metadata         *ewma.Rate
	metricsMaxSentTs int64

	// Traces telemetry.
	spans            *ewma.Rate
	spansLastWriteOn int64
}

func newThroughputCal(every time.Duration) *throughputCalc {
	return &throughputCalc{
		every:            every,
		metricsMaxSentTs: 0,
		samples:          ewma.NewEWMARate(1, every),
		metadata:         ewma.NewEWMARate(1, every),
		spans:            ewma.NewEWMARate(1, every),
		spansLastWriteOn: 0,
	}
}

func (tc *throughputCalc) run() {
	t := time.NewTicker(tc.every)
	for range t.C {
		tc.samples.Tick()
		tc.metadata.Tick()
		tc.spans.Tick()

		samplesRate := tc.samples.Rate()
		metadataRate := tc.metadata.Rate()
		spansRate := tc.spans.Rate()

		if samplesRate+metadataRate+spansRate == 0 {
			continue
		}

		throughput := []interface{}{"msg", "ingestor throughput"}
		if samplesRate != 0 {
			maxSentTs := timestamp.Time(atomic.LoadInt64(&tc.metricsMaxSentTs))
			throughput = append(throughput, []interface{}{"samples/sec", int(samplesRate), "metrics-max-sent-ts", maxSentTs}...)
		}

		if spansRate != 0 {
			spansLastWriteOn := timestamp.Time(atomic.LoadInt64(&tc.spansLastWriteOn))
			throughput = append(throughput, []interface{}{"spans/sec", int(spansRate), "spans-last-write-on", spansLastWriteOn}...)
		}

		if metadataRate != 0 {
			throughput = append(throughput, []interface{}{"metric-metadata/sec", int(metadataRate)}...)
		}

		if len(throughput) > 2 {
			// Log only if we had any activity.
			log.Info(throughput...)
		}
	}
}

func ReportMetricsProcessed(maxTs int64, numSamples, numMetadata uint64) {
	if throughputWatcher == nil {
		// Throughput watcher is disabled.
		return
	}
	throughputWatcher.samples.Incr(int64(numSamples))
	throughputWatcher.metadata.Incr(int64(numMetadata))
	if maxTs != 0 && atomic.LoadInt64(&throughputWatcher.metricsMaxSentTs) < maxTs {
		atomic.StoreInt64(&throughputWatcher.metricsMaxSentTs, maxTs)
	}
}

func ReportSpansProcessed(lastWriteTs int64, numSpans int) {
	if throughputWatcher == nil {
		return
	}
	throughputWatcher.spans.Incr(int64(numSpans))
	if lastWriteTs != 0 && atomic.LoadInt64(&throughputWatcher.spansLastWriteOn) < lastWriteTs {
		atomic.StoreInt64(&throughputWatcher.spansLastWriteOn, lastWriteTs)
	}
}
