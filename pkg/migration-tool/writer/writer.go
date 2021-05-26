// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package writer

import (
	"context"
	"fmt"
	"github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
	"sync/atomic"
)

// Config is config for writer.
type Config struct {
	Context          context.Context
	ClientRt         utils.ClientRuntime
	MigrationJobName string // Label value to the progress metric.
	ConcurrentPush   int
	HTTPConfig       config.HTTPClientConfig

	ProgressEnabled    bool
	ProgressMetricName string // Metric name to main the last pushed maxt to remote write storage.

	SigSlabRead chan *planner.Slab
	SigSlabStop chan struct{}
}

type Write struct {
	Config
	shardsSet          *shardsSet
	slabsPushed        int64
	progressTimeSeries *prompb.TimeSeries
}

// New returns a new remote write. It is responsible for writing to the remote write storage.
func New(config Config) (*Write, error) {
	ss, err := newShardsSet(config.Context, config.HTTPConfig, config.ClientRt, config.ConcurrentPush)
	if err != nil {
		return nil, fmt.Errorf("creating shards: %w", err)
	}
	write := &Write{
		Config:    config,
		shardsSet: ss,
	}
	if config.ProgressEnabled {
		write.progressTimeSeries = &prompb.TimeSeries{
			Labels: utils.LabelSet(config.ProgressMetricName, config.MigrationJobName),
		}
	}
	return write, nil
}

// Run runs the remote-writer. It waits for the remote-reader to give access to the in-memory
// data-block that is written after the most recent fetch. After reading the block and storing
// the data locally, it gives back the writing access to the remote-reader for further fetches.
func (w *Write) Run(errChan chan<- error) {
	var (
		err    error
		shards = w.shardsSet
	)
	go func() {
		defer func() {
			for _, cancelFunc := range shards.cancelFuncs {
				cancelFunc()
			}
			log.Info("msg", "writer is down")
			close(errChan)
		}()
		log.Info("msg", "writer is up")
		isErrSig := func(ref *planner.Slab, expectedSigs int) bool {
			for i := 0; i < expectedSigs; i++ {
				if err := <-shards.errChan; err != nil {
					errChan <- fmt.Errorf("remote-write run: %w", err)
					return true
				}
				ref.SetDescription(fmt.Sprintf("pushing (%d/%d) ...", i+1, expectedSigs), 1)
			}
			return false
		}
		for {
			select {
			case <-w.Context.Done():
				return
			case slabRef, ok := <-w.SigSlabRead:
				if !ok {
					return
				}
				slabRef.UpdatePBarMax(slabRef.PBarMax() + w.shardsSet.num + 1)
				// Pushing data to remote-write storage.
				slabRef.SetDescription("preparing to push", 1)
				numSigExpected := shards.scheduleTS(timeseriesRefToTimeseries(slabRef.Series()))
				slabRef.SetDescription("pushing ...", 1)
				if isErrSig(slabRef, numSigExpected) {
					return
				}
				// Pushing progress-metric to remote-write storage.
				if w.progressTimeSeries != nil {
					// Execute the block only if progress-metric is enabled.
					// Pushing progress-metric to remote-write storage.
					// This is done after making sure that all shards have successfully completed pushing of data.
					numSigExpected := w.pushProgressMetric(slabRef.UpdateProgressSeries(w.progressTimeSeries))
					if isErrSig(slabRef, numSigExpected) {
						return
					}
				}
				atomic.AddInt64(&w.slabsPushed, 1)
				if err = slabRef.Done(); err != nil {
					errChan <- fmt.Errorf("remote-write run: %w", err)
					return
				}
			}
		}
	}()
}

// Blocks returns the total number of blocks pushed to the remote-write storage.
func (w *Write) Slabs() int64 {
	return atomic.LoadInt64(&w.slabsPushed)
}

// pushProgressMetric pushes the progress-metric to the remote storage system.
func (w *Write) pushProgressMetric(series *prompb.TimeSeries) int {
	var shards = w.shardsSet
	return shards.scheduleTS([]prompb.TimeSeries{*series})
}
