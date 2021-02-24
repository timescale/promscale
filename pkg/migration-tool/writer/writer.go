// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package writer

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

const (
	defaultWriteTimeout  = time.Minute * 5
	backOffRetryDuration = time.Millisecond * 100
)

type RemoteWrite struct {
	c                  context.Context
	sigSlabRead        chan *planner.Slab
	url                string
	shardsSet          *shardsSet
	slabsPushed        atomic.Int64
	progressMetricName string // Metric name to main the last pushed maxt to remote write storage.
	migrationJobName   string // Label value to the progress metric.
	progressTimeSeries *prompb.TimeSeries
}

// New returns a new remote write. It is responsible for writing to the remote write storage.
func New(c context.Context, remoteWriteUrl, progressMetricName, migrationJobName string, numShards int, progressEnabled bool, sigRead chan *planner.Slab) (*RemoteWrite, error) {
	ss, err := newShardsSet(c, remoteWriteUrl, numShards)
	if err != nil {
		return nil, fmt.Errorf("creating shards: %w", err)
	}
	write := &RemoteWrite{
		c:                  c,
		shardsSet:          ss,
		url:                remoteWriteUrl,
		sigSlabRead:        sigRead,
		migrationJobName:   migrationJobName,
		progressMetricName: progressMetricName,
	}
	if progressEnabled {
		write.progressTimeSeries = &prompb.TimeSeries{
			Labels: utils.LabelSet(progressMetricName, migrationJobName),
		}
	}
	write.slabsPushed.Store(0)
	return write, nil
}

// Run runs the remote-writer. It waits for the remote-reader to give access to the in-memory
// data-block that is written after the most recent fetch. After reading the block and storing
// the data locally, it gives back the writing access to the remote-reader for further fetches.
func (rw *RemoteWrite) Run(errChan chan<- error) {
	var (
		err    error
		shards = rw.shardsSet
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
			case <-rw.c.Done():
				return
			case slabRef, ok := <-rw.sigSlabRead:
				if !ok {
					return
				}
				slabRef.UpdatePBarMax(slabRef.PBarMax() + rw.shardsSet.num + 1)
				// Pushing data to remote-write storage.
				slabRef.SetDescription("preparing to push", 1)
				numSigExpected := shards.scheduleTS(timeseriesRefToTimeseries(slabRef.Series()))
				slabRef.SetDescription("pushing ...", 1)
				if isErrSig(slabRef, numSigExpected) {
					return
				}
				// Pushing progress-metric to remote-write storage.
				if rw.progressTimeSeries != nil {
					// Execute the block only if progress-metric is enabled.
					// Pushing progress-metric to remote-write storage.
					// This is done after making sure that all shards have successfully completed pushing of data.
					numSigExpected := rw.pushProgressMetric(slabRef.UpdateProgressSeries(rw.progressTimeSeries))
					if isErrSig(slabRef, numSigExpected) {
						return
					}
				}
				rw.slabsPushed.Add(1)
				if err = slabRef.Done(); err != nil {
					errChan <- fmt.Errorf("remote-write run: %w", err)
					return
				}
			}
		}
	}()
}

// Blocks returns the total number of blocks pushed to the remote-write storage.
func (rw *RemoteWrite) Slabs() int64 {
	return rw.slabsPushed.Load()
}

// pushProgressMetric pushes the progress-metric to the remote storage system.
func (rw *RemoteWrite) pushProgressMetric(series *prompb.TimeSeries) int {
	var shards = rw.shardsSet
	return shards.scheduleTS([]prompb.TimeSeries{*series})
}
