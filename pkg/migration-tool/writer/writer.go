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
	sigBlockRead       chan *planner.WriterInput
	url                string
	shardsSet          *shardsSet
	blocksPushed       atomic.Int64
	progressMetricName string // Metric name to main the last pushed maxt to remote write storage.
	migrationJobName   string // Label value to the progress metric.
	progressTimeSeries *prompb.TimeSeries
}

// New returns a new remote write. It is responsible for writing to the remote write storage.
func New(c context.Context, remoteWriteUrl, progressMetricName, migrationJobName string, numShards int, progressEnabled bool, sigRead chan *planner.WriterInput) (*RemoteWrite, error) {
	ss, err := newShardsSet(c, remoteWriteUrl, numShards)
	if err != nil {
		return nil, fmt.Errorf("creating shards: %w", err)
	}
	write := &RemoteWrite{
		c:                  c,
		shardsSet:          ss,
		url:                remoteWriteUrl,
		sigBlockRead:       sigRead,
		migrationJobName:   migrationJobName,
		progressMetricName: progressMetricName,
	}
	if progressEnabled {
		write.progressTimeSeries = &prompb.TimeSeries{
			Labels: utils.LabelSet(progressMetricName, migrationJobName),
		}
	}
	write.blocksPushed.Store(0)
	return write, nil
}

// Run runs the remote-writer. It waits for the remote-reader to give access to the in-memory
// data-block that is written after the most recent fetch. After reading the block and storing
// the data locally, it gives back the writing access to the remote-reader for further fetches.
func (rw *RemoteWrite) Run() <-chan error {
	var (
		err     error
		errChan = make(chan error)
		shards  = rw.shardsSet
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
		isErrSig := func(ref *planner.Block, expectedSigs int) bool {
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
			case consumed, ok := <-rw.sigBlockRead:
				if !ok {
					return
				}
				var (
					blockRef    = consumed.BlockRef
					shardsInput = consumed.ShardsConsumable
				)
				blockRef.UpdatePBarMax(blockRef.PBarMax() + rw.shardsSet.num + 1)
				// Pushing data to remote-write storage.
				blockRef.SetDescription("preparing to push", 1)
				numSigExpected := shards.scheduleTS(shardsInput)
				blockRef.SetDescription("pushing ...", 1)
				if isErrSig(blockRef, numSigExpected) {
					// We return here and exit the goroutine since the error is already propagated
					// to the calling function.
					return
				}
				// Pushing progress-metric to remote-write storage.
				if rw.progressTimeSeries != nil {
					// Execute the block only if progress-metric is enabled.
					// Pushing progress-metric to remote-write storage.
					// This is done after making sure that all shards have successfully completed pushing of data.
					numSigExpected, err := rw.pushProgressMetric(blockRef.UpdateProgressSeries(rw.progressTimeSeries))
					if err != nil {
						errChan <- fmt.Errorf("remote-write run: %w", err)
						return
					}
					if isErrSig(blockRef, numSigExpected) {
						// We return here and exit the goroutine since the error is already propagated
						// to the calling function.
						return
					}
				}
				rw.blocksPushed.Add(1)
				if err = blockRef.Done(); err != nil {
					errChan <- fmt.Errorf("remote-write run: %w", err)
					return
				}
			}
		}
	}()
	return errChan
}

// Blocks returns the total number of blocks pushed to the remote-write storage.
func (rw *RemoteWrite) Blocks() int64 {
	return rw.blocksPushed.Load()
}

// pushProgressMetric pushes the progress-metric to the remote storage system.
func (rw *RemoteWrite) pushProgressMetric(series *prompb.TimeSeries) (int, error) {
	shards := rw.shardsSet
	buf := &[]byte{}
	req, _, _, err := utils.BuildWriteRequest([]prompb.TimeSeries{*series}, *buf)
	if err != nil {
		return -1, fmt.Errorf("push progress-metric: %w", err)
	}
	return shards.scheduleTS([]*[]byte{&req}), nil
}
