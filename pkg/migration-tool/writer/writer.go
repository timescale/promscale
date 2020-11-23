package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

const (
	DefaultWriteTimeout  = time.Minute * 5
	BackOffRetryDuration = time.Millisecond * 100
)

type RemoteWrite struct {
	sigBlockRead       chan *planner.Block
	sigBlockWrite      chan struct{} // To the reader.
	plan               *planner.Plan
	client             remote.WriteClient
	progressMetricName string // Metric name to main the last pushed maxt to remote write storage.
	migrationJobName   string // Label value to the progress metric.
}

// New returns a new remote write. It is responsible for writing to the remote write storage.
func New(remoteWriteUrl, progressMetricName, migrationJobName string, plan *planner.Plan, sigRead chan *planner.Block, sigWrite chan struct{}) (*RemoteWrite, error) {
	wc, err := utils.CreateWriteClient(fmt.Sprintf("writer-%d", 1), remoteWriteUrl, model.Duration(DefaultWriteTimeout))
	if err != nil {
		return nil, fmt.Errorf("creating write-client: %w", err)
	}
	write := &RemoteWrite{
		plan:               plan,
		client:             wc,
		sigBlockRead:       sigRead,
		sigBlockWrite:      sigWrite,
		migrationJobName:   migrationJobName,
		progressMetricName: progressMetricName,
	}
	return write, nil
}

// Run runs the remote-writer. It waits for the remote-reader to give access to the in-memory
// data-block that is written after the most recent fetch. After reading the block and storing
// the data locally, it gives back the writing access to the remote-reader for further fetches.
func (rw *RemoteWrite) Run() error {
	log.Info("msg", "writer is up")
	for {
		blockRef, ok := <-rw.sigBlockRead
		if !ok {
			break
		}
		var buf []byte
		blockRef.SetDescription(fmt.Sprintf("pushing %.2f...", float64(blockRef.Bytes())/float64(utils.Megabyte)), 1)
		ps, err := utils.GetorGenerateProgressTimeseries(rw.progressMetricName, rw.migrationJobName)
		if err != nil {
			return fmt.Errorf("get or create progress time-series: %w", err)
		}
		ps.Append(1, blockRef.Maxt())
		utils.MergeWithTimeseries(&blockRef.Timeseries, ps.TimeSeries)
		ts := timeseriesRefToTimeseries(blockRef.Timeseries)
		if err := rw.sendSamplesWithBackoff(context.Background(), ts, &buf); err != nil {
			return fmt.Errorf("remote-write run: %w", err)
		}
		if err := blockRef.Done(); err != nil {
			return fmt.Errorf("remote-write: %w", err)
		}
		rw.sigBlockWrite <- struct{}{}
	}
	log.Info("msg", "writer is down")
	return nil
}

// sendSamples to the remote storage with backoff for recoverable errors.
func (rw *RemoteWrite) sendSamplesWithBackoff(ctx context.Context, samples []prompb.TimeSeries, buf *[]byte) error {
	// TODO: Add metrics similar to upstream for tool details.
	req, err := buildWriteRequest(samples, *buf)
	if err != nil {
		// Failing to build the write request is non-recoverable, since it will
		// only error if marshaling the proto to bytes fails.
		return err
	}

	backoff := BackOffRetryDuration
	*buf = req

	attemptStore := func() error {
		if err := rw.client.Store(ctx, *buf); err != nil {
			return err
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err = attemptStore()

		if err != nil {
			// If the error is unrecoverable, we should not retry.
			if _, ok := err.(remote.RecoverableError); !ok {
				return err
			}

			// If we make it this far, we've encountered a recoverable error and will retry.
			time.Sleep(backoff)
			continue
		}
		return nil
	}
}

func buildWriteRequest(samples []prompb.TimeSeries, buf []byte) ([]byte, error) {
	req := &prompb.WriteRequest{
		Timeseries: samples,
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	// snappy uses len() to see if it needs to allocate a new slice. Make the
	// buffer as long as possible.
	if buf != nil {
		buf = buf[0:cap(buf)]
	}
	compressed := snappy.Encode(buf, data)
	return compressed, nil
}

func timeseriesRefToTimeseries(tsr []*prompb.TimeSeries) (ts []prompb.TimeSeries) {
	ts = make([]prompb.TimeSeries, len(tsr))
	for i := range tsr {
		ts[i] = *tsr[i]
	}
	return
}
