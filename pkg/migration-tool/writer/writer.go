package writer

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
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
	defaultWriteTimeout  = time.Minute * 5
	backOffRetryDuration = time.Millisecond * 100
)

type RemoteWrite struct {
	c                  context.Context
	sigBlockRead       chan *planner.Block
	client             *utils.Client
	blocksPushed       atomic.Int64
	progressMetricName string // Metric name to main the last pushed maxt to remote write storage.
	migrationJobName   string // Label value to the progress metric.
	progressTimeSeries *prompb.TimeSeries
}

// New returns a new remote write. It is responsible for writing to the remote write storage.
func New(c context.Context, remoteWriteUrl, progressMetricName, migrationJobName string, sigRead chan *planner.Block) (*RemoteWrite, error) {
	wc, err := utils.NewClient(fmt.Sprintf("writer-%d", 1), "writer", remoteWriteUrl, model.Duration(defaultWriteTimeout))
	if err != nil {
		return nil, fmt.Errorf("creating write-client: %w", err)
	}
	write := &RemoteWrite{
		c:                  c,
		client:             wc,
		sigBlockRead:       sigRead,
		migrationJobName:   migrationJobName,
		progressMetricName: progressMetricName,
		progressTimeSeries: &prompb.TimeSeries{
			Labels: utils.LabelSet(progressMetricName, migrationJobName),
		},
	}
	write.blocksPushed.Store(0)
	return write, nil
}

// Run runs the remote-writer. It waits for the remote-reader to give access to the in-memory
// data-block that is written after the most recent fetch. After reading the block and storing
// the data locally, it gives back the writing access to the remote-reader for further fetches.
func (rw *RemoteWrite) Run(errChan chan<- error) {
	var (
		buf []byte
		err error
		ts  []prompb.TimeSeries
	)
	go func() {
		defer func() {
			log.Info("msg", "writer is down")
			close(errChan)
		}()
		log.Info("msg", "writer is up")
		for {
			select {
			case <-rw.c.Done():
				return
			case blockRef, ok := <-rw.sigBlockRead:
				if !ok {
					return
				}
				ts = timeseriesRefToTimeseries(blockRef.MergeProgressSeries(rw.progressTimeSeries))
				buf = []byte{}
				if err = rw.sendSamplesWithBackoff(context.Background(), ts, &buf); err != nil {
					errChan <- fmt.Errorf("remote-write run: %w", err)
					return
				}
				rw.blocksPushed.Add(1)
				if err = blockRef.Done(); err != nil {
					errChan <- fmt.Errorf("remote-write run: %w", err)
					return
				}
			}
		}
	}()
}

// Blocks returns the total number of blocks pushed to the remote-write storage.
func (rw *RemoteWrite) Blocks() int64 {
	return rw.blocksPushed.Load()
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

	backoff := backOffRetryDuration
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
