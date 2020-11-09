package writer

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
	"net/url"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/timescale/promscale/pkg/migration-tool/planner"
)

const (
	DefaultWriteTimeout  = time.Minute * 5
	BackOffRetryDuration = time.Millisecond * 100
)

type RemoteWrite struct {
	sigBlockRead  chan struct{}
	sigBlockWrite chan struct{} // To the reader.
	plan          *planner.Plan
	client        remote.WriteClient
}

// New returns a new remote write. It is responsible for writing to the remote write storage.
func New(remoteWriteUrl string, plan *planner.Plan, sigRead, sigWrite chan struct{}) (*RemoteWrite, error) {
	parsedUrl, err := url.Parse(remoteWriteUrl)
	if err != nil {
		return nil, fmt.Errorf("url-parse: %w", err)
	}
	wc, err := remote.NewWriteClient(fmt.Sprintf("writer-%d", 1), &remote.ClientConfig{
		URL:     &config.URL{URL: parsedUrl},
		Timeout: model.Duration(DefaultWriteTimeout),
	})
	if err != nil {
		return nil, fmt.Errorf("creating write-client: %w", err)
	}
	write := &RemoteWrite{
		plan:          plan,
		client:        wc,
		sigBlockRead:  sigRead,
		sigBlockWrite: sigWrite,
	}
	return write, nil
}

func (rw *RemoteWrite) Run(wg *sync.WaitGroup, readerUp *atomic.Bool) error {
	fmt.Println("writer is up")
	defer func() {
		close(rw.sigBlockWrite)
		wg.Done()
	}()
	for {
		if _, ok := <-rw.sigBlockRead; !ok {
			break
		}
		fmt.Println("receiving read signal")
		var buf []byte
		blockRef := rw.plan.CurrentBlock()
		ts := timeseriesRefToTimeseries(blockRef.Timeseries)
		if err := rw.sendSamplesWithBackoff(context.Background(), ts, &buf); err != nil {
			return fmt.Errorf("remote-write run: %w", err)
		}
		if !readerUp.Load() {
			fmt.Println("reader down, exiting..")
			break
		}
		fmt.Println("sending write signal")
		rw.sigBlockWrite <- struct{}{}
	}
	fmt.Println("writer is down")
	return nil
}

// sendSamples to the remote storage with backoff for recoverable errors.
func (rw *RemoteWrite) sendSamplesWithBackoff(ctx context.Context, samples []prompb.TimeSeries, buf *[]byte) error {
	// TODO: Add metrics similar to upstream for tool details.
	req, _, err := buildWriteRequest(samples, *buf)
	if err != nil {
		// Failing to build the write request is non-recoverable, since it will
		// only error if marshaling the proto to bytes fails.
		return err
	}

	backoff := BackOffRetryDuration
	*buf = req

	// An anonymous function allows us to defer the completion of our per-try spans
	// without causing a memory leak, and it has the nice effect of not propagating any
	// parameters for sendSamplesWithBackoff/3.
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

func buildWriteRequest(samples []prompb.TimeSeries, buf []byte) ([]byte, int64, error) {
	var highest int64
	for _, ts := range samples {
		// At the moment we only ever append a TimeSeries with a single sample in it.
		if ts.Samples[0].Timestamp > highest {
			highest = ts.Samples[0].Timestamp
		}
	}
	req := &prompb.WriteRequest{
		Timeseries: samples,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, highest, err
	}

	// snappy uses len() to see if it needs to allocate a new slice. Make the
	// buffer as long as possible.
	if buf != nil {
		buf = buf[0:cap(buf)]
	}
	compressed := snappy.Encode(buf, data)
	return compressed, highest, nil
}

func timeseriesRefToTimeseries(tsr []*prompb.TimeSeries) (ts []prompb.TimeSeries) {
	ts = make([]prompb.TimeSeries, len(tsr))
	for i := range tsr {
		ts[i] = *tsr[i]
	}
	return
}
