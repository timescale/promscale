// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/timescale/promscale/migration-tool/pkg/log"
	"github.com/timescale/promscale/migration-tool/pkg/utils"
)

// shardsSet represents a set of shards. It consists of configuration related
// to handling and management of shards.
type shardsSet struct {
	num         int
	set         []*shard
	cancelFuncs []context.CancelFunc

	errChan chan error
}

// newShardsSet creates a shards-set and initializes it. It creates independent clients for each shard,
// contexts and starts the shards. The shards listens to the writer routine, which is responsible for
// feeding the shards with data blocks for faster (due to sharded data) flushing.
func newShardsSet(writerCtx context.Context, httpConfig config.HTTPClientConfig, clientConfig utils.ClientConfig, numShards int) (*shardsSet, error) {
	var (
		set         = make([]*shard, numShards)
		cancelFuncs = make([]context.CancelFunc, numShards)
	)
	ss := &shardsSet{
		num:     numShards,
		errChan: make(chan error, numShards),
	}
	for i := 0; i < numShards; i++ {
		client, err := utils.NewClient(fmt.Sprintf("writer-shard-%d", i), clientConfig, httpConfig)
		if err != nil {
			return nil, fmt.Errorf("creating write-shard-client-%d: %w", i, err)
		}
		ctx, cancelFunc := context.WithCancel(writerCtx)
		shard := &shard{
			ctx:          ctx,
			client:       client,
			queue:        make(chan *[]prompb.TimeSeries),
			copyShardSet: ss,
		}
		cancelFuncs[i] = cancelFunc
		set[i] = shard
	}
	ss.set = set
	ss.cancelFuncs = cancelFuncs
	// Run the shards.
	for i := 0; i < ss.num; i++ {
		go ss.set[i].run(i)
	}
	return ss, nil
}

// scheduleTS batches the time-series based on the available shards. These batches are then
// fed to the shards in a single go which then consume and push to the respective clients.
func (s *shardsSet) scheduleTS(ts []prompb.TimeSeries) (numSigExpected int) {
	var batches = make([][]prompb.TimeSeries, s.num)
	for _, series := range ts {
		hash := utils.HashLabels(labelsSlicetoLabels(series.GetLabels()))
		batchIndex := hash % uint64(s.num)
		batches[batchIndex] = append(batches[batchIndex], series)
	}
	// Feed to the shards.
	for shardIndex := 0; shardIndex < s.num; shardIndex++ {
		batchIndex := shardIndex
		if len(batches[batchIndex]) == 0 {
			// We do not want "wait state" to happen on the shards.
			continue
		}
		numSigExpected++
		s.set[shardIndex].queue <- &batches[batchIndex]
	}
	return
}

// shard is an atomic unit of writing. Each shard accepts a sharded time-series set and
// pushes the respective time-series via a POST request. More shards will lead to bigger
// throughput if the target system can handle it.
type shard struct {
	ctx          context.Context
	client       *utils.Client
	queue        chan *[]prompb.TimeSeries
	copyShardSet *shardsSet
}

// run runs the shard.
func (s *shard) run(shardIndex int) {
	defer func() {
		log.Info("msg", fmt.Sprintf("shard-%d is inactive", shardIndex))
	}()
	log.Info("msg", fmt.Sprintf("shard-%d is active", shardIndex))
	for {
		select {
		case <-s.ctx.Done():
			return
		case refTs, ok := <-s.queue:
			if !ok {
				return
			}
			if len(*refTs) != 0 {
				// We do not want to go into the backoff strategy with empty data.
				// Even though scheduleTS takes care of empty batches (or *refTs),
				// still we check the length here in order to be extra careful.
				if err := sendSamplesWithBackoff(s.ctx, s.client, refTs); err != nil {
					s.copyShardSet.errChan <- err
					return
				}
			} else {
				// We want the error to not kill the shards else the system can go in a stall.
				log.Error("msg", fmt.Sprintf("incorrect sharding logic error: empty time-series received in the shard %d! Please consider opening an issue at https://github.com/timescale/promscale/issues", shardIndex))
			}
			s.copyShardSet.errChan <- nil
		}
	}
}

const backOffRetryDuration = time.Second * 1

var bytePool = sync.Pool{New: func() interface{} { return new([]byte) }}

// sendSamples to the remote storage with backoff for recoverable errors.
func sendSamplesWithBackoff(ctx context.Context, client *utils.Client, samples *[]prompb.TimeSeries) error {
	buf := bytePool.Get().(*[]byte)
	defer func() {
		*buf = (*buf)[:0]
		bytePool.Put(buf)
	}()
	req, err := buildWriteRequest(*samples, *buf)
	if err != nil {
		// Failing to build the write request is non-recoverable, since it will
		// only error if marshaling the proto to bytes fails.
		return err
	}

	backoff := backOffRetryDuration
	nonRecvRetries := 0
	*buf = req

	for {
	retry:
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := client.Store(ctx, *buf); err != nil {
			// If the error is unrecoverable, we should not retry.
			if _, ok := err.(remote.RecoverableError); !ok {
				switch r := client.Config(); r.OnErr {
				case utils.Retry:
					if r.MaxRetry != 0 {
						// If MaxRetry is 0, we are expected to retry forever.
						if nonRecvRetries >= r.MaxRetry {
							return fmt.Errorf("exceeded retrying limit in non-recoverable error. Aborting")
						}
						nonRecvRetries++
					}
					log.Info("msg", "received non-recoverable error, retrying again after delay", "delay", r.Delay)
					time.Sleep(r.Delay)
					goto retry
				case utils.Skip:
					log.Info("msg", "received non-recoverable error, skipping current slab")
					return nil
				case utils.Abort:
				}
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

func labelsSlicetoLabels(ls []prompb.Label) prompb.Labels {
	var labels prompb.Labels
	labels.Labels = ls
	return labels
}
