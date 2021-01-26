package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
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
func newShardsSet(writerCtx context.Context, writeURL string, numShards int) (*shardsSet, error) {
	var (
		set         = make([]*shard, numShards)
		cancelFuncs = make([]context.CancelFunc, numShards)
	)
	ss := &shardsSet{
		num:     numShards,
		errChan: make(chan error, numShards),
	}
	for i := 0; i < numShards; i++ {
		client, err := utils.NewClient(fmt.Sprintf("writer-shard-%d", i), writeURL, utils.Write, model.Duration(defaultWriteTimeout))
		if err != nil {
			return nil, fmt.Errorf("creating write-shard-client-%d: %w", i, err)
		}
		ctx, cancelFunc := context.WithCancel(writerCtx)
		shard := &shard{
			ctx:          ctx,
			client:       client,
			queue:        make(chan *[]byte),
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
func (s *shardsSet) scheduleTS(shardsConsumable []*[]byte) (numSigExpected int) {
	// Feed to the shards.
	for shardIndex := 0; shardIndex < len(shardsConsumable); shardIndex++ {
		if len(*shardsConsumable[shardIndex]) == 0 {
			// We do not want "wait state" to happen on the shards.
			continue
		}
		numSigExpected++
		s.set[shardIndex].queue <- shardsConsumable[shardIndex]
	}
	return
}

// shard is an atomic unit of writing. Each shard accepts a sharded time-series set and
// pushes the respective time-series via a POST request. More shards will lead to bigger
// throughput if the target system can handle it.
type shard struct {
	ctx          context.Context
	client       *utils.Client
	queue        chan *[]byte
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
				log.Error("msg", fmt.Sprintf("incorrect sharding logic error: empty byte slice received in the shard %d! Please consider opening an issue at https://github.com/timescale/promscale/issues", shardIndex))
			}
			s.copyShardSet.errChan <- nil
		}
	}
}

// sendSamples to the remote storage with backoff for recoverable errors.
func sendSamplesWithBackoff(ctx context.Context, client *utils.Client, data *[]byte) error {
	backoff := backOffRetryDuration
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := client.Store(ctx, data); err != nil {
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
