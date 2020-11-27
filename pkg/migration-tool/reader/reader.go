package reader

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/promscale/pkg/log"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

const defaultReadTimeout = time.Minute * 5

type RemoteRead struct {
	c               context.Context
	url             string
	client          *utils.Client
	plan            *plan.Plan
	fetchersRunning atomic.Uint32
	sigBlockWrite   chan struct{}
	sigBlockRead    chan *plan.Block // To the writer.
}

// New creates a new RemoteRead. It creates a ReadClient that is imported from Prometheus remote storage.
// RemoteRead takes help of plan to understand how to create fetchers.
func New(c context.Context, readStorageUrl string, p *plan.Plan, sigRead chan *plan.Block, sigWrite chan struct{}) (*RemoteRead, error) {
	rc, err := utils.CreateReadClient(fmt.Sprintf("reader-%d", 1), readStorageUrl, model.Duration(defaultReadTimeout))
	if err != nil {
		return nil, fmt.Errorf("creating read-client: %w", err)
	}
	read := &RemoteRead{
		c:             c,
		url:           readStorageUrl,
		plan:          p,
		client:        rc,
		sigBlockRead:  sigRead,
		sigBlockWrite: sigWrite,
	}
	read.fetchersRunning.Store(0)
	return read, nil
}

// Run runs the remote read and starts fetching the samples from the read storage.
func (rr *RemoteRead) Run(errChan chan<- error, sigFinish chan<- struct{}) {
	var (
		err                   error
		blockRef              *plan.Block
		result                *prompb.QueryResult
		timeRangeMinutesDelta int64
		bytesCompressed       int
		bytesUncompressed     int
	)
	go func() {
		defer func() {
			close(rr.sigBlockRead)
			close(rr.sigBlockWrite)
			log.Info("msg", "reader is down")
			sigFinish <- struct{}{}
		}()
		log.Info("msg", "reader is up")
		select {
		case <-rr.c.Done():
			return
		default:
		}
		for rr.plan.ShouldProceed() {
			select {
			case <-rr.c.Done():
				break
			default:
			}
			blockRef, err, timeRangeMinutesDelta = rr.plan.NextBlock()
			if err != nil {
				errChan <- fmt.Errorf("remote-run run: %w", err)
				return
			}
			blockRef.SetDescription(fmt.Sprintf("fetching time-range: %d mins...", timeRangeMinutesDelta/time.Minute.Milliseconds()), 1)
			result, bytesCompressed, bytesUncompressed, err = blockRef.Fetch(context.Background(), rr.client, blockRef.Mint(), blockRef.Maxt(), []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*")})
			if err != nil {
				errChan <- fmt.Errorf("remote-run run: %w", err)
				return
			}
			if len(result.Timeseries) == 0 {
				rr.plan.DecrementBlockCount()
				rr.plan.Update(0)
				continue
			}
			blockRef.SetDescription(fmt.Sprintf("received %.2f MB with delta %d mins...", float64(bytesCompressed)/float64(utils.Megabyte), timeRangeMinutesDelta/time.Minute.Milliseconds()), 1)
			blockRef.Timeseries = result.Timeseries
			// We set compressed bytes in block since those are the bytes that will be pushed over the network to the write storage after snappy compression.
			// The pushed bytes are not exactly the bytesCompressed since while pushing, we add the progress metric. But,
			// the size of progress metric along with the sample is negligible. So, it is safe to consider bytesCompressed
			// in such a scenario.
			blockRef.SetBytesCompressed(bytesCompressed)
			blockRef.SetBytesUncompressed(bytesUncompressed)
			rr.plan.Update(bytesUncompressed)
			rr.sigBlockRead <- blockRef
			blockRef = nil
			<-rr.sigBlockWrite
		}
	}()
}
