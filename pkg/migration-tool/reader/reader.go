package reader

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/timescale/promscale/pkg/log"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

const (
	megaByte                  = 1024 * 1024
	ResponseDataSizeHalfLimit = megaByte * 25
	DefaultReadTimeout        = time.Minute * 5
	MaxTimeRangeDeltaLimit    = time.Minute * 32
)

type RemoteRead struct {
	url             string
	client          remote.ReadClient
	plan            *plan.Plan
	fetchersRunning atomic.Uint32
	sigBlockWrite   chan struct{}
	sigBlockRead    chan struct{} // To the writer.
}

// New creates a new RemoteRead. It creates a ReadClient that is imported from Prometheus remote storage.
// RemoteRead takes help of plan to understand how to create fetchers.
func New(readStorageUrl string, p *plan.Plan, sigRead, sigWrite chan struct{}) (*RemoteRead, error) {
	rc, err := utils.CreateReadClient(fmt.Sprintf("reader-%d", 1), readStorageUrl, model.Duration(DefaultReadTimeout))
	if err != nil {
		return nil, fmt.Errorf("creating read-client: %w", err)
	}
	read := &RemoteRead{
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
func (rr *RemoteRead) Run(wg *sync.WaitGroup, readerUp *atomic.Bool) error {
	defer func() {
		close(rr.sigBlockRead)
		close(rr.sigBlockWrite)
		readerUp.Store(false)
		wg.Done()
	}()
	readerUp.Store(true)
	log.Info("msg", "reader is up")
	timeRangeMinutesDelta := time.Minute.Milliseconds()
	for i := rr.plan.Mint; i <= rr.plan.Maxt; {
		var (
			mint     = i
			maxt     = mint + timeRangeMinutesDelta
			fetch    = rr.createFetch(rr.url, mint, maxt)
			blockRef = rr.plan.CreateBlock(mint, maxt)
		)
		blockRef.SetDescription(fmt.Sprintf("fetching time-range: %d mins...", timeRangeMinutesDelta/time.Minute.Milliseconds()), 1)
		result, err := fetch.start(context.Background(), []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "", ".*")})
		if err != nil {
			return fmt.Errorf("remote-read run: %w", err)
		}
		if len(result.Timeseries) == 0 {
			rr.plan.DecrementBlockCount()
			i += timeRangeMinutesDelta + 1
			if timeRangeMinutesDelta < MaxTimeRangeDeltaLimit.Milliseconds() {
				timeRangeMinutesDelta *= 2
			}
			continue
		}
		qResultSize := result.Size() // Assuming each character corresponds to one byte.
		rr.plan.UpdateLastPushedMaxt(i)
		blockRef.SetDescription(fmt.Sprintf("received %d MB with delta %d mins...", qResultSize/megaByte, timeRangeMinutesDelta/time.Minute.Milliseconds()), 1)
		blockRef.Timeseries = result.Timeseries
		rr.sigBlockRead <- struct{}{}
		<-rr.sigBlockWrite
		if err := rr.plan.Clear(); err != nil {
			return fmt.Errorf("remote-read run: %w", err)
		}
		switch {
		case qResultSize < ResponseDataSizeHalfLimit && timeRangeMinutesDelta >= MaxTimeRangeDeltaLimit.Milliseconds():
			i += timeRangeMinutesDelta + 1
		case qResultSize < ResponseDataSizeHalfLimit:
			i += timeRangeMinutesDelta + 1
			timeRangeMinutesDelta *= 2
		default:
			i += timeRangeMinutesDelta + 1
			timeRangeMinutesDelta /= 2
		}
	}
	log.Info("msg", "reader is down")
	return nil
}

type fetch struct {
	url        string
	mint, maxt int64
	clientCopy remote.ReadClient // Maintain a copy of remote client for parallel fetching.
}

func (rr *RemoteRead) createFetch(url string, mint, maxt int64) *fetch {
	f := &fetch{
		url:        url,
		mint:       mint,
		maxt:       maxt,
		clientCopy: rr.client,
	}
	rr.fetchersRunning.Add(1)
	return f
}

// start starts fetching the samples from remote read storage as client based on the matchers.
func (f *fetch) start(context context.Context, matchers []*labels.Matcher) (*prompb.QueryResult, error) {
	readRequest, err := utils.CreatePrombQuery(f.mint, f.maxt, matchers)
	if err != nil {
		return nil, fmt.Errorf("create promb query: %w", err)
	}
	result, err := f.clientCopy.Read(context, readRequest)
	if err != nil {
		return nil, fmt.Errorf("executing client-read: %w", err)
	}
	return result, nil
}
