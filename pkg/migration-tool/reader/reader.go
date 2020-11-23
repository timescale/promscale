package reader

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/timescale/promscale/pkg/log"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

const DefaultReadTimeout = time.Minute * 5

type RemoteRead struct {
	url             string
	client          remote.ReadClient
	plan            *plan.Plan
	fetchersRunning atomic.Uint32
	sigBlockWrite   chan struct{}
	sigBlockRead    chan *plan.Block // To the writer.
}

// New creates a new RemoteRead. It creates a ReadClient that is imported from Prometheus remote storage.
// RemoteRead takes help of plan to understand how to create fetchers.
func New(readStorageUrl string, p *plan.Plan, sigRead chan *plan.Block, sigWrite chan struct{}) (*RemoteRead, error) {
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
func (rr *RemoteRead) Run() error {
	defer func() {
		close(rr.sigBlockRead)
		close(rr.sigBlockWrite)
	}()
	log.Info("msg", "reader is up")
	for rr.plan.ShouldProceed() {
		blockRef, err, timeRangeMinutesDelta := rr.plan.NextBlock()
		if err != nil {
			return fmt.Errorf("remote-run run: %w", err)
		}
		blockRef.SetDescription(fmt.Sprintf("fetching time-range: %d mins...", timeRangeMinutesDelta/time.Minute.Milliseconds()), 1)
		result, err := rr.fetch(context.Background(), blockRef.Mint(), blockRef.Maxt(), []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*")})
		if err != nil {
			return fmt.Errorf("remote-read run: %w", err)
		}
		if len(result.Timeseries) == 0 {
			rr.plan.DecrementBlockCount()
			rr.plan.Update(0)
			continue
		}
		qResultBytes, err := result.Marshal()
		if err != nil {
			return fmt.Errorf("marshalling prompb query result: %w", err)
		}
		numBytes := len(qResultBytes)
		blockRef.SetDescription(fmt.Sprintf("received %.2f MB with delta %d mins...", float64(numBytes)/float64(utils.Megabyte), timeRangeMinutesDelta/time.Minute.Milliseconds()), 1)
		blockRef.Timeseries = result.Timeseries
		blockRef.SetBytes(numBytes)
		rr.plan.Update(numBytes)
		rr.sigBlockRead <- blockRef
		<-rr.sigBlockWrite
	}
	log.Info("msg", "reader is down")
	return nil
}

// fetch starts fetching the samples from remote read storage based on the matchers.
func (rr *RemoteRead) fetch(context context.Context, mint, maxt int64, matchers []*labels.Matcher) (*prompb.QueryResult, error) {
	readRequest, err := utils.CreatePrombQuery(mint, maxt, matchers)
	if err != nil {
		return nil, fmt.Errorf("create promb query: %w", err)
	}
	result, err := rr.client.Read(context, readRequest)
	if err != nil {
		return nil, fmt.Errorf("executing client-read: %w", err)
	}
	return result, nil
}
