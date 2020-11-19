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

const (
	DefaultReadTimeout        = time.Minute * 5
	MaxTimeRangeDeltaLimit    = time.Minute * 120
	ResponseDataSizeHalfLimit = utils.Megabyte * 25
)

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
	timeRangeMinutesDelta := time.Minute.Milliseconds()
	for i := rr.plan.Mint; i <= rr.plan.Maxt-timeRangeMinutesDelta; {
		var (
			mint = i
			maxt = mint + timeRangeMinutesDelta
		)
		blockRef, err := rr.plan.CreateBlock(mint, maxt)
		if err != nil {
			return fmt.Errorf("remote-run run: %w", err)
		}
		blockRef.SetDescription(fmt.Sprintf("fetching time-range: %d mins...", timeRangeMinutesDelta/time.Minute.Milliseconds()), 1)
		result, err := rr.fetch(context.Background(), mint, maxt, []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "", ".*")})
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
		qResultBytes, err := result.Marshal()
		if err != nil {
			return fmt.Errorf("marshalling prompb query result: %w", err)
		}
		rr.plan.SetLastPushedT(i)
		numBytes := len(qResultBytes)
		blockRef.SetBytes(numBytes)
		blockRef.SetDescription(fmt.Sprintf("received %.2f MB with delta %d mins...", float64(numBytes)/float64(utils.Megabyte), timeRangeMinutesDelta/time.Minute.Milliseconds()), 1)
		blockRef.Timeseries = result.Timeseries
		rr.sigBlockRead <- blockRef
		<-rr.sigBlockWrite
		if err := rr.plan.Clear(); err != nil {
			return fmt.Errorf("remote-read run: %w", err)
		}
		// TODO: optimize the ideal delta method.
		switch {
		case numBytes < ResponseDataSizeHalfLimit && timeRangeMinutesDelta >= MaxTimeRangeDeltaLimit.Milliseconds():
			// The balanced scenario where the time-range is neither too long to cause OOM nor too less
			// to slow down the migration process.
			i += timeRangeMinutesDelta + 1
		case numBytes < ResponseDataSizeHalfLimit:
			i += timeRangeMinutesDelta + 1
			// We increase the time-range for the next fetch if the current time-range fetch resulted in size that is
			// less than half the aimed maximum size of the in-memory block. This continues till we reach the
			// maximum time-range delta.
			timeRangeMinutesDelta *= 2
		default:
			i += timeRangeMinutesDelta + 1
			// Decrease the time-range delta if the current fetch size exceeds the half aimed. This is done so that
			// we reach an ideal time-range as the block numbers grow and successively reach the balanced scenario.
			timeRangeMinutesDelta /= 3
		}
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
