// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

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
	sigBlockRead    chan *plan.Block // To the writer.
	SigForceStop    chan struct{}
}

// New creates a new RemoteRead. It creates a ReadClient that is imported from Prometheus remote storage.
// RemoteRead takes help of plan to understand how to create fetchers.
func New(c context.Context, readStorageUrl string, p *plan.Plan, sigRead chan *plan.Block) (*RemoteRead, error) {
	rc, err := utils.NewClient(fmt.Sprintf("reader-%d", 1), readStorageUrl, utils.Read, model.Duration(defaultReadTimeout))
	if err != nil {
		return nil, fmt.Errorf("creating read-client: %w", err)
	}
	read := &RemoteRead{
		c:            c,
		url:          readStorageUrl,
		plan:         p,
		client:       rc,
		sigBlockRead: sigRead,
	}
	read.fetchersRunning.Store(0)
	return read, nil
}

// Run runs the remote read and starts fetching the samples from the read storage.
func (rr *RemoteRead) Run(errChan chan<- error) {
	var (
		err      error
		blockRef *plan.Block
		result   *prompb.QueryResult
	)
	go func() {
		defer func() {
			close(rr.sigBlockRead)
			log.Info("msg", "reader is down")
			close(errChan)
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
				return
			case <-rr.SigForceStop:
				return
			default:
			}
			blockRef, err = rr.plan.NextBlock()
			if err != nil {
				errChan <- fmt.Errorf("remote-run run: %w", err)
				return
			}
			ms := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*")}
			result, err = blockRef.Fetch(rr.c, rr.client, blockRef.Mint(), blockRef.Maxt(), ms)
			if err != nil {
				errChan <- fmt.Errorf("remote-run run: %w", err)
				return
			}
			if len(result.Timeseries) == 0 {
				rr.plan.DecrementBlockCount()
				continue
			}
			rr.sigBlockRead <- blockRef
			blockRef = nil
		}
	}()
}
