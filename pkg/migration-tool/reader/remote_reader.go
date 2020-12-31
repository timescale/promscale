// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package reader

import (
	"context"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/log"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

type RemoteRead struct {
	c            context.Context
	url          string
	client       *utils.Client
	plan         *plan.Plan
	sigBlockRead chan *plan.Block // To the writer.
	sigForceStop chan struct{}
}

// NewRemoteRead creates a remote-reader. It creates a ReadClient that is imported from Prometheus remote storage.
// RemoteRead takes help of plan to understand how to create fetchers.
func NewRemoteRead(c context.Context, url string, p *plan.Plan, sigRead chan *plan.Block, needsStopChan bool) (Reader, error) {
	var reader *RemoteRead
	rc, err := utils.NewClient(fmt.Sprintf("reader-%d", 1), url, utils.Read, model.Duration(defaultReadTimeout))
	if err != nil {
		return nil, fmt.Errorf("creating read-client: %w", err)
	}
	reader = &RemoteRead{
		c:            c,
		url:          url,
		plan:         p,
		client:       rc,
		sigBlockRead: sigRead,
	}
	if needsStopChan {
		reader.sigForceStop = make(chan struct{})
	}
	return reader, nil
}

func (rr *RemoteRead) SigStop() {
	if rr.sigForceStop == nil {
		panic("FATAL: force-stop channel is not initialized")
	}
	rr.sigForceStop <- struct{}{}
}

// Run runs the remote read and starts fetching the samples from the read storage.
func (rr *RemoteRead) Run(errChan chan<- error) {
	var (
		err      error
		blockRef *plan.Block
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
			case <-rr.sigForceStop:
				return
			default:
			}
			blockRef, err = rr.plan.NextBlock()
			if err != nil {
				errChan <- fmt.Errorf("remote-run run: %w", err)
				return
			}
			ms := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*")}
			err = blockRef.Fetch(rr.c, rr.client, blockRef.Mint(), blockRef.Maxt(), ms)
			if err != nil {
				errChan <- fmt.Errorf("remote-run run: %w", err)
				return
			}
			if len(blockRef.Series()) == 0 {
				rr.plan.DecrementBlockCount()
				continue
			}
			rr.sigBlockRead <- blockRef
			blockRef = nil
		}
	}()
}
