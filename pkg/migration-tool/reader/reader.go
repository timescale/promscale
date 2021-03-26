// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package reader

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
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
	concurrentPulls int
	sigSlabRead     chan *plan.Slab // To the writer.
	SigSlabStop     chan struct{}
}

// New creates a new RemoteRead. It creates a ReadClient that is imported from Prometheus remote storage.
// RemoteRead takes help of plan to understand how to create fetchers.
func New(c context.Context, readStorageUrl string, p *plan.Plan, numConcurrentPulls int, sigRead chan *plan.Slab) (*RemoteRead, error) {
	rc, err := utils.NewClient(fmt.Sprintf("reader-%d", 1), readStorageUrl, utils.Read, model.Duration(defaultReadTimeout))
	if err != nil {
		return nil, fmt.Errorf("creating read-client: %w", err)
	}
	read := &RemoteRead{
		c:               c,
		url:             readStorageUrl,
		plan:            p,
		client:          rc,
		concurrentPulls: numConcurrentPulls,
		sigSlabRead:     sigRead,
	}
	return read, nil
}

// Run runs the remote read and starts fetching the samples from the read storage.
func (rr *RemoteRead) Run(errChan chan<- error) {
	var (
		err     error
		slabRef *plan.Slab
	)
	go func() {
		defer func() {
			close(rr.sigSlabRead)
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
			case <-rr.SigSlabStop:
				return
			default:
			}
			slabRef, err = rr.plan.NextSlab()
			if err != nil {
				errChan <- fmt.Errorf("remote-run run: %w", err)
				return
			}
			ms := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*")}
			err = slabRef.Fetch(rr.c, rr.client, slabRef.Mint(), slabRef.Maxt(), ms)
			if err != nil {
				errChan <- fmt.Errorf("remote-run run: %w", err)
				return
			}
			if slabRef.IsEmpty() {
				rr.plan.DecrementSlabCount()
				continue
			}
			rr.sigSlabRead <- slabRef
			slabRef = nil
		}
	}()
}
