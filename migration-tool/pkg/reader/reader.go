// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package reader

import (
	"context"
	"fmt"

	"github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/timescale/promscale/migration-tool/pkg/log"
	plan "github.com/timescale/promscale/migration-tool/pkg/planner"
	"github.com/timescale/promscale/migration-tool/pkg/utils"
)

// Config is config for reader.
type Config struct {
	Context      context.Context
	ClientConfig utils.ClientConfig
	Plan         *plan.Plan
	HTTPConfig   config.HTTPClientConfig

	ConcurrentPulls int

	SigSlabRead chan *plan.Slab // To the writer.
	SigSlabStop chan struct{}

	MetricsMatchers []*labels.Matcher
}

type Read struct {
	Config
	client *utils.Client
}

// New creates a new Read. It creates a ReadClient that is imported from Prometheus remote storage.
// Read takes help of plan to understand how to create fetchers.
func New(config Config) (*Read, error) {
	rc, err := utils.NewClient(fmt.Sprintf("reader-%d", 1), config.ClientConfig, config.HTTPConfig)
	if err != nil {
		return nil, fmt.Errorf("creating read-client: %w", err)
	}
	read := &Read{
		Config: config,
		client: rc,
	}
	return read, nil
}

// Run runs the remote read and starts fetching the samples from the read storage.
func (r *Read) Run(errChan chan<- error) {
	var (
		err     error
		slabRef *plan.Slab
	)
	go func() {
		defer func() {
			close(r.SigSlabRead)
			log.Info("msg", "reader is down")
			close(errChan)
		}()
		log.Info("msg", "reader is up")
		select {
		case <-r.Context.Done():
			return
		default:
		}
		for r.Plan.ShouldProceed() {
			select {
			case <-r.Context.Done():
				return
			case <-r.SigSlabStop:
				return
			default:
			}
			slabRef, err = r.Plan.NextSlab()
			if err != nil {
				errChan <- fmt.Errorf("remote-read run: %w", err)
				return
			}
			ms := r.Config.MetricsMatchers
			if len(ms) == 0 {
				log.Warn("msg", "empty matchers received. Please open an issue regarding this at https://github.com/timescale/promscale/issues")
				ms = []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+")}
			}
			err = slabRef.Fetch(r.Context, r.client, slabRef.Mint(), slabRef.Maxt(), ms)
			if err != nil {
				errChan <- fmt.Errorf("remote-read run: %w", err)
				return
			}
			if slabRef.IsEmpty() {
				r.Plan.DecrementSlabCount()
				continue
			}
			r.SigSlabRead <- slabRef
			slabRef = nil
		}
	}()
}
