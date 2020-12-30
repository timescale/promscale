// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

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
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/timescale/promscale/pkg/log"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

const (
	defaultReadTimeout = time.Minute * 5
	URL                = iota
	PromTSDB
)

type RemoteRead struct {
	c               context.Context
	readType        uint
	path, url       string
	client          *utils.Client
	plan            *plan.Plan
	concurrentPulls int
	sigBlockRead    chan *plan.Block // To the writer.
	SigForceStop    chan struct{}
}

// New creates a new RemoteRead. It creates a ReadClient that is imported from Prometheus remote storage.
// RemoteRead takes help of plan to understand how to create fetchers.
func New(c context.Context, readStorageUrl string, p *plan.Plan, numConcurrentPulls int, sigRead chan *plan.Block) (*RemoteRead, error) {
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
		sigBlockRead:    sigRead,
	}
	return read, nil
}

// Run runs the remote read and starts fetching the samples from the read storage.
func (rr *RemoteRead) Run(errChan chan<- error) {
	var (
		err      error
		blockRef *plan.Block
	)
	deferFunc := func() {
		close(rr.sigBlockRead)
		log.Info("msg", "reader is down")
		close(errChan)
	}
	log.Info("msg", "reader is up")
	readURL := func() {
		// Comment.
		defer deferFunc()
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
			err = blockRef.Fetch(rr.c, rr.client, blockRef.Mint(), blockRef.Maxt(), ms)
			if err != nil {
				errChan <- fmt.Errorf("remote-run run: %w", err)
				return
			}
			if blockRef.IsEmpty() {
				rr.plan.DecrementBlockCount()
				continue
			}
			rr.sigBlockRead <- blockRef
			blockRef = nil
		}
	}

	readPromBlocks := func() {
		// Comment.
		defer deferFunc()
		db, err := tsdb.OpenDBReadOnly(rr.path, nil)
		if err != nil {
			errChan <- fmt.Errorf("remote-read run: opening prom tsdb: %w", err)
			return
		}
		defer func() {
			err = tsdb_errors.NewMulti(err, db.Close()).Err()
		}()
		querier, err := db.Querier(rr.c, rr.plan.Mint(), rr.plan.Maxt())
		if err != nil {
			errChan <- fmt.Errorf("remote-read run: prom tsdb querier: %w", err)
			return
		}
		defer querier.Close()
		ss := querier.Select(false, nil, labels.MustNewMatcher(labels.MatchRegexp, "", ".*"))
		var iteratorStore = new(sync.Map)
		type cusMap struct {
			labels labels.Labels
			itr    chunkenc.Iterator
		}
		// Fill the iteratorStore with the iterators across all the series-sets for the given time-range.
		for ss.Next() {
			series := ss.At()
			iteratorStore.Store(series.Labels().String(), cusMap{labels: series.Labels(), itr: series.Iterator()})
		}
		// Start creating blocks and filling them with samples.
		for rr.plan.ShouldProceed() {
			select {
			case <-rr.c.Done():
				return
			case <-rr.SigForceStop:
				return
			default:
			}
			blockRef, err := rr.plan.NextBlock()
			if err != nil {
				errChan <- fmt.Errorf("remote-run run: %w", err)
				return
			}
			ts := make(tsMap)
			mint, maxt := blockRef.Mint(), blockRef.Maxt()
			callBack := func(_ interface{}, value interface{}) bool {
				cus, ok := value.(cusMap)
				if !ok {
					errChan <- fmt.Errorf("remote-read run: value not found in iteratorStore")
					return false
				}
				itr := cus.itr
				var samples []prompb.Sample
				appender := func() bool {
					t, v := itr.At()
					if t >= mint && t < maxt {
						samples = append(samples, prompb.Sample{
							Timestamp: t,
							Value:     v,
						})
						return true
					}
					return false
				}
				if itr.Seek(mint) {
					appender()
					for itr.Next() {
						if !appender() {
							break
						}
					}
				}
				if len(samples) > 0 {
					ts[cus.labels.String()] = struct {
						labels  []prompb.Label
						samples []prompb.Sample
					}{labels: labelsToPrombLabels(cus.labels), samples: samples}
				}
				return true
			}
			iteratorStore.Range(callBack)
			timeseries, bytesUncompressed := tsMapToTs(ts)
			ts = nil
			blockRef.Fill(timeseries, bytesUncompressed)
			rr.sigBlockRead <- blockRef
			blockRef = nil
		}
	}
	switch rr.readType {
	case URL:
		go readURL()
	case PromTSDB:
		go readPromBlocks()
	default:
		errChan <- fmt.Errorf("invalid readType: %d", rr.readType)
	}
}

func labelsToPrombLabels(lb labels.Labels) []prompb.Label {
	lbs := make([]prompb.Label, len(lb))
	for i, l := range lb {
		lbs[i].Name = l.Name
		lbs[i].Value = l.Value
	}
	return lbs
}

type tsMap map[string]struct {
	labels  []prompb.Label
	samples []prompb.Sample
}

func tsMapToTs(m tsMap) ([]*prompb.TimeSeries, int) {
	ts := make([]*prompb.TimeSeries, len(m))
	size := 0
	i := 0
	for _, tsm := range m {
		pts := &prompb.TimeSeries{
			Labels:  tsm.labels,
			Samples: tsm.samples,
		}
		b, _ := pts.Marshal()
		size += len(b)
		ts[i] = pts
		i++
	}
	return ts, size
}
