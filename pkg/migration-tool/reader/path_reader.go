package reader

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/timescale/promscale/pkg/log"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
)

type PathRead struct {
	c            context.Context
	path         string
	plan         *plan.Plan
	sigBlockRead chan *plan.Block // To the writer.
	sigForceStop chan struct{}
}

// NewPathRead creates a reader that reads from local Prometheus blocks.
func NewPathRead(c context.Context, path string, p *plan.Plan, sigRead chan *plan.Block, needsStopChan bool) (Reader, error) {
	reader := &PathRead{
		c:            c,
		path:         path,
		plan:         p,
		sigBlockRead: sigRead,
	}
	if needsStopChan {
		reader.sigForceStop = make(chan struct{})
	}
	return reader, nil
}

func (rr *PathRead) SigStop() {
	if rr.sigForceStop == nil {
		panic("FATAL: force-stop channel is not initialized")
	}
	rr.sigForceStop <- struct{}{}
}

// Run runs the remote read and starts fetching the samples from the read storage.
func (rr *PathRead) Run(errChan chan<- error) {
	var err error
	db, err := tsdb.OpenDBReadOnly(rr.path, nil)
	if err != nil {
		errChan <- fmt.Errorf("path-read run: opening prom tsdb: %w", err)
		return
	}
	querier, err := db.Querier(rr.c, rr.plan.Mint(), rr.plan.Maxt())
	if err != nil {
		errChan <- fmt.Errorf("path-read run: prom tsdb querier: %w", err)
		return
	}
	ss := querier.Select(false, nil, labels.MustNewMatcher(labels.MatchRegexp, "", ".*"))
	var iteratorStore = make(map[string]labelIterator)
	// Fill the iteratorStore with the iterators across all the series-sets for the given time-range.
	for ss.Next() {
		series := ss.At()
		iteratorStore[series.Labels().String()] = labelIterator{labels: series.Labels(), itr: series.Iterator()}
	}
	go func() {
		defer func() {
			if err = tsdb_errors.NewMulti(err, db.Close()).Err(); err != nil {
				errChan <- fmt.Errorf("path-read run: %w", err)
			}
			if err = querier.Close(); err != nil {
				errChan <- fmt.Errorf("path-read run: %w", err)
			}
			close(rr.sigBlockRead)
			log.Info("msg", "reader is down")
			close(errChan)
		}()
		log.Info("msg", "reader is up")
		// Start creating blocks and filling them with samples.
		for rr.plan.ShouldProceed() {
			select {
			case <-rr.c.Done():
				return
			case <-rr.sigForceStop:
				return
			default:
			}
			blockRef, err := rr.plan.NextBlock()
			if err != nil {
				errChan <- fmt.Errorf("path-run run: %w", err)
				return
			}
			mint, maxt := blockRef.Mint(), blockRef.Maxt()
			var timeseries []*prompb.TimeSeries
			for _, iterator := range iteratorStore {
				iterator.setTimeRange(mint, maxt)
				if ts, containsTs := iterator.populateTimeseries(); containsTs {
					timeseries = append(timeseries, ts)
				}
			}
			blockRef.Fill(timeseries, 0)
			rr.sigBlockRead <- blockRef
			blockRef = nil
		}
	}()
}

type labelIterator struct {
	labels     labels.Labels
	itr        chunkenc.Iterator
	mint, maxt int64
}

func (li *labelIterator) setTimeRange(mint, maxt int64) {
	li.mint = mint
	li.maxt = maxt
}

func (li *labelIterator) populateTimeseries() (ts *prompb.TimeSeries, containsTs bool) {
	// Populate the timeseries map with the iterators for the current time-range delta.
	// We maintain a local store of series iterators in iteratorStore. This keeps the last progress in the memory
	// (at a negligible cost since iterator), leading to faster access of samples for subsequent time-ranges.
	var (
		itr     = li.itr
		samples []prompb.Sample
	)
	ts = new(prompb.TimeSeries)
	appender := func() bool {
		t, v := itr.At()
		if t >= li.mint && t < li.maxt {
			samples = append(samples, prompb.Sample{
				Timestamp: t,
				Value:     v,
			})
			return true
		}
		return false
	}
	if itr.Seek(li.mint) && appender() {
		for itr.Next() {
			if !appender() {
				break
			}
		}
	}
	if len(samples) > 0 {
		containsTs = true
		ts.Labels = labelsToPrombLabels(li.labels)
		ts.Samples = samples
	}
	return ts, containsTs
}

func labelsToPrombLabels(lb labels.Labels) []prompb.Label {
	lbs := make([]prompb.Label, len(lb))
	for i, l := range lb {
		lbs[i].Name = l.Name
		lbs[i].Value = l.Value
	}
	return lbs
}
