// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package planner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/schollz/progressbar/v3"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

// Slab represents an in-memory storage for data that is fetched by the reader.
type Slab struct {
	id                    int64
	mint                  int64 // inclusive.
	maxt                  int64 // exclusive.
	done                  bool
	pbarDescriptionPrefix string
	pbar                  *progressbar.ProgressBar
	timeseries            []*prompb.TimeSeries
	stores                []store
	numStores             int
	numBytesCompressed    int
	numBytesUncompressed  int
	pbarMux               *sync.Mutex
	plan                  *Plan // We keep a copy of plan so that each slab has the authority to update the stats of the planner.
}

type store struct {
	// Represent the order of a store. This helps to main the ascending order of time
	// which is important while pushing data to remote-write storage.
	id         int
	mint, maxt int64
}

// initStores initializes the stores.
func (s *Slab) initStores() {
	var (
		proceed   = s.mint
		increment = (s.maxt - s.mint) / int64(s.numStores)
	)
	for storeIndex := 0; storeIndex < s.numStores; storeIndex++ {
		s.stores[storeIndex] = store{
			id:   storeIndex,
			mint: proceed,
			maxt: proceed + increment,
		}
		proceed += increment
	}
	// To ensure that we cover the entire range, which may get missed due
	// to integer division in increment, we update the maxt of the last store
	// to be the maxt of the slab.
	s.stores[s.numStores-1].maxt = s.maxt
}

// Fetch starts fetching the samples from remote read storage based on the matchers. It takes care of concurrent pulls as well.
func (s *Slab) Fetch(ctx context.Context, client *utils.Client, mint, maxt int64, matchers []*labels.Matcher) (err error) {
	var (
		totalRequests         = s.numStores
		cancelFuncs           = make([]context.CancelFunc, totalRequests)
		timeRangeMinutesDelta = (maxt - mint) / time.Minute.Milliseconds()
		responseChan          = make(chan interface{}, totalRequests)
	)
	s.UpdatePBarMax(s.PBarMax() + totalRequests + 1)
	s.SetDescription(fmt.Sprintf("pulling (0/%d) ...", totalRequests), 1)
	for i := 0; i < totalRequests; i++ {
		readRequest, err := utils.CreatePrombQuery(s.stores[i].mint, s.stores[i].maxt, matchers)
		if err != nil {
			return fmt.Errorf("create promb query: %w", err)
		}
		cctx, cancelFunc := context.WithCancel(ctx)
		cancelFuncs[i] = cancelFunc
		// Initiate concurrent fetching of data.
		// Initializing descriptionMsg with empty string will ensure that transfer rate is shown only when totalRequests is 1.
		// This is to prevent all Read requests from showing their respective transfer rates in a single line, which leads to an improper ui-ux.
		// Hence for multiple fetches, we use the Progress-bar to show information related to the number of read requests done. This gives a better
		// idea of the data left.
		var descriptionMsg string
		if totalRequests == 1 {
			descriptionMsg = fmt.Sprintf("%s | time-range: %d mins", s.pbarDescriptionPrefix, timeRangeMinutesDelta)
		}
		go client.ReadChannels(cctx, readRequest, i, descriptionMsg, responseChan)
	}
	var (
		bytesCompressed   int
		bytesUncompressed int
	)
	for i := 0; i < totalRequests; i++ {
		resp := <-responseChan
		switch response := resp.(type) {
		case *utils.PrompbResponse:
			bytesCompressed += response.NumBytesCompressed
			bytesUncompressed += response.NumBytesUncompressed
			s.timeseries = append(s.timeseries, response.Result.Timeseries...)
			s.SetDescription(fmt.Sprintf("pulling (%d/%d) ...", i+1, totalRequests), 1)
		case error:
			for _, cancelFnc := range cancelFuncs {
				cancelFnc()
			}
			return fmt.Errorf("executing client-read: %w", response)
		}
	}
	close(responseChan)
	// We set compressed bytes in slab since those are the bytes that will be pushed over the network to the write storage after snappy compression.
	// The pushed bytes are not exactly the bytesCompressed since while pushing, we add the progress metric. But,
	// the size of progress metric along with the sample is negligible. So, it is safe to consider bytesCompressed
	// in such a scenario.
	s.numBytesCompressed = bytesCompressed
	s.numBytesUncompressed = bytesUncompressed
	s.plan.update(bytesUncompressed)
	return nil
}

// Series returns the time-series in the slab.
func (s *Slab) Series() []*prompb.TimeSeries {
	return s.timeseries
}

// GetProgressSeries returns a time-series after appending a sample to the progress-metric.
func (s *Slab) UpdateProgressSeries(ts *prompb.TimeSeries) *prompb.TimeSeries {
	ts.Samples = []prompb.Sample{{Timestamp: s.Maxt(), Value: 1}} // One sample per slab.
	return ts
}

func (s *Slab) UpdatePBarMax(steps int) {
	if s.pbarDescriptionPrefix == "" {
		return
	}
	s.pbar.ChangeMax(steps)
}

func (s *Slab) PBarMax() int {
	if s.pbarDescriptionPrefix == "" {
		return -1
	}
	return s.pbar.GetMax()
}

func (s *Slab) SetDescription(description string, proceed int) {
	if s.pbarDescriptionPrefix == "" {
		return
	}
	s.pbarMux.Lock()
	defer s.pbarMux.Unlock()
	_ = s.pbar.Add(proceed)
	s.pbar.Describe(fmt.Sprintf("%s | %s", s.pbarDescriptionPrefix, description))
}

// Done updates the text and sets the spinner to done.
func (s *Slab) Done() error {
	if s.pbarDescriptionPrefix == "" {
		return nil
	}
	s.SetDescription(
		fmt.Sprintf("pushed %.2f MB. Memory footprint: %.2f MB.", float64(s.numBytesCompressed)/float64(utils.Megabyte), float64(s.numBytesUncompressed)/float64(utils.Megabyte)),
		1,
	)
	s.done = true
	if err := s.pbar.Finish(); err != nil {
		return fmt.Errorf("finish slab-lifecycle: %w", err)
	}
	return nil
}

// IsEmpty returns true if the slab does not contain any time-series.
func (s *Slab) IsEmpty() bool {
	return len(s.timeseries) == 0
}

// Mint returns the mint of the slab (inclusive).
func (s *Slab) Mint() int64 {
	return s.mint
}

// Maxt returns the maxt of the slab (exclusive).
func (s *Slab) Maxt() int64 {
	return s.maxt
}
