package testsupport

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/prompb"
	"golang.org/x/time/rate"
)

// PromLoader is responsible for loading Prometheus data files and providing iterator over Prometheus data
type PromLoader interface {
	Iterator() PromIterator
	Close() error
}

type promLoader struct {
	db     *tsdb.DBReadOnly
	blocks []tsdb.BlockReader
}

// PromIterator allows us to iterate over Prometheus data
type PromIterator interface {
	Next() bool
	Get() TimeSeries
}

type TimeSeries struct {
	seriesHash uint64
	Val        prompb.TimeSeries
}

// Sample loaded from Prometheus block
type BlockSample struct {
	ts       int64
	val      float64
	lblsHash uint64
}

type promIterator struct {
	blocks       []tsdb.BlockReader
	curBlockIdx  int
	labelsCache  map[uint64]labels.Labels
	blockSamples []*BlockSample
	curSampleIdx int
}

var allMatcher = labels.MustNewMatcher(labels.MatchRegexp, "", ".*")

// Next moves to next sample
func (i *promIterator) Next() bool {
	if i.curSampleIdx == -1 && i.curBlockIdx == -1 {
		i.curBlockIdx++
		if err := i.loadBlockSamples(); err != nil {
			panic(err)
		}
		i.curSampleIdx = 0
		return true
	}
	if i.curSampleIdx < len(i.blockSamples)-1 {
		i.curSampleIdx++
		return true
	}
	if i.curBlockIdx == len(i.blocks)-1 {
		return false
	}
	i.curBlockIdx++
	if err := i.loadBlockSamples(); err != nil {
		panic(err)
	}
	i.curSampleIdx = 0
	return true
}

func (it *promIterator) loadBlockSamples() error {
	log.Info("msg", "loading blocks", "total samples", it.blocks[it.curBlockIdx].Meta().Stats.NumSamples,
		"series", it.blocks[it.curBlockIdx].Meta().Stats.NumSeries)
	it.blockSamples = make([]*BlockSample, it.blocks[it.curBlockIdx].Meta().Stats.NumSamples)
	it.labelsCache = make(map[uint64]labels.Labels, it.blocks[it.curBlockIdx].Meta().Stats.NumSeries)
	querier, err := tsdb.NewBlockQuerier(it.blocks[it.curBlockIdx], math.MinInt64, math.MaxInt64)
	if err != nil {
		return err
	}
	defer func() {
		if err := querier.Close(); err != nil {
			log.Error("err", err)
		}
	}()
	seriesSet := querier.Select(false, nil, allMatcher)
	sampleCounter := 0
	for seriesSet.Next() {
		series := seriesSet.At()
		seriesIt := series.Iterator()
		lblsHash := series.Labels().Hash()
		if _, ok := it.labelsCache[lblsHash]; !ok {
			it.labelsCache[lblsHash] = series.Labels()
		}
		for seriesIt.Next() {
			ts, val := seriesIt.At()
			sample := &BlockSample{ts, val, lblsHash}
			it.blockSamples[sampleCounter] = sample
			sampleCounter++
		}
	}
	sort.SliceStable(it.blockSamples, func(i, j int) bool {
		return it.blockSamples[i].ts < it.blockSamples[j].ts
	})
	return nil
}

// Get returns sample at current position
func (i *promIterator) Get() TimeSeries {
	blockSample := i.blockSamples[i.curSampleIdx]
	labels := i.labelsCache[blockSample.lblsHash]
	protoLabels := make([]prompb.Label, len(labels))
	for i, l := range labels {
		protoLabels[i] = prompb.Label{Name: l.Name, Value: l.Value}
	}
	sample := prompb.Sample{
		Value:     blockSample.val,
		Timestamp: blockSample.ts,
	}
	ts := prompb.TimeSeries{
		Labels:  protoLabels,
		Samples: []prompb.Sample{sample},
	}

	return TimeSeries{blockSample.lblsHash, ts}
}

func NewPromLoader(dataDir string) (PromLoader, error) {
	db, err := tsdb.OpenDBReadOnly(dataDir, nil)
	if err != nil {
		return nil, fmt.Errorf("error starting Prometheus TSDB in read-only: %v", err)
	}
	blocks, err := db.Blocks()
	if err != nil {
		return nil, fmt.Errorf("error loading data blocks: %v", err)
	}
	return &promLoader{db: db, blocks: blocks}, nil
}

func (loader *promLoader) Iterator() PromIterator {
	return &promIterator{blocks: loader.blocks, curSampleIdx: -1, curBlockIdx: -1}
}

func (loader *promLoader) Close() error {
	return loader.db.Close()
}

type IngestFunc func(context.Context, *prompb.WriteRequest) (uint64, uint64, error)

type sampleIngestor struct {
	samples   PromIterator
	shards    []chan prompb.TimeSeries
	batchSize int
	rate      *rate.Limiter
}

// NewSampleIngestor creates sampleIngestor responsible for splitting samples into shards and sending them down the ingestion path
// zero sampleRate means no limits
func NewSampleIngestor(shards int, samples PromIterator, batchSize int, sampleRate int) *sampleIngestor {
	var r *rate.Limiter
	if sampleRate != 0 {
		r = rate.NewLimiter(rate.Limit(sampleRate), 0)
	}
	sampleLoader := sampleIngestor{
		samples:   samples,
		shards:    make([]chan prompb.TimeSeries, shards),
		batchSize: batchSize,
		rate:      r,
	}
	for i := 0; i < shards; i++ {
		sampleLoader.shards[i] = make(chan prompb.TimeSeries, 2*batchSize)
	}
	return &sampleLoader
}

func (si *sampleIngestor) Run(ingest IngestFunc) {
	si.shardSamples()
	si.ingestSamples(ingest)
}

func (si *sampleIngestor) shardSamples() {
	go func() {
		defer func() {
			for _, shard := range si.shards {
				close(shard)
			}
		}()
		for si.samples.Next() {
			sample := si.samples.Get()
			shardIdx := sample.seriesHash % uint64(len(si.shards))
			si.shards[shardIdx] <- sample.Val
			if si.rate != nil {
				if err := si.rate.Wait(context.Background()); err != nil {
					log.Error(err)
				}
			}
		}
	}()
}

func (si *sampleIngestor) ingestSamples(ingest IngestFunc) {
	var wg sync.WaitGroup
	for i := 0; i < len(si.shards); i++ {
		wg.Add(1)
		go func(shard int) {
			defer func() {
				wg.Done()
			}()
			var req prompb.WriteRequest = prompb.WriteRequest{Timeseries: make([]prompb.TimeSeries, si.batchSize)}
			counter := 0
			for ts := range si.shards[shard] {
				if counter == si.batchSize {
					if _, _, err := ingest(context.Background(), &req); err != nil {
						log.Error(err)
					}
					req = prompb.WriteRequest{Timeseries: make([]prompb.TimeSeries, si.batchSize)}
					counter = 0
				} else {
					req.Timeseries[counter] = ts
					counter++
				}
			}
			if len(req.Timeseries) > 0 {
				// flush leftovers
				if _, _, err := ingest(context.Background(), &req); err != nil {
					log.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
}
