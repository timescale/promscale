// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	uber_atomic "go.uber.org/atomic"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tracer"
	tput "github.com/timescale/promscale/pkg/util/throughput"
)

const (
	finalizeMetricCreation = "CALL _prom_catalog.finalize_metric_creation()"
	getEpochSQL            = "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1"
)

var ErrDispatcherClosed = fmt.Errorf("dispatcher is closed")

// pgxDispatcher redirects incoming samples to the appropriate metricBatcher
// corresponding to the metric in the sample.
type pgxDispatcher struct {
	conn                   pgxconn.PgxConn
	metricTableNames       cache.MetricCache
	scache                 cache.SeriesCache
	invertedLabelsCache    *cache.InvertedLabelsCache
	exemplarKeyPosCache    cache.PositionCache
	batchers               sync.Map
	completeMetricCreation chan struct{}
	asyncAcks              bool
	copierReadRequestCh    chan<- readRequest
	seriesEpochRefresh     *time.Ticker
	doneChannel            chan struct{}
	closed                 *uber_atomic.Bool
	doneWG                 sync.WaitGroup
}

var _ model.Dispatcher = &pgxDispatcher{}

func newPgxDispatcher(conn pgxconn.PgxConn, mCache cache.MetricCache, scache cache.SeriesCache, eCache cache.PositionCache, lCache *cache.InvertedLabelsCache, cfg *Cfg) (*pgxDispatcher, error) {
	numCopiers := cfg.NumCopiers
	if numCopiers < 1 {
		log.Warn("msg", "num copiers less than 1, setting to 1")
		numCopiers = 1
	}

	// the copier read request channel retains the queue order between metrics
	maxMetrics := 10000
	copierReadRequestCh := make(chan readRequest, maxMetrics)

	metrics.IngestorChannelCap.With(prometheus.Labels{"type": "metric", "subsystem": "copier", "kind": "sample"}).Set(float64(cap(copierReadRequestCh)))
	metrics.RegisterCopierChannelLenMetric(func() float64 { return float64(len(copierReadRequestCh)) })

	if cfg.IgnoreCompressedChunks {
		// Handle decompression to not decompress anything.
		handleDecompression = skipDecompression
	}

	sw := NewSeriesWriter(conn, lCache)
	elf := NewExamplarLabelFormatter(conn, eCache)

	for i := 0; i < numCopiers; i++ {
		go runCopier(conn, copierReadRequestCh, sw, elf)
	}

	inserter := &pgxDispatcher{
		conn:                   conn,
		metricTableNames:       mCache,
		scache:                 scache,
		invertedLabelsCache:    lCache,
		exemplarKeyPosCache:    eCache,
		completeMetricCreation: make(chan struct{}, 1),
		asyncAcks:              cfg.MetricsAsyncAcks,
		copierReadRequestCh:    copierReadRequestCh,
		// set to run at half our deletion interval
		seriesEpochRefresh: time.NewTicker(30 * time.Minute),
		doneChannel:        make(chan struct{}),
		closed:             uber_atomic.NewBool(false),
	}
	inserter.closed.Store(false)
	runBatchWatcher(inserter.doneChannel)

	//on startup run a completeMetricCreation to recover any potentially
	//incomplete metric
	if err := inserter.CompleteMetricCreation(context.Background()); err != nil {
		return nil, err
	}

	go inserter.runCompleteMetricCreationWorker()

	if !cfg.DisableEpochSync {
		inserter.doneWG.Add(1)
		go func() {
			defer inserter.doneWG.Done()
			inserter.runSeriesEpochSync()
		}()
	}
	return inserter, nil
}

func (p *pgxDispatcher) runCompleteMetricCreationWorker() {
	for range p.completeMetricCreation {
		err := p.CompleteMetricCreation(context.Background())
		if err != nil {
			log.Warn("msg", "Got an error finalizing metric", "err", err)
		}
	}
}

func (p *pgxDispatcher) runSeriesEpochSync() {
	epoch, err := p.refreshSeriesEpoch(model.InvalidSeriesEpoch)
	// we don't have any great place to report errors, and if the
	// connection recovers we can still make progress, so we'll just log it
	// and continue execution
	if err != nil {
		log.Error("msg", "error refreshing the series cache", "err", err)
	}
	for {
		select {
		case <-p.seriesEpochRefresh.C:
			epoch, err = p.refreshSeriesEpoch(epoch)
			if err != nil {
				log.Error("msg", "error refreshing the series cache", "err", err)
			}
		case <-p.doneChannel:
			return
		}
	}
}

func (p *pgxDispatcher) refreshSeriesEpoch(existingEpoch model.SeriesEpoch) (model.SeriesEpoch, error) {
	dbEpoch, err := p.getServerEpoch()
	if err != nil {
		// Trash the cache just in case an epoch change occurred, seems safer
		p.scache.Reset()
		// Also trash the inverted labels cache, which can also be invalidated when the series cache is
		p.invertedLabelsCache.Reset()
		return model.InvalidSeriesEpoch, err
	}
	if existingEpoch == model.InvalidSeriesEpoch || dbEpoch != existingEpoch {
		p.scache.Reset()
		// If the series cache needs to be invalidated, so does the inverted labels cache
		p.invertedLabelsCache.Reset()
	}
	return dbEpoch, nil
}

func (p *pgxDispatcher) getServerEpoch() (model.SeriesEpoch, error) {
	var newEpoch int64
	row := p.conn.QueryRow(context.Background(), getEpochSQL)
	err := row.Scan(&newEpoch)
	if err != nil {
		return -1, err
	}

	return model.SeriesEpoch(newEpoch), nil
}

func (p *pgxDispatcher) CompleteMetricCreation(ctx context.Context) error {
	if p.closed.Load() {
		return ErrDispatcherClosed
	}
	_, span := tracer.Default().Start(ctx, "dispatcher-complete-metric-creation")
	defer span.End()
	_, err := p.conn.Exec(
		context.Background(),
		finalizeMetricCreation,
	)
	return err
}

func (p *pgxDispatcher) Close() {
	if p.closed.Load() {
		return
	}
	p.closed.Store(true)
	close(p.completeMetricCreation)
	p.batchers.Range(func(key, value interface{}) bool {
		close(value.(chan *insertDataRequest))
		return true
	})

	close(p.copierReadRequestCh)
	close(p.doneChannel)
	p.doneWG.Wait()
}

// InsertTs inserts a batch of data into the database.
// The data should be grouped by metric name.
// returns the number of rows we intended to insert (_not_ how many were
// actually inserted) and any error.
// Though we may insert data to multiple tables concurrently, if asyncAcks is
// unset this function will wait until _all_ the insert attempts have completed.
func (p *pgxDispatcher) InsertTs(ctx context.Context, dataTS model.Data) (uint64, error) {
	if p.closed.Load() {
		return 0, ErrDispatcherClosed
	}
	_, span := tracer.Default().Start(ctx, "dispatcher-insert-ts")
	defer span.End()
	var (
		numRows      uint64
		maxt         int64
		rows         = dataTS.Rows
		workFinished = new(sync.WaitGroup)
	)
	workFinished.Add(len(rows))
	// we only allocate enough space for a single error message here as we only
	// report one error back upstream. The inserter should not block on this
	// channel, but only insert if it's empty, anything else can deadlock.
	errChan := make(chan error, 1)
	for metricName, data := range rows {
		for _, insertable := range data {
			numRows += uint64(insertable.Count())
			ts := insertable.MaxTs()
			if maxt < ts {
				maxt = ts
			}
		}
		p.getMetricBatcher(metricName) <- &insertDataRequest{spanCtx: span.SpanContext(), metric: metricName, data: data, finished: workFinished, errChan: errChan}
	}
	span.SetAttributes(attribute.Int64("num_rows", int64(numRows)))
	span.SetAttributes(attribute.Int("num_metrics", len(rows)))
	reportIncomingBatch(numRows)
	reportOutgoing := func() {
		reportOutgoingBatch(numRows)
		reportBatchProcessingTime(dataTS.ReceivedTime)
	}

	waitForIngestionToComplete := func() error {
		var err error
		workFinished.Wait()
		reportOutgoing()
		select {
		case err = <-errChan:
		default:
		}
		reportMetricsTelemetry(maxt, numRows, 0)
		close(errChan)
		return err
	}
	var err error
	if !p.asyncAcks {
		err = waitForIngestionToComplete()
	} else {
		go func() {
			err := waitForIngestionToComplete()
			if err != nil {
				log.Error("msg", "error on async send", "dropped", numRows, "err", err)
			}
		}()
	}

	return numRows, err
}

func (p *pgxDispatcher) InsertMetadata(ctx context.Context, metadata []model.Metadata) (uint64, error) {
	if p.closed.Load() {
		return 0, ErrDispatcherClosed
	}
	_, span := tracer.Default().Start(ctx, "dispatcher-insert-metadata")
	defer span.End()
	totalRows := uint64(len(metadata))
	insertedRows, err := insertMetadata(p.conn, metadata)
	if err != nil {
		return insertedRows, err
	}
	reportMetricsTelemetry(0, 0, insertedRows)
	if totalRows != insertedRows {
		return insertedRows, fmt.Errorf("failed to insert all metadata: inserted %d rows out of %d rows in total", insertedRows, totalRows)
	}
	return insertedRows, nil
}

func reportMetricsTelemetry(maxTs int64, numSamples, numMetadata uint64) {
	tput.ReportMetricsProcessed(maxTs, numSamples, numMetadata)

	// Max_sent_timestamp stats.
	if maxTs < atomic.LoadInt64(&metrics.MaxSentTs) {
		return
	}
	atomic.StoreInt64(&metrics.MaxSentTs, maxTs)
	metrics.IngestorMaxSentTimestamp.With(prometheus.Labels{"type": "metric"}).Set(float64(maxTs))
}

// Get the handler for a given metric name, creating a new one if none exists
func (p *pgxDispatcher) getMetricBatcher(metric string) chan<- *insertDataRequest {
	batcher, ok := p.batchers.Load(metric)
	if !ok {
		// The ordering is important here: we need to ensure that every call
		// to getMetricInserter() returns the same inserter. Therefore, we can
		// only start up the inserter routine if we know that we won the race
		// to create the inserter, anything else will leave a zombie inserter
		// lying around.
		c := make(chan *insertDataRequest, metrics.MetricBatcherChannelCap)
		actual, old := p.batchers.LoadOrStore(metric, c)
		batcher = actual
		if !old {
			go runMetricBatcher(p.conn, c, metric, p.completeMetricCreation, p.metricTableNames, p.copierReadRequestCh)
		}
	}
	ch := batcher.(chan *insertDataRequest)
	metrics.IngestorChannelLenBatcher.Set(float64(len(ch)))
	return ch
}

type insertDataRequest struct {
	spanCtx  trace.SpanContext
	metric   string
	finished *sync.WaitGroup
	data     []model.Insertable
	errChan  chan error
}

func (idr *insertDataRequest) reportResult(err error) {
	if err != nil {
		select {
		case idr.errChan <- err:
		default:
		}
	}
	idr.finished.Done()
}
