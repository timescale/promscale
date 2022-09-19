// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
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
	finalizeMetricCreation    = "CALL _prom_catalog.finalize_metric_creation()"
	getEpochSQL               = "SELECT current_epoch, COALESCE(delete_epoch, 0) FROM _prom_catalog.ids_epoch LIMIT 1"
	getStaleSeriesIDsArraySQL = "SELECT ARRAY_AGG(id) FROM _prom_catalog.series WHERE delete_epoch IS NOT NULL"
)

var ErrDispatcherClosed = fmt.Errorf("dispatcher is closed")

// PgxDispatcher redirects incoming samples to the appropriate metricBatcher
// corresponding to the metric in the sample.
type PgxDispatcher struct {
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
	seriesRefreshLock      sync.Mutex
}

type InsertAbortError struct {
	err error
}

func NewInsertAbortError(err error) InsertAbortError {
	return InsertAbortError{err}
}

func (e InsertAbortError) Error() string {
	return e.err.Error()
}

var _ model.Dispatcher = &PgxDispatcher{}

func newPgxDispatcher(conn pgxconn.PgxConn, mCache cache.MetricCache, scache cache.SeriesCache, eCache cache.PositionCache, cfg *Cfg) (*PgxDispatcher, error) {
	numCopiers := cfg.NumCopiers
	if numCopiers < 1 {
		log.Warn("msg", "num copiers less than 1, setting to 1")
		numCopiers = 1
	}

	var dbCurrentEpoch int64
	// Bump the current epoch if it was still set to the initial value, and
	// initialize the cache's epoch. Initializing the cache's epoch is crucial
	// to ensure that ingestion will abort if a stale cache entry is present.
	row := conn.QueryRow(context.Background(), "SELECT _prom_catalog.initialize_current_epoch(now())")
	err := row.Scan(&dbCurrentEpoch)
	if err != nil {
		return nil, err
	}
	log.Info("msg", "Initializing epoch", "epoch", dbCurrentEpoch)
	scache.SetCacheEpoch(model.SeriesEpoch(dbCurrentEpoch))

	var epochDuration time.Duration
	row = conn.QueryRow(context.Background(), "SELECT _prom_catalog.get_default_value('epoch_duration')::INTERVAL")
	err = row.Scan(&epochDuration)
	if err != nil {
		return nil, err
	}
	var seriesEpochRefresh = epochDuration / 3 // This seems like a good enough rule of thumb
	log.Info("msd", "Setting series epoch refresh", "series_epoch_refresh", seriesEpochRefresh)

	// the copier read request channel retains the queue order between metrics
	maxMetrics := 10000
	copierReadRequestCh := make(chan readRequest, maxMetrics)

	metrics.IngestorChannelCap.With(prometheus.Labels{"type": "metric", "subsystem": "copier", "kind": "sample"}).Set(float64(cap(copierReadRequestCh)))
	metrics.RegisterCopierChannelLenMetric(func() float64 { return float64(len(copierReadRequestCh)) })

	if cfg.IgnoreCompressedChunks {
		// Handle decompression to not decompress anything.
		handleDecompression = skipDecompression
	}

	if err := model.RegisterCustomPgTypes(conn); err != nil {
		return nil, fmt.Errorf("registering custom pg types: %w", err)
	}

	labelArrayOID := model.GetCustomTypeOID(model.LabelArray)
	labelsCache, err := cache.NewInvertedLabelsCache(cfg.InvertedLabelsCacheSize)
	if err != nil {
		return nil, err
	}
	sw := NewSeriesWriter(conn, labelArrayOID, labelsCache, scache)
	elf := NewExamplarLabelFormatter(conn, eCache)

	for i := 0; i < numCopiers; i++ {
		go runCopier(conn, copierReadRequestCh, sw, elf)
	}

	inserter := &PgxDispatcher{
		conn:                   conn,
		metricTableNames:       mCache,
		scache:                 scache,
		invertedLabelsCache:    labelsCache,
		exemplarKeyPosCache:    eCache,
		completeMetricCreation: make(chan struct{}, 1),
		asyncAcks:              cfg.MetricsAsyncAcks,
		copierReadRequestCh:    copierReadRequestCh,
		// set to run at half our deletion interval
		seriesEpochRefresh: time.NewTicker(seriesEpochRefresh),
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

func (p *PgxDispatcher) runCompleteMetricCreationWorker() {
	for range p.completeMetricCreation {
		err := p.CompleteMetricCreation(context.Background())
		if err != nil {
			log.Warn("msg", "Got an error finalizing metric", "err", err)
		}
	}
}

func (p *PgxDispatcher) runSeriesEpochSync() {
	p.RefreshSeriesEpoch()
	for {
		select {
		case <-p.seriesEpochRefresh.C:
			p.RefreshSeriesEpoch()
		case <-p.doneChannel:
			return
		}
	}
}

func (p *PgxDispatcher) getDatabaseEpochs() (model.SeriesEpoch, model.SeriesEpoch, error) {
	var dbCurrentEpoch, dbDeleteEpoch int64
	row := p.conn.QueryRow(context.Background(), getEpochSQL)
	err := row.Scan(&dbCurrentEpoch, &dbDeleteEpoch)
	if err != nil {
		return 0, 0, err
	}
	return model.SeriesEpoch(dbCurrentEpoch), model.SeriesEpoch(dbDeleteEpoch), nil
}

func (p *PgxDispatcher) RefreshSeriesEpoch() {
	// There can be multiple concurrent executions of `RefreshSeriesEpoch`. We
	// use TryLock here to ensure that we don't have concurrent execution in
	// this function, and that we don't have multiple back-to-back executions.
	locked := p.seriesRefreshLock.TryLock()
	if !locked {
		log.Info("msg", "Skipping series cache refresh")
		return
	}
	defer p.seriesRefreshLock.Unlock()
	log.Info("msg", "Refreshing series cache epoch")
	cacheCurrentEpoch := p.scache.CacheEpoch()
	dbCurrentEpoch, dbDeleteEpoch, err := p.getDatabaseEpochs()
	log.Info("msg", "RefreshSeriesEpoch", "cacheCurrentEpoch", cacheCurrentEpoch, "dbCurrentEpoch", dbCurrentEpoch, "dbDeleteEpoch", dbDeleteEpoch)
	if err != nil {
		log.Warn("msg", "Unable to get database epoch data, will reset series and inverted labels caches", "err", err.Error())
		// Trash the cache just in case an epoch change occurred, seems safer
		p.scache.Reset(dbCurrentEpoch)
		// Also trash the inverted labels cache, which can also be invalidated when the series cache is
		p.invertedLabelsCache.Reset()
		return
	}
	if cacheCurrentEpoch.After(dbCurrentEpoch) {
		log.Warn("msg", "The connector's cache epoch is greater than the databases. This is unexpected", "connector_current_epoch", cacheCurrentEpoch, "database_current_epoch", dbCurrentEpoch)
		// Reset the caches. It's not really clear what would lead to this
		// situation, but if it does then something must have gone wrong...
		p.scache.Reset(dbCurrentEpoch)
		p.invertedLabelsCache.Reset()
		return
	}
	if dbDeleteEpoch.AfterEq(cacheCurrentEpoch) {
		// The current cache epoch has been overtaken by the database'
		// delete_epoch.
		// The only way to recover from this situation is to reset our caches
		// and let them repopulate.
		log.Warn("msg", "Cache epoch was overtaken by the database's delete epoch, resetting series and labels caches", "cache_epoch", cacheCurrentEpoch, "delete_epoch", dbDeleteEpoch)
		p.scache.Reset(dbCurrentEpoch)
		p.invertedLabelsCache.Reset()
		return
	} else {
		start := time.Now()
		staleSeriesIds, err := GetStaleSeriesIDs(p.conn)
		if err != nil {
			log.Warn("msg", "Error getting series ids, unable to update series cache", "err", err.Error())
			return
		}
		log.Info("msg", "Epoch change noticed, fetched stale series from db", "count", len(staleSeriesIds), "duration", time.Since(start))
		start = time.Now()
		evictCount := p.scache.EvictSeriesById(staleSeriesIds, dbCurrentEpoch)
		log.Info("msg", "Removed stale series", "count", evictCount, "duration", time.Since(start)) // Before merging in master, change the level to Debug.
		// Trash the inverted labels cache. Technically, we could determine
		// which label entries need to be removed from the cache, as we have
		// done for the series cache, but that doesn't seem to really be worth
		// it.
		p.invertedLabelsCache.Reset()
	}
}

func GetStaleSeriesIDs(conn pgxconn.PgxConn) ([]model.SeriesID, error) {
	staleSeriesIDs := make([]int64, 0)
	err := conn.QueryRow(context.Background(), getStaleSeriesIDsArraySQL).Scan(&staleSeriesIDs)
	if err != nil {
		return nil, fmt.Errorf("error getting stale series ids from db: %w", err)
	}
	staleSeriesIdsAsSeriesId := make([]model.SeriesID, len(staleSeriesIDs))
	for i := range staleSeriesIDs {
		staleSeriesIdsAsSeriesId[i] = model.SeriesID(staleSeriesIDs[i])
	}
	return staleSeriesIdsAsSeriesId, nil
}

func (p *PgxDispatcher) CompleteMetricCreation(ctx context.Context) error {
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

func (p *PgxDispatcher) Close() {
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
func (p *PgxDispatcher) InsertTs(ctx context.Context, dataTS model.Data) (uint64, error) {
	if p.closed.Load() {
		return 0, ErrDispatcherClosed
	}
	_, span := tracer.Default().Start(ctx, "dispatcher-insert-ts")
	defer span.End()
	var (
		numRows      uint64
		maxt         int64
		rows         = dataTS.Rows
		seriesEpoch  = dataTS.SeriesCacheEpoch
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
		p.getMetricBatcher(metricName) <- &insertDataRequest{spanCtx: span.SpanContext(), metric: metricName, seriesCacheEpoch: seriesEpoch, data: data, finished: workFinished, errChan: errChan}
	}
	span.SetAttributes(attribute.Int64("num_rows", int64(numRows)))
	span.SetAttributes(attribute.Int("num_metrics", len(rows)))
	reportIncomingBatch(numRows)
	reportOutgoing := func() {
		reportOutgoingBatch(numRows)
		reportBatchProcessingTime(dataTS.ReceivedTime)
	}

	var err error
	if !p.asyncAcks {
		workFinished.Wait()
		reportOutgoing()
		select {
		case err = <-errChan:
		default:
		}
		if errors.As(err, &InsertAbortError{}) {
			p.RefreshSeriesEpoch()
		}
		reportMetricsTelemetry(maxt, numRows, 0)
		close(errChan)
	} else {
		go func() {
			workFinished.Wait()
			reportOutgoing()
			select {
			case err = <-errChan:
			default:
			}
			close(errChan)
			if err != nil {
				if errors.As(err, &InsertAbortError{}) {
					p.RefreshSeriesEpoch()
				}
				log.Error("msg", fmt.Sprintf("error on async send, dropping %d datapoints", numRows), "err", err)
			}
			reportMetricsTelemetry(maxt, numRows, 0)
		}()
	}

	return numRows, err
}

func (p *PgxDispatcher) InsertMetadata(ctx context.Context, metadata []model.Metadata) (uint64, error) {
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
func (p *PgxDispatcher) getMetricBatcher(metric string) chan<- *insertDataRequest {
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
	spanCtx          trace.SpanContext
	metric           string
	seriesCacheEpoch model.SeriesEpoch
	finished         *sync.WaitGroup
	data             []model.Insertable
	errChan          chan error
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
