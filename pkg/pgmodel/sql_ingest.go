// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

// See the section on the Write path in the Readme for a high level overview of
// this file.
package pgmodel

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/log"
)

const (
	getCreateMetricsTableSQL = "SELECT table_name FROM " + catalogSchema + ".get_or_create_metric_table_name($1)"
	finalizeMetricCreation   = "CALL " + catalogSchema + ".finalize_metric_creation()"
	getSeriesIDForLabelSQL   = "SELECT * FROM " + catalogSchema + ".get_or_create_series_id_for_kv_array($1, $2, $3)"
	getEpochSQL              = "SELECT current_epoch FROM " + catalogSchema + ".ids_epoch LIMIT 1"
)

type Cfg struct {
	AsyncAcks       bool
	ReportInterval  int
	SeriesCacheSize uint64
	NumCopiers      int
}

// NewPgxIngestorWithMetricCache returns a new Ingestor that uses connection pool and a metrics cache
// for caching metric table names.
func NewPgxIngestorWithMetricCache(c *pgxpool.Pool, cache MetricCache, cfg *Cfg) (*DBIngestor, error) {

	conn := &pgxConnImpl{
		conn: c,
	}

	pi, err := newPgxInserter(conn, cache, cfg)
	if err != nil {
		return nil, err
	}

	return &DBIngestor{db: pi}, nil
}

// NewPgxIngestor returns a new Ingestor that write to PostgreSQL using PGX
func NewPgxIngestor(c *pgxpool.Pool) (*DBIngestor, error) {
	cache := &MetricNameCache{clockcache.WithMax(DefaultMetricCacheSize)}
	return NewPgxIngestorWithMetricCache(c, cache, &Cfg{})
}

func newPgxInserter(conn pgxConn, cache MetricCache, cfg *Cfg) (*pgxInserter, error) {
	cmc := make(chan struct{}, 1)

	numCopiers := cfg.NumCopiers
	if numCopiers < 1 {
		log.Warn("msg", "num copiers less than 1, setting to 1")
		numCopiers = 1
	}

	// We run inserters bus-style: all of them competing to grab requests off a
	// single channel. This should offer a decent compromise between batching
	// and balancing: if an inserter is awake and has little work, it'll be more
	// likely to win the race, while one that's busy or asleep won't.
	toCopiers := make(chan copyRequest, numCopiers)
	for i := 0; i < numCopiers; i++ {
		go runInserter(conn, toCopiers)
	}

	inserter := &pgxInserter{
		conn:                   conn,
		metricTableNames:       cache,
		completeMetricCreation: cmc,
		asyncAcks:              cfg.AsyncAcks,
		toCopiers:              toCopiers,
	}
	if cfg.AsyncAcks && cfg.ReportInterval > 0 {
		inserter.insertedDatapoints = new(int64)
		reportInterval := int64(cfg.ReportInterval)
		go func() {
			log.Info("msg", fmt.Sprintf("outputting throughput info once every %ds", reportInterval))
			tick := time.Tick(time.Duration(reportInterval) * time.Second)
			for range tick {
				inserted := atomic.SwapInt64(inserter.insertedDatapoints, 0)
				log.Info("msg", "Samples write throughput", "samples/sec", inserted/reportInterval)
			}
		}()
	}
	//on startup run a completeMetricCreation to recover any potentially
	//incomplete metric
	err := inserter.CompleteMetricCreation()
	if err != nil {
		return nil, err
	}

	go inserter.runCompleteMetricCreationWorker()

	return inserter, nil
}

type pgxInserter struct {
	conn                   pgxConn
	metricTableNames       MetricCache
	inserters              sync.Map
	completeMetricCreation chan struct{}
	asyncAcks              bool
	insertedDatapoints     *int64
	toCopiers              chan copyRequest
}

func (p *pgxInserter) CompleteMetricCreation() error {
	_, err := p.conn.Exec(
		context.Background(),
		finalizeMetricCreation,
	)
	return err
}

func (p *pgxInserter) runCompleteMetricCreationWorker() {
	for range p.completeMetricCreation {
		err := p.CompleteMetricCreation()
		if err != nil {
			log.Warn("msg", "Got an error finalizing metric", "err", err)
		}
	}
}

func (p *pgxInserter) Close() {
	close(p.completeMetricCreation)
	p.inserters.Range(func(key, value interface{}) bool {
		close(value.(chan insertDataRequest))
		return true
	})
	close(p.toCopiers)
}

func (p *pgxInserter) InsertNewData(rows map[string][]samplesInfo) (uint64, error) {
	return p.InsertData(rows)
}

type insertDataRequest struct {
	metric   string
	data     []samplesInfo
	finished *sync.WaitGroup
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

type insertDataTask struct {
	finished *sync.WaitGroup
	errChan  chan error
}

// Report that this task is completed, along with any error that may have
// occured. Since this is a backedge on the goroutine graph, it
// _must never block_: blocking here will cause deadlocks.
func (idt *insertDataTask) reportResult(err error) {
	if err != nil {
		select {
		case idt.errChan <- err:
		default:
		}
	}
	idt.finished.Done()
}

// Insert a batch of data into the DB.
// The data should be grouped by metric name.
// returns the number of rows we intended to insert (_not_ how many were
// actually inserted) and any error.
// Though we may insert data to multiple tables concurrently, if asyncAcks is
// unset this function will wait until _all_ the insert attempts have completed.
func (p *pgxInserter) InsertData(rows map[string][]samplesInfo) (uint64, error) {
	var numRows uint64
	workFinished := &sync.WaitGroup{}
	workFinished.Add(len(rows))
	// we only allocate enough space for a single error message here as we only
	// report one error back upstream. The inserter should not block on this
	// channel, but only insert if it's empty, anything else can deadlock.
	errChan := make(chan error, 1)
	for metricName, data := range rows {
		for _, si := range data {
			numRows += uint64(len(si.samples))
		}
		// insertMetricData() is expected to be non-blocking,
		// just a channel insert
		p.insertMetricData(metricName, data, workFinished, errChan)
	}

	var err error
	if !p.asyncAcks {
		workFinished.Wait()
		select {
		case err = <-errChan:
		default:
		}
		close(errChan)
	} else {
		go func() {
			workFinished.Wait()
			select {
			case err = <-errChan:
			default:
			}
			close(errChan)
			if err != nil {
				log.Error("msg", fmt.Sprintf("error on async send, dropping %d datapoints", numRows), "err", err)
			} else if p.insertedDatapoints != nil {
				atomic.AddInt64(p.insertedDatapoints, int64(numRows))
			}
		}()
	}

	return numRows, err
}

func (p *pgxInserter) insertMetricData(metric string, data []samplesInfo, finished *sync.WaitGroup, errChan chan error) {
	inserter := p.getMetricInserter(metric)
	inserter <- insertDataRequest{metric: metric, data: data, finished: finished, errChan: errChan}
}

//nolint
func (p *pgxInserter) createMetricTable(metric string) (string, error) {
	res, err := p.conn.Query(
		context.Background(),
		getCreateMetricsTableSQL,
		metric,
	)

	if err != nil {
		return "", err
	}

	var tableName string
	defer res.Close()
	if !res.Next() {
		err = res.Err()
		if err != nil {
			return "", err
		}
		return "", errMissingTableName
	}

	if err := res.Scan(&tableName); err != nil {
		return "", err
	}

	return tableName, nil
}

//nolint
func (p *pgxInserter) getMetricTableName(metric string) (string, error) {
	var err error
	var tableName string

	tableName, err = p.metricTableNames.Get(metric)

	if err == nil {
		return tableName, nil
	}

	if err != ErrEntryNotFound {
		return "", err
	}

	tableName, err = p.createMetricTable(metric)

	if err != nil {
		return "", err
	}

	err = p.metricTableNames.Set(metric, tableName)

	return tableName, err
}

// Get the handler for a given metric name, creating a new one if none exists
func (p *pgxInserter) getMetricInserter(metric string) chan insertDataRequest {
	inserter, ok := p.inserters.Load(metric)
	if !ok {
		// The ordering is important here: we need to ensure that every call
		// to getMetricInserter() returns the same inserter. Therefore, we can
		// only start up the inserter routine if we know that we won the race
		// to create the inserter, anything else will leave a zombie inserter
		// lying around.
		c := make(chan insertDataRequest, 1000)
		actual, old := p.inserters.LoadOrStore(metric, c)
		inserter = actual
		if !old {
			go runInserterRoutine(p.conn, c, metric, p.completeMetricCreation, p.metricTableNames, p.toCopiers)
		}
	}
	return inserter.(chan insertDataRequest)
}

type insertHandler struct {
	conn               pgxConn
	input              chan insertDataRequest
	pending            *pendingBuffer
	seriesCache        map[string]SeriesID
	seriesCacheEpoch   Epoch
	seriesCacheRefresh *time.Ticker
	metricTableName    string
	toCopiers          chan copyRequest
}

// epoch for the ID caches, -1 means that the epoch was not set
type Epoch = int64

type pendingBuffer struct {
	needsResponse []insertDataTask
	batch         SampleInfoIterator
	epoch         Epoch
}

const (
	// maximum number of insertDataRequests that should be buffered before the
	// insertHandler flushes to the next layer. We don't want too many as this
	// increases the number of lost writes if the connector dies. This number
	// was chosen arbitrarily.
	flushSize = 2000
)

var pendingBuffers = sync.Pool{
	New: func() interface{} {
		pb := new(pendingBuffer)
		pb.needsResponse = make([]insertDataTask, 0)
		pb.batch = NewSampleInfoIterator()
		pb.epoch = -1
		return pb
	},
}

type copyRequest struct {
	data  *pendingBuffer
	table string
}

func runInserterRoutine(conn pgxConn, input chan insertDataRequest, metricName string, completeMetricCreationSignal chan struct{}, metricTableNames MetricCache, toCopiers chan copyRequest) {
	var tableName string
	var firstReq insertDataRequest
	firstReqSet := false
	for firstReq = range input {
		var err error
		tableName, err = initializeInserterRoutine(conn, metricName, completeMetricCreationSignal, metricTableNames)
		if err != nil {
			firstReq.reportResult(fmt.Errorf("Initializing the insert routine has failed with %w", err))
		} else {
			firstReqSet = true
			break
		}
	}

	//input channel was closed before getting a successful request
	if !firstReqSet {
		return
	}

	handler := insertHandler{
		conn:             conn,
		input:            input,
		pending:          pendingBuffers.Get().(*pendingBuffer),
		seriesCache:      make(map[string]SeriesID),
		seriesCacheEpoch: -1,
		// set to run at half our deletion interval
		seriesCacheRefresh: time.NewTicker(30 * time.Minute),
		metricTableName:    tableName,
		toCopiers:          toCopiers,
	}

	handler.handleReq(firstReq)

	// Grab new requests from our channel and handle them. We do this hot-load
	// style: we keep grabbing requests off the channel while we can do so
	// without blocking, and flush them to the next layer when we run out, or
	// reach a predetermined threshold. The theory is that wake/sleep and
	// flushing is relatively expensive, and can be easily amortized over
	// multiple requests, so it pays to batch as much as we are able. However,
	// writes to a given metric can be relatively rare, so if we don't have
	// additional requests immediately we're likely not going to for a while.
	for {
		if !handler.hasPendingReqs() {
			stillAlive := handler.blockingHandleReq()
			if !stillAlive {
				return
			}
			continue
		}

	hotReceive:
		for handler.nonblockingHandleReq() {
			if len(handler.pending.batch.sampleInfos) >= flushSize {
				break hotReceive
			}
		}

		handler.flush()
	}
}

// Create the metric table for the metric we handle, if it does not already
// exist. This only does the most critical part of metric table creation, the
// rest is handled by completeMetricTableCreation().
func initializeInserterRoutine(conn pgxConn, metricName string, completeMetricCreationSignal chan struct{}, metricTableNames MetricCache) (tableName string, err error) {
	tableName, err = metricTableNames.Get(metricName)
	if err == ErrEntryNotFound {
		var possiblyNew bool
		tableName, possiblyNew, err = getMetricTableName(conn, metricName)
		if err != nil {
			return "", err
		}

		//ignone error since this is just an optimization
		_ = metricTableNames.Set(metricName, tableName)

		if possiblyNew {
			//pass a signal if there is space
			select {
			case completeMetricCreationSignal <- struct{}{}:
			default:
			}
		}
	} else if err != nil {
		return "", err
	}
	return tableName, err
}

func (h *insertHandler) hasPendingReqs() bool {
	return len(h.pending.batch.sampleInfos) > 0
}

func (h *insertHandler) blockingHandleReq() bool {
	for {
		select {
		case req, ok := <-h.input:
			if !ok {
				return false
			}

			h.handleReq(req)

			return true
		case <-h.seriesCacheRefresh.C:
			h.refreshSeriesCache()
		}
	}
}

func (h *insertHandler) nonblockingHandleReq() bool {
	select {
	case req := <-h.input:
		h.handleReq(req)
		return true
	default:
		return false
	}
}

func (h *insertHandler) handleReq(req insertDataRequest) bool {
	// we fill in any SeriesIds we have in cache now so we can free any Labels
	// that are no longer needed, and because the SeriesIds might get flushed.
	// (neither of these are that critical at the moment)
	_, epoch := h.fillKnownSeriesIds(req.data)
	needsFlush := h.pending.addReq(req, epoch)
	if needsFlush {
		h.flushPending()
		return true
	}
	return false
}

// Fill in any SeriesIds we already have in cache.
// This must be idempotent: if called a second time it should not affect any
// sampleInfo whose series was already set.
func (h *insertHandler) fillKnownSeriesIds(sampleInfos []samplesInfo) (numMissingSeries int, epoch Epoch) {
	epoch = h.seriesCacheEpoch
	for i, series := range sampleInfos {
		// When we first create the sampleInfos we should have set the seriesID
		// to -1 for any series whose labels field is nil. Real seriesIds must
		// always be greater than 0.
		if series.seriesID > -1 {
			continue
		}
		id, ok := h.seriesCache[series.labels.String()]
		if ok {
			sampleInfos[i].seriesID = id
			series.labels = nil
		} else {
			numMissingSeries++
		}
	}
	return
}

func (h *insertHandler) flush() {
	if !h.hasPendingReqs() {
		return
	}
	h.flushPending()
}

// Set all unset SeriesIds and flush to the next layer
func (h *insertHandler) flushPending() {
	_, epoch, err := h.setSeriesIds(h.pending.batch.sampleInfos)
	if err != nil {
		h.pending.reportResults(err)
		return
	}

	h.pending.addEpoch(epoch)

	h.toCopiers <- copyRequest{h.pending, h.metricTableName}
	h.pending = pendingBuffers.Get().(*pendingBuffer)
}

// Handles actual insertion into the DB.
// We have one of these per connection reserved for insertion.
func runInserter(conn pgxConn, in chan copyRequest) {
	// We grab copyRequests off the channel one at a time. This, and the name is
	// a legacy from when we used CopyFrom to perform the insetions, and may
	// change in the future.
	for {
		req, ok := <-in
		if !ok {
			return
		}
		err := doInsert(conn, req)
		if err != nil {
			err = insertErrorFallback(conn, req, err)
		}

		req.data.reportResults(err)
		pendingBuffers.Put(req.data)
	}
}

// certain errors are recoverable, handle those we can
//   1. if the table is compressed, decompress and retry the insertion
func insertErrorFallback(conn pgxConn, req copyRequest, err error) error {
	err = tryRecovery(conn, req, err)
	if err != nil {
		log.Warn("msg", fmt.Sprintf("time out while processing error for %s", req.table), "err", err.Error())
		return err
	}

	return doInsert(conn, req)
}

// we can currently recover from one error:
// If we inserted into a compressed chunk, we decompress the chunk and try again.
// Since a single batch can have both errors, we need to remember the insert method
// we're using, so that we deduplicate if needed.
func tryRecovery(conn pgxConn, req copyRequest, err error) error {
	// we only recover from postgres errors right now
	pgErr, ok := err.(*pgconn.PgError)
	if !ok {
		errMsg := err.Error()
		log.Warn("msg", fmt.Sprintf("unexpected error while inserting to %s", req.table), "err", errMsg)
		return err
	}

	// If the error was that the table is already compressed, decompress and try again.
	if strings.Contains(pgErr.Message, "insert/update/delete not permitted") {
		decompressErr := decompressChunks(conn, req.data, req.table)
		if decompressErr != nil {
			return err
		}

		req.data.batch.ResetPosition()
		return nil
	}

	log.Warn("msg", fmt.Sprintf("unexpected postgres error while inserting to %s", req.table), "err", pgErr.Error())
	return err
}

// Perform the actual insertion into the DB.
func doInsert(conn pgxConn, req copyRequest) (err error) {
	numRows := 0
	for i := range req.data.batch.sampleInfos {
		numRows += len(req.data.batch.sampleInfos[i].samples)
	}
	// flatten the various series into arrays.
	// there are four main bottlenecks for insertion:
	//   1. The round trip time.
	//   2. The number of requests sent.
	//   3. The number of individual INSERT statements.
	//   4. The amount of data sent.
	// While the first two of these can be handled by batching, for the latter
	// two we need to actually reduce the work done. It turns out that simply
	// collecting all the data into a postgres array and performing a single
	// INSERT using that overcomes most of the performance issues for sending
	// multiple data, and brings INSERT nearly on par with CopyFrom. In the
	// future we may wish to send compressed data instead.
	times := make([]time.Time, 0, numRows)
	vals := make([]float64, 0, numRows)
	series := make([]int64, 0, numRows)
	for req.data.batch.Next() {
		time, val, serie := req.data.batch.Values()
		times = append(times, time)
		vals = append(vals, val)
		series = append(series, int64(serie))
	}
	if len(times) != numRows {
		panic("invalid insert request")
	}

	batch := conn.NewBatch()

	epochCheck := fmt.Sprintf("SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN %s.epoch_abort($1) END FROM %s.ids_epoch LIMIT 1", catalogSchema, catalogSchema)
	batch.Queue(epochCheck, req.data.epoch)

	dataInsert := fmt.Sprintf("INSERT INTO %s(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING", pgx.Identifier{dataSchema, req.table}.Sanitize())
	batch.Queue(dataInsert, times, vals, series)

	var ct pgconn.CommandTag
	var res pgx.BatchResults
	res, err = conn.SendBatch(context.Background(), batch)
	if err != nil {
		return
	}
	defer func() { _ = res.Close() }()

	var val []byte
	row := res.QueryRow()
	err = row.Scan(&val)
	if err != nil {
		return
	}

	ct, err = res.Exec()
	if err != nil {
		return
	}

	if int64(numRows) != ct.RowsAffected() {
		log.Warn("msg", "duplicate data in sample", "table", req.table, "duplicate_count", int64(numRows)-ct.RowsAffected(), "row_count", numRows)
		duplicateSamples.Add(float64(int64(numRows) - ct.RowsAffected()))
		duplicateWrites.Inc()
	}
	return nil
}

// In the event we filling in old data and the chunk we want to INSERT into has
// already been compressed, we decompress the chunk and try again. When we do
// this we delay the recompression to give us time to insert additional data.
func decompressChunks(conn pgxConn, pending *pendingBuffer, table string) error {
	minTime := model.Time(pending.batch.minSeen).Time()

	//how much faster are we at ingestion than wall-clock time?
	ingestSpeedup := 2
	//delay the next compression job proportional to the duration between now and the data time + a constant safety
	delayBy := (time.Since(minTime) / time.Duration(ingestSpeedup)) + time.Duration(60*time.Minute)
	maxDelayBy := time.Hour * 24
	if delayBy > maxDelayBy {
		delayBy = maxDelayBy
	}
	log.Warn("msg", fmt.Sprintf("Table %s was compressed, decompressing", table), "table", table, "min-time", minTime, "age", time.Since(minTime), "delay-job-by", delayBy)

	_, rescheduleErr := conn.Exec(context.Background(), "SELECT "+catalogSchema+".delay_compression_job($1, $2)",
		table, time.Now().Add(delayBy))
	if rescheduleErr != nil {
		log.Error("msg", rescheduleErr, "context", "Rescheduling compression")
		return rescheduleErr
	}

	_, decompressErr := conn.Exec(context.Background(), "CALL "+catalogSchema+".decompress_chunks_after($1, $2);", table, minTime)
	if decompressErr != nil {
		log.Error("msg", decompressErr, "context", "Decompressing chunks")
		return decompressErr
	}

	decompressCalls.Inc()
	decompressEarliest.WithLabelValues(table).Set(float64(minTime.UnixNano()) / 1e9)
	return nil
}

// Report completion of an insert batch to all goroutines that may be waiting
// on it, along with any error that may have occurred.
// This function also resets the pending in preperation for the next batch.
func (pending *pendingBuffer) reportResults(err error) {
	for i := 0; i < len(pending.needsResponse); i++ {
		pending.needsResponse[i].reportResult(err)
		pending.needsResponse[i] = insertDataTask{}
	}
	pending.needsResponse = pending.needsResponse[:0]

	for i := 0; i < len(pending.batch.sampleInfos); i++ {
		// nil all pointers to prevent memory leaks
		pending.batch.sampleInfos[i] = samplesInfo{}
	}
	pending.batch = SampleInfoIterator{sampleInfos: pending.batch.sampleInfos[:0]}
	pending.batch.ResetPosition()
	pending.epoch = -1
}

// Set all seriesIds for a samplesInfo, fetching any missing ones from the DB,
// and repopulating the cache accordingly.
// returns: the tableName for the metric being inserted into
// TODO move up to the rest of insertHandler
func (h *insertHandler) setSeriesIds(sampleInfos []samplesInfo) (string, Epoch, error) {
	numMissingSeries, epoch := h.fillKnownSeriesIds(sampleInfos)

	if numMissingSeries == 0 {
		return "", epoch, nil
	}

	seriesToInsert := make([]*samplesInfo, 0, numMissingSeries)
	for i, series := range sampleInfos {
		if series.seriesID < 0 {
			seriesToInsert = append(seriesToInsert, &sampleInfos[i])
		}
	}
	var lastSeenLabel *Labels

	batch := h.conn.NewBatch()

	// The epoch will never decrease, so we can check it once at the beginning,
	// at worst we'll store too small an epoch, which is always safe
	batch.Queue("BEGIN;")
	batch.Queue(getEpochSQL)
	batch.Queue("COMMIT;")

	numSQLFunctionCalls := 0
	// Sort and remove duplicates. The sort is needed to remove duplicates. Each series is inserted
	// in a different transaction, thus deadlocks are not an issue.
	sort.Slice(seriesToInsert, func(i, j int) bool {
		return seriesToInsert[i].labels.Compare(seriesToInsert[j].labels) < 0
	})

	batchSeries := make([][]*samplesInfo, 0, len(seriesToInsert))
	// group the seriesToInsert by labels, one slice array per unique labels
	for _, curr := range seriesToInsert {
		if lastSeenLabel != nil && lastSeenLabel.Equal(curr.labels) {
			batchSeries[len(batchSeries)-1] = append(batchSeries[len(batchSeries)-1], curr)
			continue
		}

		batch.Queue("BEGIN;")
		batch.Queue(getSeriesIDForLabelSQL, curr.labels.metricName, curr.labels.names, curr.labels.values)
		batch.Queue("COMMIT;")
		numSQLFunctionCalls++
		batchSeries = append(batchSeries, []*samplesInfo{curr})

		lastSeenLabel = curr.labels
	}

	if numSQLFunctionCalls != len(batchSeries) {
		return "", epoch, fmt.Errorf("unexpected difference in numQueries and batchSeries")
	}

	br, err := h.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return "", epoch, err
	}
	defer br.Close()

	// BEGIN;
	_, err = br.Exec()
	if err != nil {
		return "", epoch, err
	}

	var newEpoch int64
	row := br.QueryRow()
	err = row.Scan(&newEpoch)
	if err != nil {
		return "", epoch, err
	}
	if epoch == -1 || newEpoch < epoch {
		epoch = newEpoch
	}
	if h.seriesCacheEpoch == -1 || newEpoch < h.seriesCacheEpoch {
		h.seriesCacheEpoch = newEpoch
	}

	// COMMIT;
	_, err = br.Exec()
	if err != nil {
		return "", epoch, err
	}

	var tableName string
	for i := 0; i < numSQLFunctionCalls; i++ {
		// BEGIN;
		_, err = br.Exec()
		if err != nil {
			return "", epoch, err
		}

		var id SeriesID
		row = br.QueryRow()
		err = row.Scan(&tableName, &id)
		if err != nil {
			return "", epoch, err
		}
		seriesString := batchSeries[i][0].labels.String()
		h.seriesCache[seriesString] = id
		for _, lsi := range batchSeries[i] {
			lsi.seriesID = id
		}

		// COMMIT;
		_, err = br.Exec()
		if err != nil {
			return "", epoch, err
		}
	}

	return tableName, epoch, nil
}

func (h *insertHandler) refreshSeriesCache() {
	newEpoch, err := h.getServerEpoch()
	if err != nil {
		// we don't have any great place to report this error, and if the
		// connection recovers we can still make progress, so we'll just log it
		// and continue execution
		msg := fmt.Sprintf("error refreshing the series cache for %s", h.metricTableName)
		log.Error("msg", msg, "metric_table", h.metricTableName, "err", err)
		// Trash the cache just in case an epoch change occured, seems safer
		h.seriesCache = map[string]SeriesID{}
		h.seriesCacheEpoch = -1
		return
	}

	if newEpoch != h.seriesCacheEpoch {
		h.seriesCache = map[string]SeriesID{}
		h.seriesCacheEpoch = newEpoch
	}
}

func (h *insertHandler) getServerEpoch() (Epoch, error) {
	var newEpoch int64
	row := h.conn.QueryRow(context.Background(), getEpochSQL)
	err := row.Scan(&newEpoch)
	if err != nil {
		return -1, err
	}

	return newEpoch, nil
}

func (p *pendingBuffer) addReq(req insertDataRequest, epoch Epoch) bool {
	p.addEpoch(epoch)
	p.needsResponse = append(p.needsResponse, insertDataTask{finished: req.finished, errChan: req.errChan})
	p.batch.sampleInfos = append(p.batch.sampleInfos, req.data...)
	return len(p.batch.sampleInfos) > flushSize
}

func (p *pendingBuffer) addEpoch(epoch Epoch) {
	if p.epoch == -1 || epoch < p.epoch {
		p.epoch = epoch
	}
}
