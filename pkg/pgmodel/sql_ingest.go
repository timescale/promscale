// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
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
			log.Info("msg", fmt.Sprintf("outputting throughpput info once every %ds", reportInterval))
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
			log.Warn("Got an error finalizing metric: %v", err)
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

func (idt *insertDataTask) reportResult(err error) {
	if err != nil {
		select {
		case idt.errChan <- err:
		default:
		}
	}
	idt.finished.Done()
}

func (p *pgxInserter) InsertData(rows map[string][]samplesInfo) (uint64, error) {
	var numRows uint64
	workFinished := &sync.WaitGroup{}
	workFinished.Add(len(rows))
	errChan := make(chan error, 1)
	for metricName, data := range rows {
		for _, si := range data {
			numRows += uint64(len(si.samples))
		}
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
				log.Error("msg", fmt.Sprintf("error on async send, dropping %d datapoints", numRows), "error", err)
			} else if p.insertedDatapoints != nil {
				atomic.AddInt64(p.insertedDatapoints, int64(numRows))
			}
		}()
	}

	return numRows, err
}

func (p *pgxInserter) insertMetricData(metric string, data []samplesInfo, finished *sync.WaitGroup, errChan chan error) {
	inserter := p.getMetricInserter(metric, errChan)
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

func (p *pgxInserter) getMetricInserter(metric string, errChan chan error) chan insertDataRequest {
	inserter, ok := p.inserters.Load(metric)
	if !ok {
		c := make(chan insertDataRequest, 1000)
		actual, old := p.inserters.LoadOrStore(metric, c)
		inserter = actual
		if !old {
			go runInserterRoutine(p.conn, c, metric, p.completeMetricCreation, errChan, p.metricTableNames, p.toCopiers)
		}
	}
	return inserter.(chan insertDataRequest)
}

type insertHandler struct {
	conn            pgxConn
	input           chan insertDataRequest
	pending         *pendingBuffer
	seriesCache     map[string]SeriesID
	metricTableName string
	toCopiers       chan copyRequest
}

type pendingBuffer struct {
	needsResponse []insertDataTask
	batch         SampleInfoIterator
}

const (
	flushSize = 2000
)

var pendingBuffers = sync.Pool{
	New: func() interface{} {
		pb := new(pendingBuffer)
		pb.needsResponse = make([]insertDataTask, 0)
		pb.batch = NewSampleInfoIterator()
		return pb
	},
}

type copyRequest struct {
	data  *pendingBuffer
	table string
}

func runInserterRoutine(conn pgxConn, input chan insertDataRequest, metricName string, completeMetricCreationSignal chan struct{}, errChan chan error, metricTableNames MetricCache, toCopiers chan copyRequest) {
	var tableName string
	var firstReq insertDataRequest
	firstReqSet := false
	for firstReq = range input {
		var err error
		tableName, err = initializeInserterRoutine(conn, metricName, completeMetricCreationSignal, metricTableNames)
		if err != nil {
			select {
			case errChan <- err:
			default:
			}
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
		conn:            conn,
		input:           input,
		pending:         pendingBuffers.Get().(*pendingBuffer),
		seriesCache:     make(map[string]SeriesID),
		metricTableName: tableName,
		toCopiers:       toCopiers,
	}

	handler.handleReq(firstReq)

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
	req, ok := <-h.input
	if !ok {
		return false
	}

	h.handleReq(req)

	return true
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
	h.fillKnowSeriesIds(req.data)
	needsFlush := h.pending.addReq(req)
	if needsFlush {
		h.flushPending()
		return true
	}
	return false
}

func (h *insertHandler) fillKnowSeriesIds(sampleInfos []samplesInfo) (numMissingSeries int) {
	for i, series := range sampleInfos {
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

func (h *insertHandler) flushPending() {
	_, err := h.setSeriesIds(h.pending.batch.sampleInfos)
	if err != nil {
		h.pending.reportResults(err)
		return
	}

	h.toCopiers <- copyRequest{h.pending, h.metricTableName}
	h.pending = pendingBuffers.Get().(*pendingBuffer)
}

func runInserter(conn pgxConn, in chan copyRequest) {
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
		log.Warn("msg", fmt.Sprintf("time out while processing error for %s", req.table), "error", err.Error())
		return err
	}

	return doInsert(conn, req)
}

// we can currently recover from two error:
// If we inserted duplicate data we switch to using INSERT ... ON CONFLICT DO NOTHING
// to skip redundant data points and try again.
// If we inserted into a compressed chunk, we decompress the chunk and try again.
// Since a single batch can have both errors, we need to remember the insert method
// we're using, so that we deduplicate if needed.
func tryRecovery(conn pgxConn, req copyRequest, err error) error {
	// we only recover from postgres errors right now
	pgErr, ok := err.(*pgconn.PgError)
	if !ok {
		errMsg := err.Error()
		log.Warn("msg", fmt.Sprintf("unexpected error while inserting to %s", req.table), "error", errMsg)
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

	log.Warn("msg", fmt.Sprintf("unexpected postgres error while inserting to %s", req.table), "error", pgErr.Error())
	return err
}

func doInsert(conn pgxConn, req copyRequest) (err error) {
	numRows := 0
	for i := range req.data.batch.sampleInfos {
		numRows += len(req.data.batch.sampleInfos[i].samples)
	}
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
	queryString := fmt.Sprintf("INSERT INTO %s(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a ON CONFLICT DO NOTHING", pgx.Identifier{dataSchema, req.table}.Sanitize())
	var ct pgconn.CommandTag
	ct, err = conn.Exec(context.Background(), queryString, times, vals, series)
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
}

func (h *insertHandler) setSeriesIds(sampleInfos []samplesInfo) (string, error) {
	numMissingSeries := h.fillKnowSeriesIds(sampleInfos)

	if numMissingSeries == 0 {
		return "", nil
	}

	seriesToInsert := make([]*samplesInfo, 0, numMissingSeries)
	for i, series := range sampleInfos {
		if series.seriesID < 0 {
			seriesToInsert = append(seriesToInsert, &sampleInfos[i])
		}
	}
	var lastSeenLabel *Labels

	batch := h.conn.NewBatch()
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

	br, err := h.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return "", err
	}
	defer br.Close()

	if numSQLFunctionCalls != len(batchSeries) {
		return "", fmt.Errorf("unexpected difference in numQueries and batchSeries")
	}

	var tableName string
	for i := 0; i < numSQLFunctionCalls; i++ {
		_, err = br.Exec()
		if err != nil {
			return "", err
		}
		row := br.QueryRow()

		var id SeriesID
		err = row.Scan(&tableName, &id)
		if err != nil {
			return "", err
		}
		h.seriesCache[batchSeries[i][0].labels.String()] = id
		for _, lsi := range batchSeries[i] {
			lsi.seriesID = id
		}
		_, err = br.Exec()
		if err != nil {
			return "", err
		}
	}

	return tableName, nil
}

func (p *pendingBuffer) addReq(req insertDataRequest) bool {
	p.needsResponse = append(p.needsResponse, insertDataTask{finished: req.finished, errChan: req.errChan})
	p.batch.sampleInfos = append(p.batch.sampleInfos, req.data...)
	return len(p.batch.sampleInfos) > flushSize
}
