// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/allegro/bigcache"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/prompb"
)

const (
	promSchema       = "prom_api"
	seriesViewSchema = "prom_series"
	metricViewSchema = "prom_metric"
	dataSchema       = "prom_data"
	dataSeriesSchema = "prom_data_series"
	infoSchema       = "prom_info"
	catalogSchema    = "_prom_catalog"
	extSchema        = "_prom_ext"

	getMetricsTableSQL              = "SELECT table_name FROM " + catalogSchema + ".get_metric_table_name_if_exists($1)"
	getCreateMetricsTableSQL        = "SELECT table_name FROM " + catalogSchema + ".get_or_create_metric_table_name($1)"
	getCreateMetricsTableWithNewSQL = "SELECT table_name, possibly_new FROM " + catalogSchema + ".get_or_create_metric_table_name($1)"
	finalizeMetricCreation          = "CALL " + catalogSchema + ".finalize_metric_creation()"
	getSeriesIDForLabelSQL          = "SELECT * FROM " + catalogSchema + ".get_series_id_for_key_value_array($1, $2, $3)"
)

var (
	copyColumns         = []string{"time", "value", "series_id"}
	errMissingTableName = fmt.Errorf("missing metric table name")
)

type pgxBatch interface {
	Queue(query string, arguments ...interface{})
}

type pgxConn interface {
	Close()
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	CopyFromRows(rows [][]interface{}) pgx.CopyFromSource
	NewBatch() pgxBatch
	SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error)
}

// MetricCache provides a caching mechanism for metric table names.
type MetricCache interface {
	Get(metric string) (string, error)
	Set(metric string, tableName string) error
}

type pgxConnImpl struct {
	conn     *pgxpool.Pool
	readHist prometheus.ObserverVec
}

func (p *pgxConnImpl) getConn() *pgxpool.Pool {
	return p.conn
}

func (p *pgxConnImpl) Close() {
	conn := p.getConn()
	p.conn = nil
	conn.Close()
}

func (p *pgxConnImpl) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	conn := p.getConn()

	return conn.Exec(ctx, sql, arguments...)
}

func (p *pgxConnImpl) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	conn := p.getConn()
	if p.readHist != nil {
		defer func(start time.Time, hist prometheus.ObserverVec, path string) {
			elapsedMs := float64(time.Since(start).Milliseconds())
			hist.WithLabelValues(path).Observe(elapsedMs)
		}(time.Now(), p.readHist, sql[0:6])
	}

	return conn.Query(ctx, sql, args...)
}

func (p *pgxConnImpl) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	conn := p.getConn()

	return conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *pgxConnImpl) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource {
	return pgx.CopyFromRows(rows)
}

func (p *pgxConnImpl) NewBatch() pgxBatch {
	return &pgx.Batch{}
}

func (p *pgxConnImpl) SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error) {
	conn := p.getConn()

	return conn.SendBatch(ctx, b.(*pgx.Batch)), nil
}

// SampleInfoIterator is an iterator over a collection of sampleInfos that returns
// data in the format expected for the data table row.
type SampleInfoIterator struct {
	sampleInfos     []samplesInfo
	sampleInfoIndex int
	sampleIndex     int
	minSeen         int64
}

// NewSampleInfoIterator is the constructor
func NewSampleInfoIterator() SampleInfoIterator {
	si := SampleInfoIterator{sampleInfos: make([]samplesInfo, 0)}
	si.ResetPosition()
	return si
}

//Append adds a sample info to the back of the iterator
func (t *SampleInfoIterator) Append(s samplesInfo) {
	t.sampleInfos = append(t.sampleInfos, s)
}

//ResetPosition resets the iteration position to the beginning
func (t *SampleInfoIterator) ResetPosition() {
	t.sampleIndex = -1
	t.sampleInfoIndex = 0
	t.minSeen = math.MaxInt64
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (t *SampleInfoIterator) Next() bool {
	t.sampleIndex++
	if t.sampleInfoIndex < len(t.sampleInfos) && t.sampleIndex >= len(t.sampleInfos[t.sampleInfoIndex].samples) {
		t.sampleInfoIndex++
		t.sampleIndex = 0
	}
	return t.sampleInfoIndex < len(t.sampleInfos)
}

// Values returns the values for the current row
func (t *SampleInfoIterator) Values() ([]interface{}, error) {
	info := t.sampleInfos[t.sampleInfoIndex]
	sample := info.samples[t.sampleIndex]
	row := []interface{}{
		model.Time(sample.Timestamp).Time(),
		sample.Value,
		info.seriesID,
	}
	if t.minSeen > sample.Timestamp {
		t.minSeen = sample.Timestamp
	}
	return row, nil
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *SampleInfoIterator) Err() error {
	return nil
}

type Cfg struct {
	AsyncAcks      bool
	ReportInterval int
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

	series, _ := bigcache.NewBigCache(DefaultCacheConfig())

	bc := &bCache{
		series: series,
	}

	return &DBIngestor{
		db:    pi,
		cache: bc,
	}, nil
}

// NewPgxIngestor returns a new Ingestor that write to PostgreSQL using PGX
func NewPgxIngestor(c *pgxpool.Pool) (*DBIngestor, error) {
	metrics, _ := bigcache.NewBigCache(DefaultCacheConfig())
	cache := &MetricNameCache{metrics}
	return NewPgxIngestorWithMetricCache(c, cache, &Cfg{})
}

var ConnectionsPerProc = 5

func newPgxInserter(conn pgxConn, cache MetricCache, cfg *Cfg) (*pgxInserter, error) {
	cmc := make(chan struct{}, 1)

	maxProcs := runtime.GOMAXPROCS(-1)
	if maxProcs <= 0 {
		maxProcs = runtime.NumCPU()
	}
	if maxProcs <= 0 {
		maxProcs = 1
	}

	// we leave one connection per-core for other usages
	numCopiers := maxProcs*ConnectionsPerProc - maxProcs
	toCopiers := make(chan copyRequest, numCopiers)
	for i := 0; i < numCopiers; i++ {
		go runCopyFrom(conn, toCopiers)
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

type insertDataTask struct {
	finished *sync.WaitGroup
	errChan  chan error
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

func getMetricTableName(conn pgxConn, metric string) (string, bool, error) {
	res, err := conn.Query(
		context.Background(),
		getCreateMetricsTableWithNewSQL,
		metric,
	)

	if err != nil {
		return "", true, err
	}

	var tableName string
	var possiblyNew bool
	defer res.Close()
	if !res.Next() {
		return "", true, errMissingTableName
	}

	if err := res.Scan(&tableName, &possiblyNew); err != nil {
		return "", true, err
	}

	return tableName, possiblyNew, nil
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

func runInserterRoutineFailure(input chan insertDataRequest, err error) {
	for idr := range input {
		select {
		case idr.errChan <- fmt.Errorf("The insert routine has previously failed with %w", err):
		default:
		}
		idr.finished.Done()
	}
}

func runInserterRoutine(conn pgxConn, input chan insertDataRequest, metricName string, completeMetricCreationSignal chan struct{}, errChan chan error, metricTableNames MetricCache, toCopiers chan copyRequest) {
	tableName, err := metricTableNames.Get(metricName)
	if err == ErrEntryNotFound {
		var possiblyNew bool
		tableName, possiblyNew, err = getMetricTableName(conn, metricName)
		if err != nil {
			select {
			case errChan <- err:
			default:
			}
			//won't be able to insert anyway
			runInserterRoutineFailure(input, err)
			return
		} else {
			//ignone error since this is just an optimization
			_ = metricTableNames.Set(metricName, tableName)
		}

		if possiblyNew {
			//pass a signal if there is space
			select {
			case completeMetricCreationSignal <- struct{}{}:
			default:
			}
		}
	} else if err != nil {
		if err != nil {
			select {
			case errChan <- err:
			default:
			}
		}
		//won't be able to insert anyway
		runInserterRoutineFailure(input, err)
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

func runCopyFrom(conn pgxConn, in chan copyRequest) {
	for {
		req, ok := <-in
		if !ok {
			return
		}
		_, err := conn.CopyFrom(
			context.Background(),
			pgx.Identifier{dataSchema, req.table},
			copyColumns,
			&req.data.batch,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok && strings.Contains(pgErr.Message, "insert/update/delete not permitted") {
				/* If the error was that the table is already compressed, decompress and try again. */
				decompressErr := decompressChunks(conn, req.data, req.table)
				if decompressErr != nil {
					req.data.reportResults(err)
					pendingBuffers.Put(req.data)
					continue
				}

				req.data.batch.ResetPosition()
				_, err = conn.CopyFrom(
					context.Background(),
					pgx.Identifier{dataSchema, req.table},
					copyColumns,
					&req.data.batch,
				)
			}
		}

		req.data.reportResults(err)
		pendingBuffers.Put(req.data)
	}
}

func decompressChunks(conn pgxConn, pending *pendingBuffer, table string) error {
	log.Warn("msg", fmt.Sprintf("Table %s was compressed, decompressing", table), "table", table)
	minTime := model.Time(pending.batch.minSeen).Time()

	//how much faster are we at ingestion than wall-clock time?
	ingestSpeedup := 2
	//delay the next compression job proportional to the duration between now and the data time + a constant safety
	delayBy := (time.Since(minTime) / time.Duration(ingestSpeedup)) + time.Duration(60*time.Minute)
	maxDelayBy := time.Hour * 24
	if delayBy > maxDelayBy {
		delayBy = maxDelayBy
	}

	_, rescheduleErr := conn.Exec(context.Background(),
		`SELECT alter_job_schedule(
							(SELECT job_id
							FROM _timescaledb_config.bgw_policy_compress_chunks p
							INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = p.hypertable_id)
							WHERE h.schema_name = $1 and h.table_name = $2),
							next_start=>$3)`, dataSchema, table, time.Now().Add(delayBy))
	if rescheduleErr != nil {
		log.Error("msg", rescheduleErr, "context", "Rescheduling compression")
		return rescheduleErr
	}

	_, decompressErr := conn.Exec(context.Background(), "CALL "+catalogSchema+".decompress_chunks_after($1, $2);", table, minTime)
	if decompressErr != nil {
		log.Error("msg", decompressErr, "context", "Decompressing chunks")
		return decompressErr
	}

	return nil
}

func (pending *pendingBuffer) reportResults(err error) {
	for i := 0; i < len(pending.needsResponse); i++ {
		if err != nil {
			select {
			case pending.needsResponse[i].errChan <- err:
			default:
			}
		}
		pending.needsResponse[i].finished.Done()
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

// NewPgxReaderWithMetricCache returns a new DBReader that reads from PostgreSQL using PGX
// and caches metric table names using the supplied cacher.
func NewPgxReaderWithMetricCache(c *pgxpool.Pool, cache MetricCache) *DBReader {
	pi := &pgxQuerier{
		conn: &pgxConnImpl{
			conn: c,
		},
		metricTableNames: cache,
	}

	return &DBReader{
		db: pi,
	}
}

// NewPgxReader returns a new DBReader that reads that from PostgreSQL using PGX.
func NewPgxReader(c *pgxpool.Pool, readHist prometheus.ObserverVec) *DBReader {
	metrics, _ := bigcache.NewBigCache(DefaultCacheConfig())
	cache := &MetricNameCache{metrics}
	return NewPgxReaderWithMetricCache(c, cache)
}

type metricTimeRangeFilter struct {
	metric    string
	startTime string
	endTime   string
}

type pgxQuerier struct {
	conn             pgxConn
	metricTableNames MetricCache
}

// HealthCheck implements the healtchecker interface
func (q *pgxQuerier) HealthCheck() error {
	rows, err := q.conn.Query(context.Background(), "SELECT")

	if err != nil {
		return err
	}

	rows.Close()
	return nil
}

func (q *pgxQuerier) Select(mint int64, maxt int64, sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	rows, err := q.getResultRows(mint, maxt, ms)

	if err != nil {
		return nil, nil, err
	}

	return buildSeriesSet(rows, sortSeries)
}

func (q *pgxQuerier) Query(query *prompb.Query) ([]*prompb.TimeSeries, error) {
	if query == nil {
		return []*prompb.TimeSeries{}, nil
	}

	matchers, err := FromLabelMatchers(query.Matchers)

	if err != nil {
		return nil, err
	}

	rows, err := q.getResultRows(query.StartTimestampMs, query.EndTimestampMs, matchers)

	if err != nil {
		return nil, err
	}

	defer func() {
		for _, r := range rows {
			r.Close()
		}
	}()

	results := make([]*prompb.TimeSeries, 0, len(rows))

	for _, r := range rows {
		ts, err := buildTimeSeries(r)

		if err != nil {
			return nil, err
		}

		results = append(results, ts...)
	}

	return results, nil
}

func (q *pgxQuerier) getResultRows(startTimestamp int64, endTimestamp int64, matchers []*labels.Matcher) ([]pgx.Rows, error) {
	metric, cases, values, err := buildSubQueries(matchers)
	if err != nil {
		return nil, err
	}

	filter := metricTimeRangeFilter{
		metric:    metric,
		startTime: toRFC3339Nano(startTimestamp),
		endTime:   toRFC3339Nano(endTimestamp),
	}

	if metric != "" {
		return q.querySingleMetric(metric, filter, cases, values)
	}

	sqlQuery := buildMetricNameSeriesIDQuery(cases)
	rows, err := q.conn.Query(context.Background(), sqlQuery, values...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	metrics, series, err := getSeriesPerMetric(rows)

	if err != nil {
		return nil, err
	}

	results := make([]pgx.Rows, 0, len(metrics))

	for i, metric := range metrics {
		tableName, err := q.getMetricTableName(metric)
		if err != nil {
			// If the metric table is missing, there are no results for this query.
			if err == errMissingTableName {
				continue
			}

			return nil, err
		}
		filter.metric = tableName
		sqlQuery = buildTimeseriesBySeriesIDQuery(filter, series[i])
		rows, err = q.conn.Query(context.Background(), sqlQuery)

		if err != nil {
			return nil, err
		}

		results = append(results, rows)
	}

	return results, nil
}

func (q *pgxQuerier) querySingleMetric(metric string, filter metricTimeRangeFilter, cases []string, values []interface{}) ([]pgx.Rows, error) {
	tableName, err := q.getMetricTableName(metric)
	if err != nil {
		// If the metric table is missing, there are no results for this query.
		if err == errMissingTableName {
			return nil, nil
		}

		return nil, err
	}
	filter.metric = tableName

	sqlQuery := buildTimeseriesByLabelClausesQuery(filter, cases)
	rows, err := q.conn.Query(context.Background(), sqlQuery, values...)

	if err != nil {
		// If we are getting undefined table error, it means the query
		// is looking for a metric which doesn't exist in the system.
		if e, ok := err.(*pgconn.PgError); !ok || e.Code != pgerrcode.UndefinedTable {
			return nil, err
		}
	}
	return []pgx.Rows{rows}, nil
}

func (q *pgxQuerier) getMetricTableName(metric string) (string, error) {
	var err error
	var tableName string

	tableName, err = q.metricTableNames.Get(metric)

	if err == nil {
		return tableName, nil
	}

	if err != ErrEntryNotFound {
		return "", err
	}

	tableName, err = q.queryMetricTableName(metric)

	if err != nil {
		return "", err
	}

	err = q.metricTableNames.Set(metric, tableName)

	return tableName, err
}

func (q *pgxQuerier) queryMetricTableName(metric string) (string, error) {
	res, err := q.conn.Query(
		context.Background(),
		getMetricsTableSQL,
		metric,
	)

	if err != nil {
		return "", err
	}

	var tableName string
	defer res.Close()
	if !res.Next() {
		return "", errMissingTableName
	}

	if err := res.Scan(&tableName); err != nil {
		return "", err
	}

	return tableName, nil
}
