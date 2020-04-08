// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package pgmodel

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/allegro/bigcache"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

const (
	promSchema    = "prom"
	catalogSchema = "_prom_catalog"

	getMetricsTableSQL       = "SELECT table_name FROM " + promSchema + ".get_metric_table_name_if_exists($1)"
	getCreateMetricsTableSQL = "SELECT table_name FROM " + promSchema + ".get_or_create_metric_table_name($1)"
	getSeriesIDForLabelSQL   = "SELECT " + promSchema + ".get_series_id_for_key_value_array($1, $2, $3)"

	subQueryEQ            = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value = $%d)"
	subQueryEQMatchEmpty  = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value != $%d)"
	subQueryNEQ           = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value = $%d) "
	subQueryNEQMatchEmpty = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value != $%d)"
	subQueryRE            = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value ~ $%d) "
	subQueryREMatchEmpty  = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value !~ $%d)"
	subQueryNRE           = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value ~ $%d)"
	subQueryNREMatchEmpty = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value !~ $%d)"

	metricNameSeriesIDSQLFormat = `SELECT m.metric_name, array_agg(s.id)
	FROM _prom_catalog.series s
	INNER JOIN _prom_catalog.metric m
	ON (m.id = s.metric_id)
	WHERE %s
	GROUP BY m.metric_name`

	timeseriesByMetricSQLFormat = `SELECT ` + promSchema + `.label_array_to_jsonb(s.labels), array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)
	FROM %s m
	INNER JOIN _prom_catalog.series s
	ON m.series_id = s.id
	WHERE %s
	AND time >= '%s'
	AND time <= '%s'
	GROUP BY s.id`

	timeseriesBySeriesIDsSQLFormat = `SELECT ` + promSchema + `.label_array_to_jsonb(s.labels), array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)
	FROM %s m
	INNER JOIN _prom_catalog.series s
	ON m.series_id = s.id
	WHERE m.series_id IN (%s)
	AND time >= '%s'
	AND time <= '%s'
	GROUP BY s.id`
)

var (
	copyColumns           = []string{"time", "value", "series_id"}
	errMissingTableName   = fmt.Errorf("missing metric table name")
	errInvalidLabelsValue = func(s string) error { return fmt.Errorf("invalid labels value %s", s) }
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
	conn *pgxpool.Pool
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
	sampleInfos     []*samplesInfo
	sampleInfoIndex int
	sampleIndex     int
}

// NewSampleInfoIterator is the constructor
func NewSampleInfoIterator() *SampleInfoIterator {
	return &SampleInfoIterator{sampleInfos: make([]*samplesInfo, 0), sampleIndex: -1, sampleInfoIndex: 0}
}

//Append adds a sample info to the back of the iterator
func (t *SampleInfoIterator) Append(s *samplesInfo) {
	t.sampleInfos = append(t.sampleInfos, s)
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
	return row, nil
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *SampleInfoIterator) Err() error {
	return nil
}

// NewPgxIngestorWithMetricCache returns a new Ingestor that uses connection pool and a metrics cache
// for caching metric table names.
func NewPgxIngestorWithMetricCache(c *pgxpool.Pool, cache MetricCache) *DBIngestor {

	conn := &pgxConnImpl{
		conn: c,
	}

	pi := newPgxInserter(conn, cache)

	config := bigcache.DefaultConfig(10 * time.Minute)
	series, _ := bigcache.NewBigCache(config)

	bc := &bCache{
		series: series,
	}

	return &DBIngestor{
		db:    pi,
		cache: bc,
	}
}

// NewPgxIngestor returns a new Ingestor that write to PostgreSQL using PGX
func NewPgxIngestor(c *pgxpool.Pool) *DBIngestor {
	config := bigcache.DefaultConfig(10 * time.Minute)
	metrics, _ := bigcache.NewBigCache(config)
	cache := &MetricNameCache{metrics}
	return NewPgxIngestorWithMetricCache(c, cache)
}

func newPgxInserter(conn pgxConn, cache MetricCache) *pgxInserter {
	maxProcs := runtime.GOMAXPROCS(-1)
	if maxProcs <= 0 {
		maxProcs = runtime.NumCPU()
	}
	if maxProcs <= 0 {
		maxProcs = 1
	}
	inserters := make([]chan insertDataRequest, maxProcs)
	for i := 0; i < maxProcs; i++ {
		ch := make(chan insertDataRequest, 1000)
		inserters[i] = ch
		go runInserterRoutine(conn, ch)
	}

	return &pgxInserter{
		conn:             conn,
		metricTableNames: cache,
		inserters:        inserters,
		seed:             maphash.MakeSeed(),
	}
}

type pgxInserter struct {
	conn             pgxConn
	metricTableNames MetricCache
	inserters        []chan insertDataRequest
	seed             maphash.Seed
}

func (p *pgxInserter) Close() {
	for i := 0; i < len(p.inserters); i++ {
		close(p.inserters[i])
	}
}

func (p *pgxInserter) InsertNewData(newSeries []SeriesWithCallback, rows map[string][]*samplesInfo) (uint64, error) {
	err := p.InsertSeries(newSeries)
	if err != nil {
		return 0, err
	}

	return p.InsertData(rows)
}

func (p *pgxInserter) InsertSeries(seriesToInsert []SeriesWithCallback) error {
	if len(seriesToInsert) == 0 {
		return nil
	}

	var lastSeenLabel Labels

	batch := p.conn.NewBatch()
	numSQLFunctionCalls := 0
	// Sort and remove duplicates. The sort is needed to remove duplicates. Each series is inserted
	// in a different transaction, thus deadlocks are not an issue.
	sort.Slice(seriesToInsert, func(i, j int) bool {
		return seriesToInsert[i].Series.Compare(seriesToInsert[j].Series) < 0
	})

	batchSeries := make([][]SeriesWithCallback, 0, len(seriesToInsert))
	for _, curr := range seriesToInsert {
		if !lastSeenLabel.isEmpty() && lastSeenLabel.Equal(curr.Series) {
			batchSeries[len(batchSeries)-1] = append(batchSeries[len(batchSeries)-1], curr)
			continue
		}

		batch.Queue("BEGIN;")
		batch.Queue(getSeriesIDForLabelSQL, curr.Series.metric_name, curr.Series.names, curr.Series.values)
		batch.Queue("COMMIT;")
		numSQLFunctionCalls++
		batchSeries = append(batchSeries, []SeriesWithCallback{curr})

		lastSeenLabel = curr.Series
	}

	br, err := p.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return err
	}
	defer br.Close()

	if numSQLFunctionCalls != len(batchSeries) {
		return fmt.Errorf("unexpected difference in numQueries and batchSeries")
	}

	for i := 0; i < numSQLFunctionCalls; i++ {
		_, err = br.Exec()
		if err != nil {
			return err
		}
		row := br.QueryRow()

		var id SeriesID
		err = row.Scan(&id)
		if err != nil {
			return err
		}
		for _, swc := range batchSeries[i] {
			err := swc.Callback(swc.Series, id)
			if err != nil {
				return err
			}
		}
		_, err = br.Exec()
		if err != nil {
			return err
		}
	}

	return nil
}

type insertDataRequest struct {
	metricTable string
	data        []*samplesInfo
	finished    *sync.WaitGroup
	errChan     chan error
}

type insertDataTask struct {
	finished *sync.WaitGroup
	errChan  chan error
}

func (p *pgxInserter) InsertData(rows map[string][]*samplesInfo) (uint64, error) {
	// check that all the metrics are valid, we don't want to deadlock waiting
	// on an Insert to a metric that does not exist
	for metricName := range rows {
		_, err := p.getMetricTableName(metricName)
		if err != nil {
			return 0, err
		}
	}

	var numRows uint64
	workFinished := &sync.WaitGroup{}
	workFinished.Add(len(rows))
	errChan := make(chan error, 1)
	for metricName, data := range rows {
		for _, si := range data {
			numRows += uint64(len(si.samples))
		}
		tableName, err := p.getMetricTableName(metricName)
		if err != nil {
			panic("lost metric table name")
		}
		p.insertMetricData(tableName, data, workFinished, errChan)
	}

	workFinished.Wait()
	var err error
	select {
	case err = <-errChan:
	default:
	}

	return numRows, err
}

func (p *pgxInserter) insertMetricData(metricTable string, data []*samplesInfo, finished *sync.WaitGroup, errChan chan error) {
	inserter := p.getMetricTableInserter(metricTable)
	inserter <- insertDataRequest{metricTable: metricTable, data: data, finished: finished, errChan: errChan}
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

func (p *pgxInserter) getMetricTableInserter(metricTable string) chan insertDataRequest {
	h := maphash.Hash{}
	h.SetSeed(p.seed)
	h.WriteString(metricTable)
	if len(p.inserters) < 1 {
		panic(fmt.Sprintf("invalid len %d", len(p.inserters)))
	}
	inserter := h.Sum64() % uint64(len(p.inserters))
	return p.inserters[inserter]
}

type InsertHandler struct {
	conn    pgxConn
	input   chan insertDataRequest
	pending orderedMap
}

type orderedMap struct {
	elements map[string]*list.Element
	order    list.List // list of PendingBuffer

	oldBuffers []*PendingBuffer
}

type PendingBuffer struct {
	metricTable   string
	needsResponse []insertDataTask
	batch         SampleInfoIterator
	start         time.Time
}

const (
	flushSize    = 2000
	flushTimeout = 500 * time.Millisecond
)

func runInserterRoutine(conn pgxConn, input chan insertDataRequest) {
	handler := InsertHandler{
		conn:    conn,
		input:   input,
		pending: makeOrderedMap(),
	}

	for {
		if !handler.hasPendingReqs() {
			stillAlive := handler.blockingHandleReq()
			if !stillAlive {
				return
			}
			continue
		}

		startHandling := time.Now()
	hotReceive:
		for {
			for i := 0; i < 1000; i++ {
				receivingNewReqs := handler.nonblockingHandleReq()
				if !receivingNewReqs {
					break hotReceive
				}
			}
			if time.Now().Sub(startHandling) > flushTimeout {
				handler.flushTimedOutReqs()
				startHandling = time.Now()
			}
		}

		handler.flushEarliestReq()
	}
}

func (h *InsertHandler) hasPendingReqs() bool {
	return !h.pending.IsEmpty()
}

func (h *InsertHandler) blockingHandleReq() bool {
	req, ok := <-h.input
	if !ok {
		return false
	}

	h.handleReq(req)

	return true
}

func (h *InsertHandler) nonblockingHandleReq() bool {
	select {
	case req := <-h.input:
		h.handleReq(req)
		return true
	default:
		return false
	}
}

func (h *InsertHandler) handleReq(req insertDataRequest) bool {
	needsFlush, pending := h.pending.addReq(req)
	if needsFlush {
		h.flushPending(pending)
		return true
	}
	return false
}

func (h *InsertHandler) flushTimedOutReqs() {
	for {
		earliest, earliestPending := h.pending.Front()
		if earliest == nil {
			return
		}

		elapsed := time.Now().Sub(earliestPending.start)
		if elapsed < flushTimeout {
			return
		}

		h.flushPending(earliest)
	}
}

func (h *InsertHandler) flushEarliestReq() {
	earliest, _ := h.pending.Front()
	if earliest == nil {
		return
	}

	h.flushPending(earliest)
}

func (h *InsertHandler) flushPending(pendingElem *list.Element) {
	pending := h.pending.Remove(pendingElem)

	_, err := h.conn.CopyFrom(
		context.Background(),
		pgx.Identifier{promSchema, pending.metricTable},
		copyColumns,
		&pending.batch,
	)

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
		pending.batch.sampleInfos[i] = nil
	}
	pending.batch = SampleInfoIterator{sampleInfos: pending.batch.sampleInfos[:0], sampleIndex: -1, sampleInfoIndex: 0}

	h.pending.giveOldBuffer(pending)
}

func makeOrderedMap() orderedMap {
	return orderedMap{
		elements: make(map[string]*list.Element),
		order:    list.List{},
	}
}

func (m *orderedMap) IsEmpty() bool {
	return len(m.elements) == 0
}

func (m *orderedMap) addReq(req insertDataRequest) (bool, *list.Element) {
	pending, ok := m.elements[req.metricTable]

	var needsFlush bool
	var pendingBuffer *PendingBuffer
	if ok {
		pendingBuffer = pending.Value.(*PendingBuffer)
	} else {
		pendingBuffer = m.newPendingBuffer(req.metricTable)
		pending = m.order.PushBack(pendingBuffer)
		m.elements[req.metricTable] = pending
	}

	needsFlush = pendingBuffer.addReq(req)
	return needsFlush, pending
}

func (p *PendingBuffer) addReq(req insertDataRequest) bool {
	p.needsResponse = append(p.needsResponse, insertDataTask{finished: req.finished, errChan: req.errChan})
	p.batch.sampleInfos = append(p.batch.sampleInfos, req.data...)
	return len(p.batch.sampleInfos) > flushSize
}

func (m *orderedMap) newPendingBuffer(metricTable string) *PendingBuffer {
	var buffer *PendingBuffer
	if len(m.oldBuffers) > 0 {
		last := len(m.oldBuffers) - 1
		buffer, m.oldBuffers = m.oldBuffers[last], m.oldBuffers[:last]
	} else {
		buffer = &PendingBuffer{
			batch: SampleInfoIterator{sampleInfos: make([]*samplesInfo, 0), sampleIndex: -1, sampleInfoIndex: 0},
		}
	}

	buffer.start = time.Now()
	buffer.metricTable = metricTable
	return buffer
}

func (m *orderedMap) Front() (*list.Element, *PendingBuffer) {
	elem := m.order.Front()
	return elem, elem.Value.(*PendingBuffer)
}

func (m *orderedMap) Remove(elem *list.Element) *PendingBuffer {
	pending := elem.Value.(*PendingBuffer)
	m.order.Remove(elem)
	delete(m.elements, pending.metricTable)
	return pending
}

func (m *orderedMap) giveOldBuffer(buffer *PendingBuffer) {
	m.oldBuffers = append(m.oldBuffers, buffer)
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
func NewPgxReader(c *pgxpool.Pool) *DBReader {
	config := bigcache.DefaultConfig(10 * time.Minute)
	metrics, _ := bigcache.NewBigCache(config)
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
	rows, err := q.conn.Query(context.Background(), "SELECT 1")

	if err != nil {
		return err
	}

	rows.Close()
	return nil
}

func (q *pgxQuerier) Query(query *prompb.Query) ([]*prompb.TimeSeries, error) {
	if query == nil {
		return []*prompb.TimeSeries{}, nil
	}

	metric, cases, values, err := q.buildSubQueries(query)
	if err != nil {
		return nil, err
	}
	filter := metricTimeRangeFilter{
		metric:    metric,
		startTime: toRFC3339Nano(query.StartTimestampMs),
		endTime:   toRFC3339Nano(query.EndTimestampMs),
	}

	if metric != "" {
		tableName, err := q.getMetricTableName(metric)
		if err != nil {
			// If the metric table is missing, there are no results for this query.
			if err == errMissingTableName {
				return make([]*prompb.TimeSeries, 0), nil
			}

			return nil, err
		}
		filter.metric = tableName

		sqlQuery := q.buildTimeseriesByLabelClausesQuery(filter, cases)
		rows, err := q.conn.Query(context.Background(), sqlQuery, values...)

		if err != nil {
			// If we are getting undefined table error, it means the query
			// is looking for a metric which doesn't exist in the system.
			if e, ok := err.(*pgconn.PgError); !ok || e.Code != pgerrcode.UndefinedTable {
				return nil, err
			}
		}

		defer rows.Close()
		return q.buildTimeSeries(rows)
	}

	sqlQuery := q.buildMetricNameSeriesIDQuery(cases)
	rows, err := q.conn.Query(context.Background(), sqlQuery, values...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	metrics, series, err := q.getSeriesPerMetric(rows)

	if err != nil {
		return nil, err
	}

	results := make([]*prompb.TimeSeries, 0, len(metrics))

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
		sqlQuery = q.buildTimeseriesBySeriesIDQuery(filter, series[i])
		rows, err = q.conn.Query(context.Background(), sqlQuery)

		if err != nil {
			return nil, err
		}

		defer rows.Close()
		ts, err := q.buildTimeSeries(rows)

		if err != nil {
			return nil, err
		}

		results = append(results, ts...)
	}

	return results, nil
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

func (q *pgxQuerier) buildTimeSeries(rows pgx.Rows) ([]*prompb.TimeSeries, error) {
	results := make([]*prompb.TimeSeries, 0)

	for rows.Next() {
		var (
			timestamps []time.Time
			values     []float64
			labels     sampleLabels
		)
		err := rows.Scan(&labels, &timestamps, &values)

		if err != nil {
			return nil, err
		}

		if len(timestamps) != len(values) {
			return nil, fmt.Errorf("query returned a mismatch in timestamps and values")
		}

		result := &prompb.TimeSeries{
			Labels:  labels.ToPrompb(),
			Samples: make([]prompb.Sample, 0, len(timestamps)),
		}

		for i := range timestamps {
			result.Samples = append(result.Samples, prompb.Sample{
				Timestamp: toMilis(timestamps[i]),
				Value:     values[i],
			})
		}

		results = append(results, result)
	}

	return results, nil
}

// fromLabelMatchers parses protobuf label matchers to Prometheus label matchers.
// TODO: This is a copy of a function in github.com/prometheus/prometheus/storage/remote
// package b/c it was causing build issues. We should remove it and resolve the build issues.
func fromLabelMatchers(matchers []*prompb.LabelMatcher) ([]*labels.Matcher, error) {
	result := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		var mtype labels.MatchType
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			mtype = labels.MatchEqual
		case prompb.LabelMatcher_NEQ:
			mtype = labels.MatchNotEqual
		case prompb.LabelMatcher_RE:
			mtype = labels.MatchRegexp
		case prompb.LabelMatcher_NRE:
			mtype = labels.MatchNotRegexp
		default:
			return nil, errors.New("invalid matcher type")
		}
		matcher, err := labels.NewMatcher(mtype, matcher.Name, matcher.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, matcher)
	}
	return result, nil
}

type clauseBuilder struct {
	clauses []string
	args    []interface{}
}

func (c *clauseBuilder) addClause(clause string, args ...interface{}) error {
	argIndex := len(c.args) + 1
	argCountInClause := strings.Count(clause, "%d")

	if argCountInClause != len(args) {
		return fmt.Errorf("invalid number of args")
	}

	argIndexes := make([]interface{}, 0, argCountInClause)

	for argCountInClause > 0 {
		argIndexes = append(argIndexes, argIndex)
		argIndex++
		argCountInClause--
	}

	c.clauses = append(c.clauses, fmt.Sprintf(clause, argIndexes...))
	c.args = append(c.args, args...)

	return nil
}

func (c *clauseBuilder) build() ([]string, []interface{}) {
	return c.clauses, c.args
}

func (q *pgxQuerier) buildSubQueries(query *prompb.Query) (string, []string, []interface{}, error) {
	var err error
	metric := ""
	metricMatcherCount := 0
	cb := clauseBuilder{}
	matchers, err := fromLabelMatchers(query.Matchers)

	if err != nil {
		return "", nil, nil, err
	}

	for _, m := range matchers {
		// From the PromQL docs: "Label matchers that match
		// empty label values also select all time series that
		// do not have the specific label set at all."
		matchesEmpty := m.Matches("")

		switch m.Type {
		case labels.MatchRegexp:
			sq := subQueryRE
			if matchesEmpty {
				sq = subQueryREMatchEmpty
			}
			err = cb.addClause(sq, m.Name, anchorValue(m.Value))
		case labels.MatchNotEqual:
			sq := subQueryNEQ
			if matchesEmpty {
				sq = subQueryNEQMatchEmpty
			}
			err = cb.addClause(sq, m.Name, m.Value)
		case labels.MatchNotRegexp:
			sq := subQueryNRE
			if matchesEmpty {
				sq = subQueryNREMatchEmpty
			}
			err = cb.addClause(sq, m.Name, anchorValue(m.Value))
		case labels.MatchEqual:
			if m.Name == metricNameLabelName {
				metricMatcherCount++
				metric = m.Value
			}
			sq := subQueryEQ
			if matchesEmpty {
				sq = subQueryEQMatchEmpty
			}
			err = cb.addClause(sq, m.Name, m.Value)
		}

		if err != nil {
			return "", nil, nil, err
		}

		// Empty value (default case) is ignored.
	}

	// We can be certain that we want a single metric only if we find a single metric name matcher.
	// Note: possible future optimization for this case, since multiple metric names would exclude
	// each other and give empty result.
	if metricMatcherCount > 1 {
		metric = ""
	}
	clauses, values := cb.build()

	if len(clauses) == 0 {
		err = fmt.Errorf("no clauses generated")
	}

	return metric, clauses, values, err
}

func (q *pgxQuerier) buildMetricNameSeriesIDQuery(cases []string) string {
	return fmt.Sprintf(metricNameSeriesIDSQLFormat, strings.Join(cases, " AND "))
}

func (q *pgxQuerier) buildTimeseriesByLabelClausesQuery(filter metricTimeRangeFilter, cases []string) string {
	return fmt.Sprintf(
		timeseriesByMetricSQLFormat,
		pgx.Identifier{promSchema, filter.metric}.Sanitize(),
		strings.Join(cases, " AND "),
		filter.startTime,
		filter.endTime,
	)
}

func (q *pgxQuerier) buildTimeseriesBySeriesIDQuery(filter metricTimeRangeFilter, series []SeriesID) string {
	s := make([]string, 0, len(series))
	for _, sID := range series {
		s = append(s, fmt.Sprintf("%d", sID))
	}
	return fmt.Sprintf(
		timeseriesBySeriesIDsSQLFormat,
		pgx.Identifier{promSchema, filter.metric}.Sanitize(),
		strings.Join(s, ","),
		filter.startTime,
		filter.endTime,
	)
}

func (q *pgxQuerier) getSeriesPerMetric(rows pgx.Rows) ([]string, [][]SeriesID, error) {
	metrics := make([]string, 0)
	series := make([][]SeriesID, 0)

	for rows.Next() {
		var (
			metricName string
			seriesIDs  []int64
		)
		if err := rows.Scan(&metricName, &seriesIDs); err != nil {
			return nil, nil, err
		}

		sIDs := make([]SeriesID, 0, len(seriesIDs))

		for _, v := range seriesIDs {
			sIDs = append(sIDs, SeriesID(v))
		}

		metrics = append(metrics, metricName)
		series = append(series, sIDs)
	}

	return metrics, series, nil
}

// anchorValue adds anchors to values in regexps since PromQL docs
// states that "Regex-matches are fully anchored."
func anchorValue(str string) string {
	l := len(str)

	if l == 0 || (str[0] == '^' && str[l-1] == '$') {
		return str
	}

	if str[0] == '^' {
		return fmt.Sprintf("%s$", str)
	}

	if str[l-1] == '$' {
		return fmt.Sprintf("^%s", str)
	}

	return fmt.Sprintf("^%s$", str)
}

type sampleLabels struct {
	JSON        []byte
	Map         map[string]string
	OrderedKeys []string
}

func createOrderedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (l *sampleLabels) Scan(value interface{}) error {
	if value == nil {
		*l = sampleLabels{}
		return nil
	}

	var t []byte

	switch v := value.(type) {
	case string:
		t = []byte(v)
	case []byte:
		t = v
	default:
		return errInvalidLabelsValue(reflect.TypeOf(value).String())
	}

	m := make(map[string]string)
	err := json.Unmarshal(t, &m)

	if err != nil {
		return err
	}

	*l = sampleLabels{
		JSON:        t,
		Map:         m,
		OrderedKeys: createOrderedKeys(m),
	}
	return nil
}

func (l sampleLabels) ToPrompb() []prompb.Label {
	result := make([]prompb.Label, 0, l.len())

	for _, k := range l.OrderedKeys {
		result = append(result, prompb.Label{
			Name:  k,
			Value: l.Map[k],
		})
	}

	return result
}

func (l *sampleLabels) len() int {
	return len(l.OrderedKeys)
}

func toMilis(t time.Time) int64 {
	return t.UnixNano() / 1e6
}

func toRFC3339Nano(milliseconds int64) string {
	sec := milliseconds / 1000
	nsec := (milliseconds - (sec * 1000)) * 1000000
	return time.Unix(sec, nsec).UTC().Format(time.RFC3339Nano)
}
