// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package pgmodel

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/prompb"
)

const (
	getMetricsTableSQL = "SELECT table_name FROM " + catalogSchema + ".get_metric_table_name_if_exists($1)"
	getLabelNamesSQL   = "SELECT distinct key from " + catalogSchema + ".label"
	getLabelValuesSQL  = "SELECT value from " + catalogSchema + ".label WHERE key = $1"
	getLabelsSQL       = "SELECT (labels_info($1::int[])).*"
)

// NewPgxReaderWithMetricCache returns a new DBReader that reads from PostgreSQL using PGX
// and caches metric table names using the supplied cacher.
func NewPgxReaderWithMetricCache(c *pgxpool.Pool, cache MetricCache, labelsCacheSize uint64) *DBReader {
	pi := &pgxQuerier{
		conn: &pgxConnImpl{
			conn: c,
		},
		metricTableNames: cache,
		labels:           clockcache.WithMax(labelsCacheSize),
	}

	return &DBReader{
		db: pi,
	}
}

// NewPgxReader returns a new DBReader that reads that from PostgreSQL using PGX.
func NewPgxReader(c *pgxpool.Pool, readHist prometheus.ObserverVec, labelsCacheSize uint64) *DBReader {
	cache := &MetricNameCache{clockcache.WithMax(DefaultMetricCacheSize)}
	return NewPgxReaderWithMetricCache(c, cache, labelsCacheSize)
}

type metricTimeRangeFilter struct {
	metric    string
	startTime string
	endTime   string
}

type pgxQuerier struct {
	conn             pgxConn
	metricTableNames MetricCache
	// contains [int64]labels.Label
	labels *clockcache.Cache
}

var _ Querier = (*pgxQuerier)(nil)

// HealthCheck implements the HealthChecker interface.
func (q *pgxQuerier) HealthCheck() error {
	rows, err := q.conn.Query(context.Background(), "SELECT")

	if err != nil {
		return err
	}

	rows.Close()
	return nil
}

// NumCachedLabels implements the Querier interface.
func (q *pgxQuerier) NumCachedLabels() int {
	return q.labels.Len()
}

// LabelsCacheCapacity implements the Querier interface.
func (q *pgxQuerier) LabelsCacheCapacity() int {
	return q.labels.Cap()
}

// Select implements the Querier interface. It is the entry point for our
// own version of the Prometheus engine.
func (q *pgxQuerier) Select(mint int64, maxt int64, sortSeries bool, hints *storage.SelectHints, path []parser.Node, ms ...*labels.Matcher) (storage.SeriesSet, parser.Node) {
	rows, topNode, err := q.getResultRows(mint, maxt, hints, path, ms)
	if err != nil {
		return errorSeriesSet{err: err}, nil
	}

	ss := buildSeriesSet(rows, q)
	return ss, topNode
}

// Query implements the Querier interface. It is the entry point for
// remote-storage queries.
func (q *pgxQuerier) Query(query *prompb.Query) ([]*prompb.TimeSeries, error) {
	if query == nil {
		return []*prompb.TimeSeries{}, nil
	}

	matchers, err := fromLabelMatchers(query.Matchers)

	if err != nil {
		return nil, err
	}

	rows, _, err := q.getResultRows(query.StartTimestampMs, query.EndTimestampMs, nil, nil, matchers)

	if err != nil {
		return nil, err
	}

	results, err := buildTimeSeries(rows, q)

	return results, err
}

// fromLabelMatchers parses protobuf label matchers to Prometheus label matchers.
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

type timescaleRow struct {
	labelIds []int64
	times    pgtype.TimestamptzArray
	values   pgtype.Float8Array
	err      error
}

// getResultRows fetches the result row datasets from the database using the
// supplied query parameters.
func (q *pgxQuerier) getResultRows(startTimestamp int64, endTimestamp int64, hints *storage.SelectHints, path []parser.Node, matchers []*labels.Matcher) ([]timescaleRow, parser.Node, error) {
	// Build a subquery per metric matcher.
	metric, cases, values, err := buildSubQueries(matchers)
	if err != nil {
		return nil, nil, err
	}

	filter := metricTimeRangeFilter{
		metric:    metric,
		startTime: toRFC3339Nano(startTimestamp),
		endTime:   toRFC3339Nano(endTimestamp),
	}

	// If all metric matchers match on a single metric (common case),
	// we query only that single metric.
	if metric != "" {
		return q.querySingleMetric(metric, filter, cases, values, hints, path)
	}

	return q.queryMultipleMetrics(filter, cases, values)
}

// querySingleMetric returns all the result rows for a single metric using the
// supplied query parameters. It uses the hints and node path to try to push
// down query functions where possible.
func (q *pgxQuerier) querySingleMetric(metric string, filter metricTimeRangeFilter, cases []string, values []interface{}, hints *storage.SelectHints, path []parser.Node) ([]timescaleRow, parser.Node, error) {
	tableName, err := q.getMetricTableName(metric)
	if err != nil {
		// If the metric table is missing, there are no results for this query.
		if err == errMissingTableName {
			return nil, nil, nil
		}

		return nil, nil, err
	}
	filter.metric = tableName

	sqlQuery, values, topNode, err := buildTimeseriesByLabelClausesQuery(filter, cases, values, hints, path)
	if err != nil {
		return nil, nil, err
	}

	rows, err := q.conn.Query(context.Background(), sqlQuery, values...)
	if err != nil {
		// If we are getting undefined table error, it means the query
		// is looking for a metric which doesn't exist in the system.
		if e, ok := err.(*pgconn.PgError); !ok || e.Code != pgerrcode.UndefinedTable {
			return nil, nil, err
		}
	}

	defer rows.Close()

	// TODO this allocation assumes we usually have 1 row, if not, refactor
	tsRows, err := appendTsRows(make([]timescaleRow, 0, 1), rows)
	return tsRows, topNode, err
}

// queryMultipleMetrics returns all the result rows for across multiple metrics
// using the supplied query parameters.
func (q *pgxQuerier) queryMultipleMetrics(filter metricTimeRangeFilter, cases []string, values []interface{}) ([]timescaleRow, parser.Node, error) {
	// First fetch series IDs per metric.
	sqlQuery := buildMetricNameSeriesIDQuery(cases)
	rows, err := q.conn.Query(context.Background(), sqlQuery, values...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	metrics, series, err := getSeriesPerMetric(rows)
	if err != nil {
		return nil, nil, err
	}

	// TODO this assume on average on row per-metric. Is this right?
	results := make([]timescaleRow, 0, len(metrics))

	numQueries := 0
	batch := q.conn.NewBatch()

	// Generate queries for each metric and send them in a single batch.
	for i, metric := range metrics {
		//TODO batch getMetricTableName
		tableName, err := q.getMetricTableName(metric)
		if err != nil {
			// If the metric table is missing, there are no results for this query.
			if err == errMissingTableName {
				continue
			}
			return nil, nil, err
		}
		filter.metric = tableName
		sqlQuery = buildTimeseriesBySeriesIDQuery(filter, series[i])
		batch.Queue(sqlQuery)
		numQueries += 1
	}

	batchResults, err := q.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return nil, nil, err
	}
	defer batchResults.Close()

	for i := 0; i < numQueries; i++ {
		rows, err = batchResults.Query()
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		// Append all rows into results.
		results, err = appendTsRows(results, rows)
		// Can't defer because we need to Close before the next loop iteration.
		rows.Close()
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
	}

	return results, nil, nil
}

// appendTsRows adds new results rows to already existing result rows and
// returns the as a result.
func appendTsRows(out []timescaleRow, in pgx.Rows) ([]timescaleRow, error) {
	if in.Err() != nil {
		return out, in.Err()
	}
	for in.Next() {
		var row timescaleRow
		row.err = in.Scan(&row.labelIds, &row.times, &row.values)
		out = append(out, row)
		if row.err != nil {
			log.Error("err", row.err)
			return out, row.err
		}
	}
	return out, in.Err()
}

// getMetricTableName gets the table name for a specific metric from internal
// cache. If not found, fetches it from the database and updates the cache.
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

// errorSeriesSet represents an error result in a form of a series set.
// This behavior is inherited from Prometheus codebase.
type errorSeriesSet struct {
	err error
}

func (errorSeriesSet) Next() bool                   { return false }
func (errorSeriesSet) At() storage.Series           { return nil }
func (e errorSeriesSet) Err() error                 { return e.err }
func (e errorSeriesSet) Warnings() storage.Warnings { return nil }

// LabelNames implements the Querier interface. It returns all distinct
// label names available in the database.
func (q *pgxQuerier) LabelNames() ([]string, error) {
	rows, err := q.conn.Query(context.Background(), getLabelNamesSQL)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	labelNames := make([]string, 0)

	for rows.Next() {
		var labelName string
		if err := rows.Scan(&labelName); err != nil {
			return nil, err
		}

		labelNames = append(labelNames, labelName)
	}

	sort.Strings(labelNames)
	return labelNames, nil
}

// LabelValues implements the Querier interface. It returns all distinct values
// for a specified label name.
func (q *pgxQuerier) LabelValues(labelName string) ([]string, error) {
	rows, err := q.conn.Query(context.Background(), getLabelValuesSQL, labelName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	labelValues := make([]string, 0)

	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}

		labelValues = append(labelValues, value)
	}

	sort.Strings(labelValues)
	return labelValues, nil
}

type labelQuerier interface {
	getLabelsForIds(ids []int64) (lls labels.Labels, err error)
}

func (q *pgxQuerier) getPrompbLabelsForIds(ids []int64) (lls []prompb.Label, err error) {
	ll, err := q.getLabelsForIds(ids)
	if err != nil {
		return
	}
	lls = make([]prompb.Label, len(ll))
	for i := range ll {
		lls[i] = prompb.Label{Name: ll[i].Name, Value: ll[i].Value}
	}
	return
}

// getLabelsForIds returns label names and values for the supplied IDs.
func (q *pgxQuerier) getLabelsForIds(ids []int64) (lls labels.Labels, err error) {
	keys := make([]interface{}, len(ids))
	values := make([]interface{}, len(ids))
	for i := range ids {
		keys[i] = ids[i]
	}
	numHits := q.labels.GetValues(keys, values)

	if numHits < len(ids) {
		var numFetches int
		numFetches, err = q.fetchMissingLabels(keys[numHits:], ids[numHits:], values[numHits:])
		if err != nil {
			return
		}
		values = values[:numHits+numFetches]
	}

	lls = make([]labels.Label, 0, len(values))
	for i := range values {
		lls = append(lls, values[i].(labels.Label))
	}

	return
}

// fetchMissingLabels imports the missing label IDs from the database into the
// internal cache. It also modifies the newLabels slice to include the missing
// values.
func (q *pgxQuerier) fetchMissingLabels(misses []interface{}, missedIds []int64, newLabels []interface{}) (numNewLabels int, err error) {
	for i := range misses {
		missedIds[i] = misses[i].(int64)
	}
	rows, err := q.conn.Query(context.Background(), getLabelsSQL, missedIds)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var ids []int64
		var keys []string
		var vals []string
		err = rows.Scan(&ids, &keys, &vals)
		if err != nil {
			return 0, err
		}
		if len(ids) != len(keys) {
			return 0, fmt.Errorf("query returned a mismatch in ids and keys: %d, %d", len(ids), len(keys))
		}
		if len(keys) != len(vals) {
			return 0, fmt.Errorf("query returned a mismatch in timestamps and values: %d, %d", len(keys), len(vals))
		}
		if len(keys) > len(misses) {
			return 0, fmt.Errorf("query returned wrong number of labels: %d, %d", len(misses), len(keys))
		}

		numNewLabels = len(keys)
		misses = misses[:len(keys)]
		newLabels = newLabels[:len(keys)]
		for i := range newLabels {
			misses[i] = ids[i]
			newLabels[i] = labels.Label{Name: keys[i], Value: vals[i]}
		}

		numInserted := q.labels.InsertBatch(misses, newLabels)
		if numInserted < len(misses) {
			log.Warn("msg", "labels cache starving, may need to increase size")
		}
	}
	return numNewLabels, nil
}
