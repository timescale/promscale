package pgmodel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/allegro/bigcache"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

const (
	getCreateMetricsTableSQL = "SELECT table_name FROM get_or_create_metric_table_name($1)"
	getSeriesIDForLabelSQL   = "SELECT get_series_id_for_key_value_array($1, $2, $3)"

	subQueryEQ            = "labels && (SELECT array_agg(l.id) FROM _prom_catalog.label l WHERE l.key = $%d and l.value = $%d)"
	subQueryEQMatchEmpty  = "NOT labels && (SELECT array_agg(l.id) FROM _prom_catalog.label l WHERE l.key = $%d and l.value != $%d)"
	subQueryNEQ           = "NOT labels && (SELECT array_agg(l.id) FROM _prom_catalog.label l WHERE l.key = $%d and l.value = $%d) "
	subQueryNEQMatchEmpty = "labels && (SELECT array_agg(l.id) FROM _prom_catalog.label l WHERE l.key = $%d and l.value != $%d)"
	subQueryRE            = "labels && (SELECT array_agg(l.id) FROM _prom_catalog.label l WHERE l.key = $%d and l.value ~ $%d) "
	subQueryREMatchEmpty  = "NOT labels && (SELECT array_agg(l.id) FROM _prom_catalog.label l WHERE l.key = $%d and l.value !~ $%d)"
	subQueryNRE           = "NOT labels && (SELECT array_agg(l.id) FROM _prom_catalog.label l WHERE l.key = $%d and l.value ~ $%d)"
	subQueryNREMatchEmpty = "labels && (SELECT array_agg(l.id) FROM _prom_catalog.label l WHERE l.key = $%d and l.value !~ $%d)"

	metricNameSeriesIDSQLFormat = `SELECT m.metric_name, array_agg(s.id)
	FROM _prom_catalog.series s
	INNER JOIN _prom_catalog.metric m
	ON (m.id = s.metric_id)
	WHERE %s
	GROUP BY m.metric_name`

	timeseriesByMetricSQLFormat = `SELECT label_array_to_jsonb(s.labels), array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)
	FROM %s m
	INNER JOIN _prom_catalog.series s
	ON m.series_id = s.id
	WHERE %s
	AND time >= '%s'
	AND time <= '%s'
	GROUP BY s.id`

	timeseriesBySeriesIDsSQLFormat = `SELECT label_array_to_jsonb(s.labels), array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)
	FROM %s m
	INNER JOIN _prom_catalog.series s
	ON m.series_id = s.id
	WHERE m.series_id IN (%s)
	AND time >= '%s'
	AND time <= '%s'
	GROUP BY s.id`

	dataTableSchema = "prom"
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

// NewPgxIngestor returns a new Ingestor that write to PostgreSQL using PGX
func NewPgxIngestor(c *pgxpool.Pool) *DBIngestor {
	pi := &pgxInserter{
		conn: &pgxConnImpl{
			conn: c,
		},
	}

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

type pgxInserter struct {
	conn pgxConn
	// TODO: update implementation to match existing caching layer?
	metricTableNames sync.Map
}

func (p *pgxInserter) InsertNewData(newSeries []SeriesWithCallback, rows map[string]*SampleInfoIterator) (uint64, error) {
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

func (p *pgxInserter) InsertData(rows map[string]*SampleInfoIterator) (uint64, error) {
	var result uint64
	var err error
	var tableName string
	for metricName, data := range rows {
		tableName, err = p.getMetricTableName(metricName)
		if err != nil {
			return result, err
		}
		inserted, err := p.conn.CopyFrom(
			context.Background(),
			pgx.Identifier{dataTableSchema, tableName},
			copyColumns,
			data,
		)
		if err != nil {
			return result, err
		}
		result = result + uint64(inserted)
	}

	return result, nil
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
	var tableNameI interface{}
	var err error
	var ok bool
	var tableName string
	if tableNameI, ok = p.metricTableNames.Load(metric); !ok {
		tableName, err = p.createMetricTable(metric)
		if err != nil {
			return "", err
		}
		p.metricTableNames.Store(metric, tableName)
	} else {
		tableName = tableNameI.(string)
	}

	return tableName, nil
}

type metricTimeRangeFilter struct {
	metric    string
	startTime string
	endTime   string
}

type pgxQuerier struct {
	conn pgxConn
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
		startTime: toRFC3339(query.StartTimestampMs),
		endTime:   toRFC3339(query.EndTimestampMs),
	}

	if metric != "" {
		sqlQuery := q.buildTimeseriesByLabelClausesQuery(filter, cases)
		rows, err := q.conn.Query(context.Background(), sqlQuery, values...)

		if err != nil {
			return nil, err
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
		filter.metric = metric
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
		pgx.Identifier{dataTableSchema, filter.metric}.Sanitize(),
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
		pgx.Identifier{dataTableSchema, filter.metric}.Sanitize(),
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
			seriesIDs  []SeriesID
		)
		if err := rows.Scan(&metricName, &seriesIDs); err != nil {
			return nil, nil, err
		}

		metrics = append(metrics, metricName)
		series = append(series, seriesIDs)
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
		l = &sampleLabels{}
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
	return t.UnixNano() / 1000
}

func toRFC3339(milliseconds int64) string {
	sec := milliseconds / 1000
	nsec := (milliseconds - (sec * 1000)) * 1000000
	return time.Unix(sec, nsec).UTC().Format(time.RFC3339)
}
