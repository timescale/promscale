// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package pgmodel

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/prompb"
)

// rowResults represents a collection of a multi-column row result
type rowResults [][]interface{}

type mockPGXConn struct {
	insertLock        sync.Mutex
	queryLock         sync.Mutex
	DBName            string
	ExecSQLs          []string
	ExecArgs          [][]interface{}
	ExecErr           error
	QuerySQLs         []string
	QueryArgs         [][]interface{}
	QueryResults      []rowResults
	QueryResultsIndex int
	QueryNoRows       bool
	QueryErr          map[int]error // Mapping query call to error response.
	CopyFromTableName []string
	Times             []time.Time
	Vals              []float64
	Series            []int64
	CopyFromResult    int64
	CopyFromError     error
	CopyFromRowsRows  [][]interface{}
	Batch             []*mockBatch
}

func (m *mockPGXConn) Close() {
}

func (m *mockPGXConn) UseDatabase(dbName string) {
	m.DBName = dbName
}

func (m *mockPGXConn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	if strings.HasPrefix(sql, "INSERT INTO ") && strings.HasSuffix(sql, "DO NOTHING") {
		m.insertLock.Lock()
		defer m.insertLock.Unlock()
		if len(arguments) != 3 {
			panic(fmt.Sprintf("invalid arguments: %v", arguments))
		}
		end := 0
		for end < len(sql) && sql[end] != '(' {
			end += 1
		}
		tableName := sql[len("INSERT INTO "):end]
		m.CopyFromTableName = append(m.CopyFromTableName, tableName)

		times := arguments[0].([]time.Time)
		vals := arguments[1].([]float64)
		series := arguments[2].([]int64)

		m.Times = append(m.Times, times...)
		m.Vals = append(m.Vals, vals...)
		m.Series = append(m.Series, series...)

		return pgconn.CommandTag([]byte{}), m.CopyFromError
	} else {
		m.ExecSQLs = append(m.ExecSQLs, sql)
		m.ExecArgs = append(m.ExecArgs, arguments)
		return pgconn.CommandTag([]byte{}), m.ExecErr
	}
}

func (m *mockPGXConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	m.queryLock.Lock()
	defer m.queryLock.Unlock()
	defer func() {
		m.QueryResultsIndex++
	}()
	m.QuerySQLs = append(m.QuerySQLs, sql)
	m.QueryArgs = append(m.QueryArgs, args)
	if len(m.QueryResults) <= m.QueryResultsIndex {
		return &mockRows{results: nil, noNext: m.QueryNoRows}, m.QueryErr[m.QueryResultsIndex]

	}
	return &mockRows{results: m.QueryResults[m.QueryResultsIndex], noNext: m.QueryNoRows}, m.QueryErr[m.QueryResultsIndex]
}

func (m *mockPGXConn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	panic("should never be called")
}

func (m *mockPGXConn) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource {
	m.CopyFromRowsRows = rows
	return pgx.CopyFromRows(rows)
}

func (m *mockPGXConn) NewBatch() pgxBatch {
	return &mockBatch{}
}

func (m *mockPGXConn) SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error) {
	m.queryLock.Lock()
	defer m.queryLock.Unlock()
	defer func() { m.QueryResultsIndex++ }()
	batch := b.(*mockBatch)
	m.Batch = append(m.Batch, batch)
	return &mockBatchResult{results: m.QueryResults}, m.QueryErr[m.QueryResultsIndex]
}

type mockMetricCache struct {
	metricCache  map[string]string
	getMetricErr error
	setMetricErr error
}

func (m *mockMetricCache) Get(metric string) (string, error) {
	if m.getMetricErr != nil {
		return "", m.getMetricErr
	}

	val, ok := m.metricCache[metric]
	if !ok {
		return "", ErrEntryNotFound
	}

	return val, nil
}

func (m *mockMetricCache) Set(metric string, tableName string) error {
	m.metricCache[metric] = tableName
	return m.setMetricErr
}

type batchItem struct {
	query     string
	arguments []interface{}
}

// Batch queries are a way of bundling multiple queries together to avoid
// unnecessary network round trips.
type mockBatch struct {
	items []*batchItem
}

func (b *mockBatch) Queue(query string, arguments ...interface{}) {
	b.items = append(b.items, &batchItem{
		query:     query,
		arguments: arguments,
	})
}

type mockBatchResult struct {
	idx     int
	results []rowResults
}

// Exec reads the results from the next query in the batch as if the query has been sent with Conn.Exec.
func (m *mockBatchResult) Exec() (pgconn.CommandTag, error) {
	return nil, nil
}

// Query reads the results from the next query in the batch as if the query has been sent with Conn.Query.
func (m *mockBatchResult) Query() (pgx.Rows, error) {
	defer func() { m.idx++ }()
	if len(m.results) <= m.idx {
		return &mockRows{results: nil, noNext: false}, nil
	}
	return &mockRows{results: m.results[m.idx], noNext: false}, nil
}

// Close closes the batch operation. This must be called before the underlying connection can be used again. Any error
// that occurred during a batch operation may have made it impossible to resyncronize the connection with the server.
// In this case the underlying connection will have been closed.
func (m *mockBatchResult) Close() error {
	return nil
}

// QueryRow reads the results from the next query in the batch as if the query has been sent with Conn.QueryRow.
func (m *mockBatchResult) QueryRow() pgx.Row {
	defer func() { m.idx++ }()
	if len(m.results) <= m.idx {
		return &mockRows{results: nil, noNext: false}

	}
	return &mockRows{results: m.results[m.idx], noNext: false}
}

type mockRows struct {
	idx     int
	noNext  bool
	results rowResults
}

// Close closes the rows, making the connection ready for use again. It is safe
// to call Close after rows is already closed.
func (m *mockRows) Close() {
}

// Err returns any error that occurred while reading.
func (m *mockRows) Err() error {
	return nil
}

// CommandTag returns the command tag from this query. It is only available after Rows is closed.
func (m *mockRows) CommandTag() pgconn.CommandTag {
	panic("not implemented")
}

func (m *mockRows) FieldDescriptions() []pgproto3.FieldDescription {
	panic("not implemented")
}

// Next prepares the next row for reading. It returns true if there is another
// row and false if no more rows are available. It automatically closes rows
// when all rows are read.
func (m *mockRows) Next() bool {
	return !m.noNext && m.idx < len(m.results)
}

// Scan reads the values from the current row into dest values positionally.
// dest can include pointers to core types, values implementing the Scanner
// interface, []byte, and nil. []byte will skip the decoding process and directly
// copy the raw bytes received from PostgreSQL. nil will skip the value entirely.
func (m *mockRows) Scan(dest ...interface{}) error {
	defer func() { m.idx++ }()

	if m.idx >= len(m.results) {
		return fmt.Errorf("scanning error, no more results: got %d wanted %d", m.idx, len(m.results))
	}

	if len(dest) > len(m.results[m.idx]) {
		return fmt.Errorf("scanning error, missing results for scanning: got %d wanted %d", len(m.results[m.idx]), len(dest))
	}

	for i := range dest {
		switch s := m.results[m.idx][i].(type) {
		case []time.Time:
			if d, ok := dest[i].(*[]time.Time); ok {
				*d = s
			} else if d, ok := dest[i].(*pgtype.TimestamptzArray); ok {
				*d = pgtype.TimestamptzArray{
					Elements: make([]pgtype.Timestamptz, len(s)),
				}
				for i := range s {
					d.Elements[i] = pgtype.Timestamptz{
						Time: s[i],
					}
				}
			}
		case []float64:
			if d, ok := dest[i].(*[]float64); ok {
				*d = s
			} else if d, ok := dest[i].(*pgtype.Float8Array); ok {
				*d = pgtype.Float8Array{
					Elements: make([]pgtype.Float8, len(s)),
				}
				for i := range s {
					d.Elements[i] = pgtype.Float8{
						Float: s[i],
					}
				}
			}
		case []int64:
			if d, ok := dest[i].(*[]int64); ok {
				*d = s
				continue
			}
			if d, ok := dest[i].(*[]SeriesID); ok {
				for _, id := range s {
					*d = append(*d, SeriesID(id))
				}
				continue
			}
			return fmt.Errorf("wrong value type []int64")
		case []string:
			if d, ok := dest[i].(*[]string); ok {
				*d = s
			}
		case time.Time:
			if d, ok := dest[i].(*time.Time); ok {
				*d = s
			}
		case float64:
			if _, ok := dest[i].(float64); !ok {
				return fmt.Errorf("wrong value type float64")
			}
			dv := reflect.ValueOf(dest[i])
			dvp := reflect.Indirect(dv)
			dvp.SetFloat(float64(m.results[m.idx][i].(float64)))
		case int:
			if _, ok := dest[i].(int); !ok {
				return fmt.Errorf("wrong value type int")
			}
			dv := reflect.ValueOf(dest[i])
			dvp := reflect.Indirect(dv)
			dvp.SetInt(int64(m.results[m.idx][i].(int32)))
		case int32:
			if _, ok := dest[i].(int32); !ok {
				return fmt.Errorf("wrong value type int32")
			}
			dv := reflect.ValueOf(dest[i])
			dvp := reflect.Indirect(dv)
			dvp.SetInt(int64(m.results[m.idx][i].(int32)))
		case uint64:
			if _, ok := dest[i].(uint64); !ok {
				return fmt.Errorf("wrong value type uint64")
			}
			dv := reflect.ValueOf(dest[i])
			dvp := reflect.Indirect(dv)
			dvp.SetUint(m.results[m.idx][i].(uint64))
		case int64:
			_, ok1 := dest[i].(int64)
			_, ok2 := dest[i].(*SeriesID)
			if !ok1 && !ok2 {
				return fmt.Errorf("wrong value type int64")
			}
			dv := reflect.ValueOf(dest[i])
			dvp := reflect.Indirect(dv)
			dvp.SetInt(m.results[m.idx][i].(int64))
		case string:
			if _, ok := dest[i].(*string); !ok {
				return fmt.Errorf("wrong value type string")
			}
			dv := reflect.ValueOf(dest[i])
			dvp := reflect.Indirect(dv)
			dvp.SetString(m.results[m.idx][i].(string))
		}
	}

	return nil
}

// Values returns the decoded row values.
func (m *mockRows) Values() ([]interface{}, error) {
	panic("not implemented")
}

// RawValues returns the unparsed bytes of the row values. The returned [][]byte is only valid until the next Next
// call or the Rows is closed. However, the underlying byte data is safe to retain a reference to and mutate.
func (m *mockRows) RawValues() [][]byte {
	panic("not implemented")
}

func createSeriesResults(x int64) []rowResults {
	ret := make([]rowResults, 0, x)
	var i int64 = 1
	x++

	for i < x {
		ret = append(ret, rowResults{{"table", i}})
		i++
	}

	return ret
}

func createSeries(x int) []*labels.Labels {
	ret := make([]*labels.Labels, 0, x)
	i := 1
	x++

	for i < x {
		label := labels.Labels{
			labels.Label{
				Name:  fmt.Sprintf("name_%d", i),
				Value: fmt.Sprintf("value_%d", i),
			},
			labels.Label{
				Name:  fmt.Sprint(MetricNameLabelName),
				Value: fmt.Sprintf("metric_%d", i),
			},
		}
		ret = append(ret, &label)
		i++
	}

	return ret
}

func TestPGXInserterInsertSeries(t *testing.T) {
	testCases := []struct {
		name         string
		series       []*labels.Labels
		queryResults []rowResults
		queryErr     map[int]error
	}{
		{
			name: "Zero series",
		},
		{
			name:         "One series",
			series:       createSeries(1),
			queryResults: createSeriesResults(1),
		},
		{
			name:         "Two series",
			series:       createSeries(2),
			queryResults: createSeriesResults(2),
		},
		{
			name:         "Double series",
			series:       append(createSeries(2), createSeries(1)...),
			queryResults: createSeriesResults(2),
		},
		{
			name:         "Query err",
			series:       createSeries(2),
			queryResults: createSeriesResults(2),
			queryErr:     map[int]error{0: fmt.Errorf("some query error")},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mock := &mockPGXConn{
				QueryErr:     c.queryErr,
				QueryResults: c.queryResults,
			}

			inserter := insertHandler{conn: mock, seriesCache: make(map[string]SeriesID)}

			lsi := make([]samplesInfo, 0)
			for _, ser := range c.series {
				ls, err := LabelsFromSlice(*ser)
				if err != nil {
					t.Errorf("invalid labels %+v, %v", ls, err)
				}
				lsi = append(lsi, samplesInfo{labels: ls, seriesID: -1})
			}

			_, err := inserter.setSeriesIds(lsi)
			if err != nil {
				switch {
				case len(c.queryErr) > 0:
					for _, qErr := range c.queryErr {
						if err != qErr {
							t.Errorf("unexpected query error:\ngot\n%s\nwanted\n%s", err, qErr)
						}
					}
					return
				default:
					t.Errorf("unexpected error: %v", err)
				}
			}

			for _, si := range lsi {
				if si.seriesID <= 0 {
					t.Error("Series not set")
				}
			}

			if c.queryErr != nil {
				t.Errorf("expected query error:\ngot\n%v\nwanted\n%v", err, c.queryErr)
			}
		})
	}
}

func createRows(x int) map[string][]samplesInfo {
	return createRowsByMetric(x, 1)
}

func createRowsByMetric(x int, metricCount int) map[string][]samplesInfo {
	ret := make(map[string][]samplesInfo)
	i := 0

	metrics := make([]string, 0, metricCount)

	for metricCount > i {
		metrics = append(metrics, fmt.Sprintf("metric_%d", i))
		i++
	}

	i = 0

	for i < x {
		metricIndex := i % metricCount

		ret[metrics[metricIndex]] = append(ret[metrics[metricIndex]], samplesInfo{samples: []prompb.Sample{{}}})
		i++
	}
	return ret
}

func TestPGXInserterInsertData(t *testing.T) {
	testCases := []struct {
		name           string
		rows           map[string][]samplesInfo
		queryNoRows    bool
		queryErr       map[int]error
		copyFromResult int64
		copyFromErr    error
		metricsGetErr  error
		metricsSetErr  error
	}{
		{
			name: "Zero data",
		},
		{
			name: "One data",
			rows: createRows(1),
		},
		{
			name: "Two data",
			rows: createRows(2),
		},
		{
			name:     "Create table error",
			rows:     createRows(5),
			queryErr: map[int]error{0: fmt.Errorf("create table error")},
		},
		{
			name:        "Copy from error",
			rows:        createRows(5),
			copyFromErr: fmt.Errorf("some error"),
		},
		{
			name:           "Not all data inserted",
			rows:           createRows(5),
			copyFromResult: 4,
		},
		{
			name:        "Can't find/create table in DB",
			rows:        createRows(5),
			queryNoRows: true,
		},
		{
			name:          "Metrics get error",
			rows:          createRows(1),
			metricsGetErr: fmt.Errorf("some metrics error"),
		},
		{
			name:          "Metrics set error",
			rows:          createRows(1),
			metricsSetErr: fmt.Errorf("some metrics error"),
		},
	}
	for _, co := range testCases {
		c := co
		t.Run(c.name, func(t *testing.T) {
			mock := &mockPGXConn{
				QueryNoRows:    c.queryNoRows,
				QueryErr:       c.queryErr,
				CopyFromResult: c.copyFromResult,
				CopyFromError:  c.copyFromErr,
			}

			//The database will be queried for metricNames
			results := make([]rowResults, 0, len(c.rows))
			for metricName := range c.rows {
				//create metric name
				results = append(results, rowResults{{metricName, true}})
				//send batch
				results = append(results, rowResults{{}})
			}
			mock.QueryResults = results

			metricCache := map[string]string{"metric_1": "metricTableName_1"}
			mockMetrics := &mockMetricCache{
				metricCache:  metricCache,
				getMetricErr: c.metricsGetErr,
				setMetricErr: c.metricsSetErr,
			}
			inserter, err := newPgxInserter(mock, mockMetrics, &Cfg{})
			if err != nil {
				t.Fatal(err)
			}

			_, err = inserter.InsertData(c.rows)

			if err != nil {
				var expErr error

				switch {
				case c.metricsGetErr != nil:
					expErr = c.metricsGetErr
				case c.metricsSetErr != nil:
					expErr = c.metricsSetErr
				case c.copyFromErr != nil:
					expErr = c.copyFromErr
				case c.queryErr != nil:
					for _, qErr := range c.queryErr {
						expErr = qErr
					}
				case c.queryNoRows:
					expErr = errMissingTableName
				}

				if err != expErr {
					t.Errorf("unexpected error:\ngot\n%s\nwanted\n%s", err, expErr)
				}

				return
			}

			if c.copyFromErr != nil {
				t.Errorf("expected error:\ngot\nnil\nwanted\n%s", c.copyFromErr)
			}

			if len(c.rows) == 0 {
				return
			}

			if len(mock.CopyFromTableName) != len(c.rows) {
				t.Errorf("number of table names differs from input: got %d want %d\n", len(mock.CopyFromTableName), len(c.rows))
			}

			tNames := make([]pgx.Identifier, 0, len(c.rows))
			for tableName := range c.rows {
				realTableName, err := mockMetrics.Get(tableName)
				if err != nil {
					t.Fatalf("error when fetching metric table name: %s", err)
				}
				tNames = append(tNames, pgx.Identifier{dataSchema, realTableName})
			}

			// Sorting because range over a map gives random iteration order.
			sort.Slice(tNames, func(i, j int) bool { return tNames[i][1] < tNames[j][1] })
			sort.Slice(mock.CopyFromTableName, func(i, j int) bool { return mock.CopyFromTableName[i][1] < mock.CopyFromTableName[j][1] })
			expectedNames := make([]string, len(tNames))
			for i := range tNames {
				expectedNames[i] = tNames[i].Sanitize()
			}
			if !reflect.DeepEqual(mock.CopyFromTableName, expectedNames) {
				t.Errorf("unexpected copy table:\ngot\n%s\nwanted\n%s", mock.CopyFromTableName, tNames)
			}
		})
	}
}

type sqlRecorder struct {
	queries   []sqlQuery
	nextQuery int
	lock      sync.Mutex
	t         *testing.T
}

type sqlQuery struct {
	sql     string
	args    []interface{}
	results rowResults
	err     error
}

func (r *sqlRecorder) Close() {
}

func (r *sqlRecorder) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	_, err := r.checkQuery(sql, arguments...)
	return pgconn.CommandTag{}, err
}

func (r *sqlRecorder) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	rows, err := r.checkQuery(sql, args...)
	return &mockRows{results: rows}, err
}

func (m *sqlRecorder) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	panic("should never be called")
}

func (m *sqlRecorder) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource {
	panic("should never be called")
}

func (m *sqlRecorder) NewBatch() pgxBatch {
	return &mockBatch{}
}

func (r *sqlRecorder) SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	batch := b.(*mockBatch)

	results := make([]rowResults, 0, len(batch.items))
	for _, q := range batch.items {
		//TODO err
		rows, _ := r.checkQuery(q.query, q.arguments...)
		results = append(results, rows)
	}
	return &mockBatchResult{results: results}, nil
}

func (r *sqlRecorder) checkQuery(sql string, args ...interface{}) (rowResults, error) {
	idx := r.nextQuery
	if idx >= len(r.queries) {
		r.t.Errorf("@ %d extra query: %s", idx, sql)
		return nil, fmt.Errorf("extra query")
	}
	row := r.queries[idx]
	r.nextQuery += 1
	if sql != row.sql {
		r.t.Errorf("@ %d unexpected query:\ngot:\n\t%s\nexpected:\n\t%s", idx, sql, row.sql)
	}
	if !reflect.DeepEqual(args, row.args) {
		r.t.Errorf("@ %d unexpected query args for\n\t%s\ngot:\n\t%v\nexpected:\n\t%v", idx, sql, args, row.args)
	}
	return row.results, row.err
}

func TestPGXQuerierQuery(t *testing.T) {
	testCases := []struct {
		name       string
		query      *prompb.Query
		result     []*prompb.TimeSeries
		err        error
		sqlQueries []sqlQuery // XXX whitespace in these is significant
	}{
		{
			name: "Error metric name value",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: MetricNameLabelName, Value: "bar"},
				},
			},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"__name__", "bar"},
					results: rowResults{{1, []int64{}}},
					err:     error(nil),
				},
			},
			err: fmt.Errorf("wrong value type int"),
		},
		{
			name: "Error first query",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: MetricNameLabelName, Value: "bar"},
				},
			},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"__name__", "bar"},
					results: rowResults{{"{}", []time.Time{}, []float64{}}},
					err:     fmt.Errorf("some error"),
				},
			},
		},
		{
			name: "Error second query",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: "foo", Value: "bar"},
				},
			},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"foo", "bar"},
					results: rowResults{{`foo`, []int64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"foo"},
					results: rowResults{{"foo"}},
					err:     fmt.Errorf("some error 2"),
				},
			},
		},
		{
			name: "Error third query",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: "foo", Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"foo", "bar"},
					results: rowResults{{"foo", []int64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"foo"},
					results: rowResults{{"foo"}},
					err:     error(nil),
				},
				{
					sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"foo\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"foo\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					args:    []interface{}(nil),
					results: rowResults(nil),
					err:     fmt.Errorf("some error 3")}},
		},
		{
			name: "Error scan values",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: MetricNameLabelName, Value: "bar"},
				},
			},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"__name__", "bar"},
					results: rowResults{{0}},
					err:     error(nil),
				},
			},
			err: fmt.Errorf("scanning error, missing results for scanning: got 1 wanted 2"),
		},
		{
			name:   "Empty query",
			result: []*prompb.TimeSeries{},
		},
		{
			name: "Simple query, no result",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"foo", "bar"},
					results: rowResults(nil),
					err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, metric doesn't exist",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{},
			sqlQueries: []sqlQuery{
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"bar"},
					results: rowResults(nil),
					err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, exclude matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: MetricNameLabelName, Value: "foo"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"__name__", "bar"},
					results: rowResults{{"foo", []int64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"foo"},
					results: rowResults{{"foo"}},
					err:     error(nil),
				},
				{
					sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"foo\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"foo\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					args:    []interface{}(nil),
					results: rowResults{{[]int64{1}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT (labels_info($1::int[])).*",
					args:    []interface{}{[]int64{1}},
					results: rowResults{{[]int64{1}, []string{"__name__"}, []string{"foo"}}},
					err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, metric name matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: MetricNameLabelName, Value: "bar"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []sqlQuery{
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"bar"},
					results: rowResults{{"bar"}},
					err:     error(nil),
				},
				{
					sql: "SELECT s.labels, array_agg(m.time ORDER BY time) as time_array, array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"bar\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"bar\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					args:    []interface{}{"__name__", "bar"},
					results: rowResults{{[]int64{2}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT (labels_info($1::int[])).*",
					args:    []interface{}{[]int64{2}},
					results: rowResults{{[]int64{2}, []string{"__name__"}, []string{"bar"}}},
					err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, empty metric name matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_RE, Name: MetricNameLabelName, Value: ""},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: MetricNameLabelName, Value: "foo"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
				{
					Labels:  []prompb.Label{{Name: MetricNameLabelName, Value: "bar"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value !~ $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"__name__", "^$"},
					results: rowResults{{"foo", []int64{1}}, {"bar", []int64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"foo"},
					results: rowResults{{"foo"}},
					err:     error(nil),
				},
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"bar"},
					results: rowResults{{"bar"}},
					err:     error(nil),
				},
				{
					sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"foo\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"foo\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					args:    []interface{}(nil),
					results: rowResults{{[]int64{3}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					err:     error(nil),
				},
				{
					sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"bar\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"bar\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					args:    []interface{}(nil),
					results: rowResults{{[]int64{4}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT (labels_info($1::int[])).*",
					args:    []interface{}{[]int64{3}},
					results: rowResults{{[]int64{3}, []string{"__name__"}, []string{"foo"}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT (labels_info($1::int[])).*",
					args:    []interface{}{[]int64{4}},
					results: rowResults{{[]int64{4}, []string{"__name__"}, []string{"bar"}}},
					err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, double metric name matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: MetricNameLabelName, Value: "foo"},
					{Type: prompb.LabelMatcher_EQ, Name: MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: MetricNameLabelName, Value: "foo"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
				{
					Labels:  []prompb.Label{{Name: MetricNameLabelName, Value: "bar"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2) AND labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $3 and l.value = $4)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"__name__", "foo", "__name__", "bar"},
					results: rowResults{{"foo", []int64{1}}, {"bar", []int64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"foo"},
					results: rowResults{{"foo"}},
					err:     error(nil),
				},
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"bar"},
					results: rowResults{{"bar"}},
					err:     error(nil),
				},
				{
					sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"foo\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"foo\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					args:    []interface{}(nil),
					results: rowResults{{[]int64{5}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					err:     error(nil),
				},
				{
					sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"bar\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"bar\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					args:    []interface{}(nil),
					results: rowResults{{[]int64{6}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT (labels_info($1::int[])).*",
					args:    []interface{}{[]int64{5}},
					results: rowResults{{[]int64{5}, []string{"__name__"}, []string{"foo"}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT (labels_info($1::int[])).*",
					args:    []interface{}{[]int64{6}},
					results: rowResults{{[]int64{6}, []string{"__name__"}, []string{"bar"}}},
					err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, no metric name matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: "foo", Value: "bar"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"foo", "bar"},
					results: rowResults{{"metric", []int64{1, 99, 98}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"metric"},
					results: rowResults{{"metric"}},
					err:     error(nil),
				},
				{
					sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"metric\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"metric\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1,99,98)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					args:    []interface{}(nil),
					results: rowResults{{[]int64{7}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT (labels_info($1::int[])).*",
					args:    []interface{}{[]int64{7}},
					results: rowResults{{[]int64{7}, []string{"foo"}, []string{"bar"}}},
					err:     error(nil),
				},
			},
		},
		{
			name: "Complex query, multiple matchers",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
					{Type: prompb.LabelMatcher_NEQ, Name: "foo1", Value: "bar1"},
					{Type: prompb.LabelMatcher_RE, Name: "foo2", Value: "^bar2"},
					{Type: prompb.LabelMatcher_NRE, Name: "foo3", Value: "bar3$"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "foo", Value: "bar"},
						{Name: "foo2", Value: "bar2"},
					},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $3 and l.value = $4) AND labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $5 and l.value ~ $6) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $7 and l.value ~ $8)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"foo", "bar", "foo1", "bar1", "foo2", "^bar2$", "foo3", "^bar3$"},
					results: rowResults{{"metric", []int64{1, 4, 5}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"metric"},
					results: rowResults{{"metric"}},
					err:     error(nil),
				},
				{
					sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"metric\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"metric\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1,4,5)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					args:    []interface{}(nil),
					results: rowResults{{[]int64{8, 9}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT (labels_info($1::int[])).*",
					args:    []interface{}{[]int64{9, 8}},
					results: rowResults{{[]int64{8, 9}, []string{"foo", "foo2"}, []string{"bar", "bar2"}}},
					err:     error(nil),
				},
			},
		},
		{
			name: "Complex query, empty equal matchers",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "foo", Value: ""},
					{Type: prompb.LabelMatcher_NEQ, Name: "foo1", Value: "bar1"},
					{Type: prompb.LabelMatcher_RE, Name: "foo2", Value: "^bar2$"},
					{Type: prompb.LabelMatcher_NRE, Name: "foo3", Value: "bar3"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "foo2", Value: "bar2"},
					},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []sqlQuery{
				{
					sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value != $2) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $3 and l.value = $4) AND labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $5 and l.value ~ $6) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $7 and l.value ~ $8)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					args:    []interface{}{"foo", "", "foo1", "bar1", "foo2", "^bar2$", "foo3", "^bar3$"},
					results: rowResults{{"metric", []int64{1, 2}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					args:    []interface{}{"metric"},
					results: rowResults{{"metric"}},
					err:     error(nil),
				},
				{
					sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"metric\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"metric\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1,2)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					args:    []interface{}(nil),
					results: rowResults{{[]int64{10}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					err:     error(nil),
				},
				{
					sql:     "SELECT (labels_info($1::int[])).*",
					args:    []interface{}{[]int64{10}},
					results: rowResults{{[]int64{10}, []string{"foo2"}, []string{"bar2"}}},
					err:     error(nil),
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mock := &sqlRecorder{queries: c.sqlQueries, t: t}
			metricCache := map[string]string{"metric_1": "metricTableName_1"}
			mockMetrics := &mockMetricCache{
				metricCache: metricCache,
			}
			querier := pgxQuerier{conn: mock, metricTableNames: mockMetrics, labels: clockcache.WithMax(0)}

			result, err := querier.Query(c.query)

			if err != nil {
				switch {
				case c.err == nil:
					found := false
					for _, q := range c.sqlQueries {
						if err == q.err {
							found = true
							break
						}
						if q.err != nil {
							t.Errorf("unexpected error:\ngot\n\t%v\nwanted\n\t%v", err, q.err)
						}
					}
					if !found {
						t.Errorf("unexpected error for query: %v", err)
					}
				case c.err != nil:
					if err.Error() != c.err.Error() {
						t.Errorf("unexpected error:\ngot\n\t%v\nwanted\n\t%v", err, c.err)
					}
				}
			}

			if !reflect.DeepEqual(result, c.result) {
				t.Errorf("unexpected result:\ngot\n%#v\nwanted\n%+v", result, c.result)
			}
		})
	}
}

func TestPgxQuerierLabelsNames(t *testing.T) {
	testLabelMethods(t, func(querier *pgxQuerier) ([]string, error) {
		return querier.LabelNames()
	})
}

func TestPgxQuerierLabelsValues(t *testing.T) {
	testLabelMethods(t, func(querier *pgxQuerier) ([]string, error) {
		return querier.LabelValues("m")
	})
}

func testLabelMethods(t *testing.T, f func(*pgxQuerier) ([]string, error)) {
	testCases := []struct {
		name         string
		expectedRes  []string
		errorOnQuery bool
		errorOnScan  bool
	}{
		{
			name:         "Error on query",
			errorOnQuery: true,
		}, {
			name:        "Error on scanning values",
			errorOnScan: true,
			expectedRes: []string{"a"},
		}, {
			name:        "Empty result, is ok",
			expectedRes: []string{},
		}, {
			name:        "Result should be sorted",
			expectedRes: []string{"b", "a"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			queryResults := toRowResults(tc.expectedRes, tc.errorOnScan)
			queryErr := make(map[int]error)
			if tc.errorOnQuery {
				queryErr[0] = fmt.Errorf("some error")
			}
			mock := &mockPGXConn{
				QueryErr:     queryErr,
				QueryResults: queryResults,
			}
			querier := pgxQuerier{conn: mock}
			res, err := f(&querier)
			if tc.errorOnQuery && err == nil {
				t.Error("unexpected lack of error")
				return
			} else if tc.errorOnScan && err == nil {
				t.Error("unexpected lack of error")
				return
			} else if err != nil && !tc.errorOnQuery && !tc.errorOnScan {
				t.Errorf("unexpected error: %v", err)
				return
			} else if tc.errorOnQuery || tc.errorOnScan {
				return
			}
			outputIsSorted := sort.SliceIsSorted(res, func(i, j int) bool {
				return res[i] < res[j]
			})
			if !outputIsSorted {
				t.Error("returned label names are not sorted")
			}
			sort.Strings(tc.expectedRes)
			if !reflect.DeepEqual(tc.expectedRes, res) {
				t.Errorf("expected: %v, got: %v", tc.expectedRes, res)
			}
		})
	}
}
func toRowResults(labelNames []string, convertImproperly bool) []rowResults {
	toReturn := make([]rowResults, 1)
	toReturn[0] = make(rowResults, len(labelNames))
	for i, labelName := range labelNames {
		if convertImproperly {
			toReturn[0][i] = []interface{}{1}
		} else {
			toReturn[0][i] = []interface{}{labelName}
		}
	}
	return toReturn
}
