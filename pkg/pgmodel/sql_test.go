// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package pgmodel

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
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

// rowResults represents a collection of a multi-column row result
type rowResults [][]interface{}

func newSqlRecorder(queries []sqlQuery, t *testing.T) *sqlRecorder {
	return &sqlRecorder{queries: queries, t: t}
}

func (r *sqlRecorder) Close() {
}

func (r *sqlRecorder) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	results, err := r.checkQuery(sql, arguments...)

	if len(results) == 0 {
		return nil, err
	}
	if len(results) != 1 {
		r.t.Errorf("mock exec: too many return rows %v\n in Exec\n %v\n args %v",
			results, sql, arguments)
		return nil, err
	}
	if len(results[0]) != 1 {
		r.t.Errorf("mock exec: too many return values %v\n in Exec\n %v\n args %v",
			results, sql, arguments)
		return nil, err
	}

	return results[0][0].(pgconn.CommandTag), err
}

func (r *sqlRecorder) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	rows, err := r.checkQuery(sql, args...)
	return &mockRows{results: rows}, err
}

func (r *sqlRecorder) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	r.lock.Lock()
	defer r.lock.Unlock()
	rows, err := r.checkQuery(sql, args...)
	return &mockRows{results: rows, err: err}
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

	start := r.nextQuery
	for _, q := range batch.items {
		_, _ = r.checkQuery(q.query, q.arguments...)
	}
	// TODO switch to q.query[] subslice
	return &mockBatchResult{queries: r.queries[start:r.nextQuery]}, nil
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
		r.t.Errorf("@ %d unexpected query args for\n\t%s\ngot:\n\t%#v\nexpected:\n\t%#v", idx, sql, args, row.args)
	}
	return row.results, row.err
}

type batchItem struct {
	query     string
	arguments []interface{}
}

// Batch queries are a way of bundling multiple queries together to avoid
// unnecessary network round trips.
type mockBatch struct {
	items []batchItem
}

func (b *mockBatch) Queue(query string, arguments ...interface{}) {
	b.items = append(b.items, batchItem{
		query:     query,
		arguments: arguments,
	})
}

type mockBatchResult struct {
	idx     int
	queries []sqlQuery
	t       *testing.T
}

// Exec reads the results from the next query in the batch as if the query has been sent with Conn.Exec.
func (m *mockBatchResult) Exec() (pgconn.CommandTag, error) {
	defer func() { m.idx++ }()

	q := m.queries[m.idx]

	if len(q.results) == 0 {
		return nil, q.err
	}
	if len(q.results) != 1 {
		m.t.Errorf("mock exec: too many return rows %v\n in batch Exec\n %+v", q.results, q)
		return nil, q.err
	}
	if len(q.results[0]) != 1 {
		m.t.Errorf("mock exec: too many return values %v\n in batch Exec\n %+v", q.results, q)
		return nil, q.err
	}

	return q.results[0][0].(pgconn.CommandTag), q.err
}

// Query reads the results from the next query in the batch as if the query has been sent with Conn.Query.
func (m *mockBatchResult) Query() (pgx.Rows, error) {
	defer func() { m.idx++ }()

	q := m.queries[m.idx]
	return &mockRows{results: q.results, noNext: false}, q.err
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
	q := m.queries[m.idx]
	return &mockRows{results: q.results, err: q.err, noNext: false}
}

type mockRows struct {
	idx     int
	noNext  bool
	results rowResults
	err     error
}

// Close closes the rows, making the connection ready for use again. It is safe
// to call Close after rows is already closed.
func (m *mockRows) Close() {
}

// Err returns any error that occurred while reading.
func (m *mockRows) Err() error {
	return m.err
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

	if m.err != nil {
		return m.err
	}

	if m.idx >= len(m.results) {
		return fmt.Errorf("mock scanning error, no more results in batch: got %d wanted %d", m.idx, len(m.results))
	}

	if len(dest) > len(m.results[m.idx]) {
		return fmt.Errorf("mock scanning error, missing results for scanning: got %d %#v\nwanted %d",
			len(m.results[m.idx]),
			m.results[m.idx],
			len(dest),
		)
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
			_, ok1 := dest[i].(*int64)
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

// test code start

// sql_ingest tests

func TestPGXInserterInsertSeries(t *testing.T) {
	testCases := []struct {
		name       string
		series     []labels.Labels
		sqlQueries []sqlQuery
	}{
		{
			name: "Zero series",
		},
		{
			name: "One series",
			series: []labels.Labels{
				{
					{Name: "name_1", Value: "value_1"},
					{Name: "__name__", Value: "metric_1"},
				},
			},

			sqlQueries: []sqlQuery{
				{sql: "BEGIN;"},
				{
					sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					args:    []interface{}(nil),
					results: rowResults{{int64(1)}},
					err:     error(nil),
				},
				{sql: "COMMIT;"},
				{sql: "BEGIN;"},
				{
					sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					args: []interface{}{
						"metric_1",
						[]string{"__name__", "name_1"},
						[]string{"metric_1", "value_1"},
					},
					results: rowResults{{"table", int64(1)}},
					err:     error(nil),
				},
				{sql: "COMMIT;"},
			},
		},
		{
			name: "Two series",
			series: []labels.Labels{
				{
					{Name: "name_1", Value: "value_1"},
					{Name: "__name__", Value: "metric_1"},
				},
				{
					{Name: "name_2", Value: "value_2"},
					{Name: "__name__", Value: "metric_2"},
				},
			},
			sqlQueries: []sqlQuery{
				{sql: "BEGIN;"},
				{
					sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					args:    []interface{}(nil),
					results: rowResults{{int64(1)}},
					err:     error(nil),
				},
				{sql: "COMMIT;"},
				{sql: "BEGIN;"},
				{
					sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					args: []interface{}{
						"metric_1",
						[]string{"__name__", "name_1"},
						[]string{"metric_1", "value_1"},
					},
					results: rowResults{{"table", int64(1)}},
					err:     error(nil),
				},
				{sql: "COMMIT;"},
				{sql: "BEGIN;"},
				{
					sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					args: []interface{}{
						"metric_2",
						[]string{"__name__", "name_2"},
						[]string{"metric_2", "value_2"},
					},
					results: rowResults{{"table", int64(2)}},
					err:     error(nil),
				},
				{sql: "COMMIT;"},
			},
		},
		{
			name: "Double series",
			series: []labels.Labels{
				{
					{Name: "name_1", Value: "value_1"},
					{Name: "__name__", Value: "metric_1"}},
				{
					{Name: "name_2", Value: "value_2"},
					{Name: "__name__", Value: "metric_2"}},
				{
					{Name: "name_1", Value: "value_1"},
					{Name: "__name__", Value: "metric_1"},
				},
			},
			sqlQueries: []sqlQuery{
				{sql: "BEGIN;"},
				{
					sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					args:    []interface{}(nil),
					results: rowResults{{int64(1)}},
					err:     error(nil),
				},
				{sql: "COMMIT;"},
				{sql: "BEGIN;"},
				{
					sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					args: []interface{}{
						"metric_1",
						[]string{"__name__", "name_1"},
						[]string{"metric_1", "value_1"},
					},
					results: rowResults{{"table", int64(1)}},
					err:     error(nil),
				},
				{sql: "COMMIT;"},
				{sql: "BEGIN;"},
				{
					sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					args: []interface{}{
						"metric_2", []string{"__name__", "name_2"},
						[]string{"metric_2", "value_2"},
					},
					results: rowResults{{"table", int64(2)}},
					err:     error(nil),
				},
				{sql: "COMMIT;"},
			},
		},
		{
			name: "Query err",
			series: []labels.Labels{
				{
					{Name: "name_1", Value: "value_1"},
					{Name: "__name__", Value: "metric_1"}},
				{
					{Name: "name_2", Value: "value_2"},
					{Name: "__name__", Value: "metric_2"},
				},
			},
			sqlQueries: []sqlQuery{
				{sql: "BEGIN;"},
				{
					sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					args:    []interface{}(nil),
					results: rowResults{{int64(1)}},
					err:     error(nil),
				},
				{sql: "COMMIT;"},
				{sql: "BEGIN;"},
				{
					sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					args: []interface{}{
						"metric_1",
						[]string{"__name__", "name_1"},
						[]string{"metric_1", "value_1"},
					},
					results: rowResults{{"table", int64(1)}},
					err:     fmt.Errorf("some query error"),
				},
				{sql: "COMMIT;"},
				{sql: "BEGIN;"},
				{
					sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					args: []interface{}{
						"metric_2",
						[]string{"__name__", "name_2"},
						[]string{"metric_2", "value_2"},
					},
					results: rowResults{{"table", int64(2)}},
					err:     error(nil),
				},
				{sql: "COMMIT;"},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mock := newSqlRecorder(c.sqlQueries, t)

			inserter := insertHandler{
				conn:             mock,
				seriesCache:      make(map[string]SeriesID),
				seriesCacheEpoch: -1,
			}

			lsi := make([]samplesInfo, 0)
			for _, ser := range c.series {
				ls, err := LabelsFromSlice(ser)
				if err != nil {
					t.Errorf("invalid labels %+v, %v", ls, err)
				}
				lsi = append(lsi, samplesInfo{labels: ls, seriesID: -1})
			}

			_, _, err := inserter.setSeriesIds(lsi)
			if err != nil {
				foundErr := false
				for _, q := range c.sqlQueries {
					if q.err != nil {
						foundErr = true
						if err != q.err {
							t.Errorf("unexpected query error:\ngot\n%s\nwanted\n%s", err, q.err)
						}
					}
				}
				if !foundErr {
					t.Errorf("unexpected error: %v", err)
				}
			}

			if err == nil {
				for _, si := range lsi {
					if si.seriesID <= 0 {
						t.Error("Series not set", lsi)
					}
				}
			}
		})
	}
}

func TestPGXInserterCacheReset(t *testing.T) {

	series := []labels.Labels{
		{
			{Name: "__name__", Value: "metric_1"},
			{Name: "name_1", Value: "value_1"},
		},
		{
			{Name: "name_1", Value: "value_2"},
			{Name: "__name__", Value: "metric_1"},
		},
	}

	sqlQueries := []sqlQuery{

		// first series cache fetch
		{sql: "BEGIN;"},
		{
			sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			args:    []interface{}(nil),
			results: rowResults{{int64(1)}},
			err:     error(nil),
		},
		{sql: "COMMIT;"},
		{sql: "BEGIN;"},
		{
			sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
			args: []interface{}{
				"metric_1",
				[]string{"__name__", "name_1"},
				[]string{"metric_1", "value_1"},
			},
			results: rowResults{{"table", int64(1)}},
			err:     error(nil),
		},
		{sql: "COMMIT;"},
		{sql: "BEGIN;"},
		{
			sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
			args: []interface{}{
				"metric_1",
				[]string{"__name__", "name_1"},
				[]string{"metric_1", "value_2"},
			},
			results: rowResults{{"table", int64(2)}},
			err:     error(nil),
		},
		{sql: "COMMIT;"},

		// first labels cache refresh, does not trash
		{
			sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			args:    []interface{}(nil),
			results: rowResults{{int64(1)}},
			err:     error(nil),
		},

		// second labels cache refresh, trash the cache
		{
			sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			args:    []interface{}(nil),
			results: rowResults{{int64(2)}},
			err:     error(nil),
		},

		// repopulate the cache
		{sql: "BEGIN;"},
		{
			sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			args:    []interface{}(nil),
			results: rowResults{{int64(2)}},
			err:     error(nil),
		},
		{sql: "COMMIT;"},
		{sql: "BEGIN;"},
		{
			sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
			args: []interface{}{
				"metric_1",
				[]string{"__name__", "name_1"},
				[]string{"metric_1", "value_1"},
			},
			results: rowResults{{"table", int64(3)}},
			err:     error(nil),
		},
		{sql: "COMMIT;"},
		{sql: "BEGIN;"},
		{
			sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
			args: []interface{}{
				"metric_1",
				[]string{"__name__", "name_1"},
				[]string{"metric_1", "value_2"},
			},
			results: rowResults{{"table", int64(4)}},
			err:     error(nil),
		},
		{sql: "COMMIT;"},
	}

	mock := newSqlRecorder(sqlQueries, t)

	inserter := insertHandler{
		conn:             mock,
		seriesCache:      make(map[string]SeriesID),
		seriesCacheEpoch: -1,
	}

	makeSamples := func(series []labels.Labels) []samplesInfo {
		lsi := make([]samplesInfo, 0)
		for _, ser := range series {
			ls, err := LabelsFromSlice(ser)
			if err != nil {
				t.Errorf("invalid labels %+v, %v", ls, err)
			}
			lsi = append(lsi, samplesInfo{labels: ls, seriesID: -1})
		}
		return lsi
	}

	samples := makeSamples(series)
	_, _, err := inserter.setSeriesIds(samples)
	if err != nil {
		t.Fatal(err)
	}

	expectedIds := map[string]SeriesID{
		"value_1": SeriesID(1),
		"value_2": SeriesID(2),
	}

	for _, si := range samples {
		value := si.labels.values[1]
		expectedId := expectedIds[value]
		if si.seriesID != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", si.seriesID, expectedId)
		}
	}

	// refreshing during the same epoch givesthe same IDs without checking the DB
	inserter.refreshSeriesCache()

	samples = makeSamples(series)
	_, _, err = inserter.setSeriesIds(samples)
	if err != nil {
		t.Fatal(err)
	}

	for _, si := range samples {
		value := si.labels.values[1]
		expectedId := expectedIds[value]
		if si.seriesID != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", si.seriesID, expectedId)
		}
	}

	// trash the cache
	inserter.refreshSeriesCache()

	// retrying rechecks the DB and uses the new IDs
	samples = makeSamples(series)
	_, _, err = inserter.setSeriesIds(samples)
	if err != nil {
		t.Fatal(err)
	}

	expectedIds = map[string]SeriesID{
		"value_1": SeriesID(3),
		"value_2": SeriesID(4),
	}

	for _, si := range samples {
		value := si.labels.values[1]
		expectedId := expectedIds[value]
		if si.seriesID != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", si.seriesID, expectedId)
		}
	}
}

func TestPGXInserterInsertData(t *testing.T) {
	testCases := []struct {
		name          string
		rows          map[string][]samplesInfo
		sqlQueries    []sqlQuery
		metricsGetErr error
	}{
		{
			name: "Zero data",
			sqlQueries: []sqlQuery{
				{sql: "CALL _prom_catalog.finalize_metric_creation()"},
			},
		},
		{
			name: "One data",
			rows: map[string][]samplesInfo{
				"metric_0": {{samples: make([]prompb.Sample, 1)}},
			},
			sqlQueries: []sqlQuery{
				{sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					args:    []interface{}{"metric_0"},
					results: rowResults{{"metric_0", true}},
					err:     error(nil),
				},
				{
					sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					args:    []interface{}{int64(-1)},
					results: rowResults{{[]byte{}}},
					err:     error(nil),
				},
				{
					sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					args: []interface{}{
						[]time.Time{time.Unix(0, 0)},
						[]float64{0},
						[]int64{0},
					},
					results: rowResults{{pgconn.CommandTag{'1'}}},
					err:     error(nil),
				},
			},
		},
		{
			name: "Two data",
			rows: map[string][]samplesInfo{
				"metric_0": {
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
				},
			},
			sqlQueries: []sqlQuery{
				{sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					args:    []interface{}{"metric_0"},
					results: rowResults{{"metric_0", true}},
					err:     error(nil),
				},
				{
					sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					args:    []interface{}{int64(-1)},
					results: rowResults{{[]byte{}}},
					err:     error(nil),
				},
				{
					sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					args: []interface{}{
						[]time.Time{time.Unix(0, 0), time.Unix(0, 0)},
						[]float64{0, 0},
						[]int64{0, 0},
					},
					results: rowResults{{pgconn.CommandTag{'1'}}},
					err:     error(nil),
				},
			},
		},
		{
			name: "Create table error",
			rows: map[string][]samplesInfo{
				"metric_0": {
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
				},
			},
			sqlQueries: []sqlQuery{
				{sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					args:    []interface{}{"metric_0"},
					results: rowResults{},
					err:     fmt.Errorf("create table error"),
				},
			},
		},
		{
			name: "Epoch Error",
			rows: map[string][]samplesInfo{
				"metric_0": {{samples: make([]prompb.Sample, 1)}},
			},
			sqlQueries: []sqlQuery{
				{sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					args:    []interface{}{"metric_0"},
					results: rowResults{{"metric_0", true}},
					err:     error(nil),
				},
				{
					sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					args:    []interface{}{int64(-1)},
					results: rowResults{{[]byte{}}},
					err:     fmt.Errorf("epoch error"),
				},
				{
					sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					args: []interface{}{
						[]time.Time{time.Unix(0, 0)},
						[]float64{0},
						[]int64{0},
					},
					results: rowResults{},
					err:     error(nil),
				},
			},
		},
		{
			name: "Copy from error",
			rows: map[string][]samplesInfo{
				"metric_0": {
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
				},
			},

			sqlQueries: []sqlQuery{
				{sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					args:    []interface{}{"metric_0"},
					results: rowResults{{"metric_0", true}},
					err:     error(nil),
				},
				{
					sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					args:    []interface{}{int64(-1)},
					results: rowResults{{[]byte{}}},
					err:     error(nil),
				},
				{
					sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					args: []interface{}{
						[]time.Time{time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0)},
						make([]float64, 5),
						make([]int64, 5),
					},
					results: rowResults{{pgconn.CommandTag{'1'}}},
					err:     fmt.Errorf("some INSERT error"),
				},
			},
		},
		{
			name: "Can't find/create table in DB",
			rows: map[string][]samplesInfo{
				"metric_0": {
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
					{samples: make([]prompb.Sample, 1)},
				},
			},
			sqlQueries: []sqlQuery{
				{sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					sql:  "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					args: []interface{}{"metric_0"},
					// no results is deliberate
					results: rowResults{},
					err:     error(nil),
				},
			},
		},
		{
			name: "Metrics get error",
			rows: map[string][]samplesInfo{
				"metric_0": {{samples: make([]prompb.Sample, 1)}},
			},
			metricsGetErr: fmt.Errorf("some metrics error"),
			sqlQueries: []sqlQuery{
				{sql: "CALL _prom_catalog.finalize_metric_creation()"},
			},
		},
	}

	for _, co := range testCases {
		c := co
		t.Run(c.name, func(t *testing.T) {
			mock := newSqlRecorder(c.sqlQueries, t)

			metricCache := map[string]string{"metric_1": "metricTableName_1"}
			mockMetrics := &mockMetricCache{
				metricCache:  metricCache,
				getMetricErr: c.metricsGetErr,
			}
			inserter, err := newPgxInserter(mock, mockMetrics, &Cfg{})
			if err != nil {
				t.Fatal(err)
			}

			_, err = inserter.InsertData(c.rows)

			var expErr error

			switch {
			case c.metricsGetErr != nil:
				expErr = c.metricsGetErr
			case c.name == "Can't find/create table in DB":
				expErr = errMissingTableName
			default:
				for _, q := range c.sqlQueries {
					if q.err != nil {
						expErr = q.err
						break
					}
				}
			}

			if err != nil {
				if !errors.Is(err, expErr) {
					t.Errorf("unexpected error:\ngot\n%s\nwanted\n%s", err, expErr)
				}

				return
			}

			if expErr != nil {
				t.Errorf("expected error:\ngot\nnil\nwanted\n%s", expErr)
			}

			if len(c.rows) == 0 {
				return
			}
		})
	}
}

// sql_reader tests

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
			err: fmt.Errorf("mock scanning error, missing results for scanning: got 1 []interface {}{0}\nwanted 2"),
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
			mock := newSqlRecorder(c.sqlQueries, t)
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
			} else if !reflect.DeepEqual(result, c.result) {
				t.Errorf("unexpected result:\ngot\n%#v\nwanted\n%+v", result, c.result)
			}
		})
	}
}

func TestPgxQuerierLabelsNames(t *testing.T) {
	testCases := []struct {
		name        string
		expectedRes []string
		sqlQueries  []sqlQuery
	}{
		{
			name: "Error on query",
			sqlQueries: []sqlQuery{
				{
					sql:     "SELECT distinct key from _prom_catalog.label",
					args:    []interface{}(nil),
					results: rowResults{},
					err:     fmt.Errorf("some error"),
				},
			},
		}, {
			name: "Error on scanning values",
			sqlQueries: []sqlQuery{
				{
					sql:     "SELECT distinct key from _prom_catalog.label",
					args:    []interface{}(nil),
					results: rowResults{{1}},
				},
			},
		}, {
			name: "Empty result, is ok",
			sqlQueries: []sqlQuery{
				{
					sql:     "SELECT distinct key from _prom_catalog.label",
					args:    []interface{}(nil),
					results: rowResults{},
				},
			},
			expectedRes: []string{},
		}, {
			name: "Result should be sorted",
			sqlQueries: []sqlQuery{
				{
					sql:     "SELECT distinct key from _prom_catalog.label",
					args:    []interface{}(nil),
					results: rowResults{{"b"}, {"a"}},
				},
			},
			expectedRes: []string{"a", "b"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := newSqlRecorder(tc.sqlQueries, t)
			querier := pgxQuerier{conn: mock}
			res, err := querier.LabelNames()

			var expectedErr error
			for _, q := range tc.sqlQueries {
				if q.err != nil {
					expectedErr = err
					break
				}
			}

			if tc.name == "Error on scanning values" {
				if err.Error() != "wrong value type int" {
					expectedErr = fmt.Errorf("wrong value type int")
					t.Errorf("unexpected error\n got: %v\n expected: %v", err, expectedErr)
					return
				}
			} else if expectedErr != err {
				t.Errorf("unexpected error\n got: %v\n expected: %v", err, expectedErr)
				return
			}

			outputIsSorted := sort.SliceIsSorted(res, func(i, j int) bool {
				return res[i] < res[j]
			})
			if !outputIsSorted {
				t.Errorf("returned label names %v are not sorted", res)
			}

			if !reflect.DeepEqual(tc.expectedRes, res) {
				t.Errorf("expected: %v, got: %v", tc.expectedRes, res)
			}
		})
	}
}

func TestPgxQuerierLabelsValues(t *testing.T) {
	testCases := []struct {
		name        string
		expectedRes []string
		sqlQueries  []sqlQuery
	}{
		{
			name: "Error on query",
			sqlQueries: []sqlQuery{
				{
					sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					args:    []interface{}{"m"},
					results: rowResults{},
					err:     fmt.Errorf("some error"),
				},
			},
		}, {
			name: "Error on scanning values",
			sqlQueries: []sqlQuery{
				{
					sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					args:    []interface{}{"m"},
					results: rowResults{{1}},
				},
			},
		}, {
			name: "Empty result, is ok",
			sqlQueries: []sqlQuery{
				{
					sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					args:    []interface{}{"m"},
					results: rowResults{},
				},
			},
			expectedRes: []string{},
		}, {
			name: "Result should be sorted",
			sqlQueries: []sqlQuery{
				{
					sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					args:    []interface{}{"m"},
					results: rowResults{{"b"}, {"a"}},
				},
			},
			expectedRes: []string{"a", "b"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := newSqlRecorder(tc.sqlQueries, t)
			querier := pgxQuerier{conn: mock}
			res, err := querier.LabelValues("m")

			var expectedErr error
			for _, q := range tc.sqlQueries {
				if q.err != nil {
					expectedErr = err
					break
				}
			}

			if tc.name == "Error on scanning values" {
				if err.Error() != "wrong value type int" {
					expectedErr = fmt.Errorf("wrong value type int")
					t.Errorf("unexpected error\n got: %v\n expected: %v", err, expectedErr)
					return
				}
			} else if expectedErr != err {
				t.Errorf("unexpected error\n got: %v\n expected: %v", err, expectedErr)
				return
			}

			outputIsSorted := sort.SliceIsSorted(res, func(i, j int) bool {
				return res[i] < res[j]
			})
			if !outputIsSorted {
				t.Errorf("returned label names %v are not sorted", res)
			}

			if !reflect.DeepEqual(tc.expectedRes, res) {
				t.Errorf("expected: %v, got: %v", tc.expectedRes, res)
			}
		})
	}
}
