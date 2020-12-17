// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package pgmodel

import (
	"context"
	"errors"
	"fmt"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/ingester"
	"github.com/timescale/promscale/pkg/pgmodel/utils"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

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

			inserter := ingester.insertHandler{
				conn:             mock,
				seriesCache:      make(map[string]utils.SeriesID),
				seriesCacheEpoch: -1,
			}

			lsi := make([]utils.samplesInfo, 0)
			for _, ser := range c.series {
				ls, err := utils.LabelsFromSlice(ser)
				if err != nil {
					t.Errorf("invalid labels %+v, %v", ls, err)
				}
				lsi = append(lsi, utils.samplesInfo{labels: ls, seriesID: -1})
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

	inserter := ingester.insertHandler{
		conn:             mock,
		seriesCache:      make(map[string]utils.SeriesID),
		seriesCacheEpoch: -1,
	}

	makeSamples := func(series []labels.Labels) []utils.samplesInfo {
		lsi := make([]utils.samplesInfo, 0)
		for _, ser := range series {
			ls, err := utils.LabelsFromSlice(ser)
			if err != nil {
				t.Errorf("invalid labels %+v, %v", ls, err)
			}
			lsi = append(lsi, utils.samplesInfo{labels: ls, seriesID: -1})
		}
		return lsi
	}

	samples := makeSamples(series)
	_, _, err := inserter.setSeriesIds(samples)
	if err != nil {
		t.Fatal(err)
	}

	expectedIds := map[string]utils.SeriesID{
		"value_1": utils.SeriesID(1),
		"value_2": utils.SeriesID(2),
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

	expectedIds = map[string]utils.SeriesID{
		"value_1": utils.SeriesID(3),
		"value_2": utils.SeriesID(4),
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
		rows          map[string][]utils.samplesInfo
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
			rows: map[string][]utils.samplesInfo{
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
			rows: map[string][]utils.samplesInfo{
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
			rows: map[string][]utils.samplesInfo{
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
			rows: map[string][]utils.samplesInfo{
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
					//this is the attempt on the full batch
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
				{
					//this is the attempt on the individual copyRequests
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
			rows: map[string][]utils.samplesInfo{
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
					// this is the entire batch insert
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
				{
					// this is the retry on individual copy requests
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
			rows: map[string][]utils.samplesInfo{
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
			rows: map[string][]utils.samplesInfo{
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
			inserter, err := ingester.newPgxInserter(mock, mockMetrics, &ingester.Cfg{})
			if err != nil {
				t.Fatal(err)
			}
			defer inserter.Close()

			_, err = inserter.InsertData(c.rows)

			var expErr error

			switch {
			case c.metricsGetErr != nil:
				expErr = c.metricsGetErr
			case c.name == "Can't find/create table in DB":
				expErr = utils.errMissingTableName
			default:
				for _, q := range c.sqlQueries {
					if q.err != nil {
						expErr = q.err
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
					{Type: prompb.LabelMatcher_NEQ, Name: utils.MetricNameLabelName, Value: "bar"},
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
					{Type: prompb.LabelMatcher_NEQ, Name: utils.MetricNameLabelName, Value: "bar"},
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
					{Type: prompb.LabelMatcher_NEQ, Name: utils.MetricNameLabelName, Value: "bar"},
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
					{Type: prompb.LabelMatcher_EQ, Name: utils.MetricNameLabelName, Value: "bar"},
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
					{Type: prompb.LabelMatcher_NEQ, Name: utils.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "foo"}},
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
					{Type: prompb.LabelMatcher_EQ, Name: utils.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "bar"}},
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
					{Type: prompb.LabelMatcher_RE, Name: utils.MetricNameLabelName, Value: ""},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "foo"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "bar"}},
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
					{Type: prompb.LabelMatcher_EQ, Name: utils.MetricNameLabelName, Value: "foo"},
					{Type: prompb.LabelMatcher_EQ, Name: utils.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "foo"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "bar"}},
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
			querier := pgxQuerier{conn: mock, metricTableNames: mockMetrics, labelsReader: utils.NewLabelsReader(mock, clockcache.WithMax(0))}

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
