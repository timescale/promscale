// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	pgmodelErrs "github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/scache"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestPGXInserterInsertSeries(t *testing.T) {
	testCases := []struct {
		name       string
		series     []labels.Labels
		sqlQueries []model.SqlQuery
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

			sqlQueries: []model.SqlQuery{
				{Sql: "BEGIN;"},
				{
					Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}(nil),
					Results: model.RowResults{{int64(1)}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					Args: []interface{}{
						"metric_1",
						[]string{"__name__", "name_1"},
						[]string{"metric_1", "value_1"},
					},
					Results: model.RowResults{{"table", int64(1)}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
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
			sqlQueries: []model.SqlQuery{
				{Sql: "BEGIN;"},
				{
					Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}(nil),
					Results: model.RowResults{{int64(1)}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					Args: []interface{}{
						"metric_1",
						[]string{"__name__", "name_1"},
						[]string{"metric_1", "value_1"},
					},
					Results: model.RowResults{{"table", int64(1)}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					Args: []interface{}{
						"metric_2",
						[]string{"__name__", "name_2"},
						[]string{"metric_2", "value_2"},
					},
					Results: model.RowResults{{"table", int64(2)}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
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
			sqlQueries: []model.SqlQuery{
				{Sql: "BEGIN;"},
				{
					Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}(nil),
					Results: model.RowResults{{int64(1)}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					Args: []interface{}{
						"metric_1",
						[]string{"__name__", "name_1"},
						[]string{"metric_1", "value_1"},
					},
					Results: model.RowResults{{"table", int64(1)}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					Args: []interface{}{
						"metric_2", []string{"__name__", "name_2"},
						[]string{"metric_2", "value_2"},
					},
					Results: model.RowResults{{"table", int64(2)}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
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
			sqlQueries: []model.SqlQuery{
				{Sql: "BEGIN;"},
				{
					Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}(nil),
					Results: model.RowResults{{int64(1)}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					Args: []interface{}{
						"metric_1",
						[]string{"__name__", "name_1"},
						[]string{"metric_1", "value_1"},
					},
					Results: model.RowResults{{"table", int64(1)}},
					Err:     fmt.Errorf("some query error"),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
					Args: []interface{}{
						"metric_2",
						[]string{"__name__", "name_2"},
						[]string{"metric_2", "value_2"},
					},
					Results: model.RowResults{{"table", int64(2)}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mock := model.NewSqlRecorder(c.sqlQueries, t)
			scache.ResetStoredLabels()

			inserter := insertHandler{
				conn: mock,
			}

			lsi := make([]model.SamplesInfo, 0)
			for _, ser := range c.series {
				ls, err := scache.GetSeriesFromLabels(ser)
				if err != nil {
					t.Errorf("invalid labels %+v, %v", ls, err)
				}
				lsi = append(lsi, model.SamplesInfo{Labels: ls})
			}

			err := inserter.setSeriesIds(lsi)
			if err != nil {
				foundErr := false
				for _, q := range c.sqlQueries {
					if q.Err != nil {
						foundErr = true
						if err != q.Err {
							t.Errorf("unexpected query error:\ngot\n%s\nwanted\n%s", err, q.Err)
						}
					}
				}
				if !foundErr {
					t.Errorf("unexpected error: %v", err)
				}
			}

			if err == nil {
				for _, si := range lsi {
					si, se, err := si.Labels.GetSeriesID()
					require.NoError(t, err)
					require.True(t, si > 0, "series id not set")
					require.True(t, se > 0, "epoch not set")
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

	sqlQueries := []model.SqlQuery{

		// first series cache fetch
		{Sql: "BEGIN;"},
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: model.RowResults{{int64(1)}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},
		{Sql: "BEGIN;"},
		{
			Sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
			Args: []interface{}{
				"metric_1",
				[]string{"__name__", "name_1"},
				[]string{"metric_1", "value_1"},
			},
			Results: model.RowResults{{"table", int64(1)}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},
		{Sql: "BEGIN;"},
		{
			Sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
			Args: []interface{}{
				"metric_1",
				[]string{"__name__", "name_1"},
				[]string{"metric_1", "value_2"},
			},
			Results: model.RowResults{{"table", int64(2)}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},

		// first labels cache refresh, does not trash
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: model.RowResults{{int64(1)}},
			Err:     error(nil),
		},

		// second labels cache refresh, trash the cache
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: model.RowResults{{int64(2)}},
			Err:     error(nil),
		},

		// repopulate the cache
		{Sql: "BEGIN;"},
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: model.RowResults{{int64(2)}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},
		{Sql: "BEGIN;"},
		{
			Sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
			Args: []interface{}{
				"metric_1",
				[]string{"__name__", "name_1"},
				[]string{"metric_1", "value_1"},
			},
			Results: model.RowResults{{"table", int64(3)}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},
		{Sql: "BEGIN;"},
		{
			Sql: "SELECT * FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, $2, $3)",
			Args: []interface{}{
				"metric_1",
				[]string{"__name__", "name_1"},
				[]string{"metric_1", "value_2"},
			},
			Results: model.RowResults{{"table", int64(4)}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},
	}

	mock := model.NewSqlRecorder(sqlQueries, t)

	handler := insertHandler{
		conn: mock,
	}
	inserter := pgxInserter{
		conn: mock,
	}

	makeSamples := func(series []labels.Labels) []model.SamplesInfo {
		lsi := make([]model.SamplesInfo, 0)
		for _, ser := range series {
			ls, err := scache.GetSeriesFromLabels(ser)
			if err != nil {
				t.Errorf("invalid labels %+v, %v", ls, err)
			}
			lsi = append(lsi, model.SamplesInfo{Labels: ls})
		}
		return lsi
	}

	samples := makeSamples(series)
	err := handler.setSeriesIds(samples)
	if err != nil {
		t.Fatal(err)
	}

	expectedIds := map[string]model.SeriesID{
		"value_1": model.SeriesID(1),
		"value_2": model.SeriesID(2),
	}

	for _, si := range samples {
		value := si.Labels.Values[1]
		expectedId := expectedIds[value]
		gotId, _, err := si.Labels.GetSeriesID()
		require.NoError(t, err)
		if gotId != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", gotId, expectedId)
		}
	}

	// refreshing during the same epoch givesthe same IDs without checking the DB
	inserter.refreshSeriesEpoch(1)

	samples = makeSamples(series)
	err = handler.setSeriesIds(samples)
	if err != nil {
		t.Fatal(err)
	}

	for _, si := range samples {
		value := si.Labels.Values[1]
		expectedId := expectedIds[value]
		gotId, _, err := si.Labels.GetSeriesID()
		require.NoError(t, err)
		if gotId != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", gotId, expectedId)
		}
	}

	// trash the cache
	inserter.refreshSeriesEpoch(1)

	// retrying rechecks the DB and uses the new IDs
	samples = makeSamples(series)
	err = handler.setSeriesIds(samples)
	if err != nil {
		t.Fatal(err)
	}

	expectedIds = map[string]model.SeriesID{
		"value_1": model.SeriesID(3),
		"value_2": model.SeriesID(4),
	}

	for _, si := range samples {
		value := si.Labels.Values[1]
		expectedId := expectedIds[value]
		gotId, _, err := si.Labels.GetSeriesID()
		require.NoError(t, err)
		if gotId != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", gotId, expectedId)
		}
	}
}

func TestPGXInserterInsertData(t *testing.T) {
	makeLabel := func() *model.Series {
		l := &model.Series{}
		l.SetSeriesID(1, 1)
		return l
	}

	testCases := []struct {
		name          string
		rows          map[string][]model.SamplesInfo
		sqlQueries    []model.SqlQuery
		metricsGetErr error
	}{
		{
			name: "Zero data",
			sqlQueries: []model.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
			},
		},
		{
			name: "One data",
			rows: map[string][]model.SamplesInfo{
				"metric_0": {{Samples: make([]prompb.Sample, 1), Labels: makeLabel()}},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{{"metric_0", true}},
					Err:     error(nil),
				},
				{
					Sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					Args: []interface{}{
						[]time.Time{time.Unix(0, 0)},
						[]float64{0},
						[]int64{1},
					},
					Results: model.RowResults{{pgconn.CommandTag{'1'}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{int64(1)},
					Results: model.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Two data",
			rows: map[string][]model.SamplesInfo{
				"metric_0": {
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
				},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{{"metric_0", true}},
					Err:     error(nil),
				},

				{
					Sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					Args: []interface{}{
						[]time.Time{time.Unix(0, 0), time.Unix(0, 0)},
						[]float64{0, 0},
						[]int64{1, 1},
					},
					Results: model.RowResults{{pgconn.CommandTag{'1'}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{int64(1)},
					Results: model.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Create table error",
			rows: map[string][]model.SamplesInfo{
				"metric_0": {
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
				},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{},
					Err:     fmt.Errorf("create table error"),
				},
			},
		},
		{
			name: "Epoch Error",
			rows: map[string][]model.SamplesInfo{
				"metric_0": {{Samples: make([]prompb.Sample, 1), Labels: makeLabel()}},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{{"metric_0", true}},
					Err:     error(nil),
				},

				{
					Sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					Args: []interface{}{
						[]time.Time{time.Unix(0, 0)},
						[]float64{0},
						[]int64{1},
					},
					Results: model.RowResults{},
					Err:     error(nil),
				},
				{
					//this is the attempt on the full batch
					Sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{int64(1)},
					Results: model.RowResults{{[]byte{}}},
					Err:     fmt.Errorf("epoch error"),
				},

				{
					Sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					Args: []interface{}{
						[]time.Time{time.Unix(0, 0)},
						[]float64{0},
						[]int64{1},
					},
					Results: model.RowResults{},
					Err:     error(nil),
				},
				{
					//this is the attempt on the individual copyRequests
					Sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{int64(1)},
					Results: model.RowResults{{[]byte{}}},
					Err:     fmt.Errorf("epoch error"),
				},
			},
		},
		{
			name: "Copy from error",
			rows: map[string][]model.SamplesInfo{
				"metric_0": {
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
				},
			},

			sqlQueries: []model.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{{"metric_0", true}},
					Err:     error(nil),
				},

				{
					Sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					Args: []interface{}{
						[]time.Time{time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0)},
						make([]float64, 5),
						[]int64{1, 1, 1, 1, 1},
					},
					Results: model.RowResults{{pgconn.CommandTag{'1'}}},
					Err:     fmt.Errorf("some INSERT error"),
				},
				{
					// this is the entire batch insert
					Sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{int64(1)},
					Results: model.RowResults{{[]byte{}}},
					Err:     error(nil),
				},

				{
					Sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					Args: []interface{}{
						[]time.Time{time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0)},
						make([]float64, 5),
						[]int64{1, 1, 1, 1, 1},
					},
					Results: model.RowResults{{pgconn.CommandTag{'1'}}},
					Err:     fmt.Errorf("some INSERT error"),
				},
				{
					// this is the retry on individual copy requests
					Sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{int64(1)},
					Results: model.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Can't find/create table in DB",
			rows: map[string][]model.SamplesInfo{
				"metric_0": {
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
					{Samples: make([]prompb.Sample, 1), Labels: makeLabel()},
				},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:  "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args: []interface{}{"metric_0"},
					// no results is deliberate
					Results: model.RowResults{},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Metrics get error",
			rows: map[string][]model.SamplesInfo{
				"metric_0": {{Samples: make([]prompb.Sample, 1)}},
			},
			metricsGetErr: fmt.Errorf("some metrics error"),
			sqlQueries: []model.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
			},
		},
	}

	for _, co := range testCases {
		c := co
		t.Run(c.name, func(t *testing.T) {
			mock := model.NewSqlRecorder(c.sqlQueries, t)

			metricCache := map[string]string{"metric_1": "metricTableName_1"}
			mockMetrics := &model.MockMetricCache{
				MetricCache:  metricCache,
				GetMetricErr: c.metricsGetErr,
			}
			inserter, err := newPgxInserter(mock, mockMetrics, &Cfg{DisableEpochSync: true})
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
				expErr = pgmodelErrs.ErrMissingTableName
			default:
				for _, q := range c.sqlQueries {
					if q.Err != nil {
						expErr = q.Err
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
