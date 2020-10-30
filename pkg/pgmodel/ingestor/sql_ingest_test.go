package ingestor

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/pgmodel/utils"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestPGXInserterInsertSeries(t *testing.T) {
	testCases := []struct {
		name       string
		series     []labels.Labels
		sqlQueries []utils.SqlQuery
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

			sqlQueries: []utils.SqlQuery{
				{Sql: "BEGIN;"},
				{
					Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{int64(1)}},
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
					Results: utils.RowResults{{"table", int64(1)}},
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
			sqlQueries: []utils.SqlQuery{
				{Sql: "BEGIN;"},
				{
					Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{int64(1)}},
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
					Results: utils.RowResults{{"table", int64(1)}},
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
					Results: utils.RowResults{{"table", int64(2)}},
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
			sqlQueries: []utils.SqlQuery{
				{Sql: "BEGIN;"},
				{
					Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{int64(1)}},
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
					Results: utils.RowResults{{"table", int64(1)}},
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
					Results: utils.RowResults{{"table", int64(2)}},
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
			sqlQueries: []utils.SqlQuery{
				{Sql: "BEGIN;"},
				{
					Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{int64(1)}},
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
					Results: utils.RowResults{{"table", int64(1)}},
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
					Results: utils.RowResults{{"table", int64(2)}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mock := utils.NewSqlRecorder(c.sqlQueries, t)

			inserter := insertHandler{
				conn:             mock,
				seriesCache:      make(map[string]utils.SeriesID),
				seriesCacheEpoch: -1,
			}

			lsi := make([]utils.SamplesInfo, 0)
			for _, ser := range c.series {
				ls, err := utils.LabelsFromSlice(ser)
				if err != nil {
					t.Errorf("invalid labels %+v, %v", ls, err)
				}
				lsi = append(lsi, utils.SamplesInfo{Labels: ls, SeriesID: -1})
			}

			_, _, err := inserter.setSeriesIds(lsi)
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
					if si.SeriesID <= 0 {
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

	sqlQueries := []utils.SqlQuery{

		// first series cache fetch
		{Sql: "BEGIN;"},
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: utils.RowResults{{int64(1)}},
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
			Results: utils.RowResults{{"table", int64(1)}},
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
			Results: utils.RowResults{{"table", int64(2)}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},

		// first labels cache refresh, does not trash
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: utils.RowResults{{int64(1)}},
			Err:     error(nil),
		},

		// second labels cache refresh, trash the cache
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: utils.RowResults{{int64(2)}},
			Err:     error(nil),
		},

		// repopulate the cache
		{Sql: "BEGIN;"},
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: utils.RowResults{{int64(2)}},
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
			Results: utils.RowResults{{"table", int64(3)}},
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
			Results: utils.RowResults{{"table", int64(4)}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},
	}

	mock := utils.NewSqlRecorder(sqlQueries, t)

	inserter := insertHandler{
		conn:             mock,
		seriesCache:      make(map[string]utils.SeriesID),
		seriesCacheEpoch: -1,
	}

	makeSamples := func(series []labels.Labels) []utils.SamplesInfo {
		lsi := make([]utils.SamplesInfo, 0)
		for _, ser := range series {
			ls, err := utils.LabelsFromSlice(ser)
			if err != nil {
				t.Errorf("invalid labels %+v, %v", ls, err)
			}
			lsi = append(lsi, utils.SamplesInfo{Labels: ls, SeriesID: -1})
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
		value := si.Labels.Values[1]
		expectedId := expectedIds[value]
		if si.SeriesID != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", si.SeriesID, expectedId)
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
		value := si.Labels.Values[1]
		expectedId := expectedIds[value]
		if si.SeriesID != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", si.SeriesID, expectedId)
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
		value := si.Labels.Values[1]
		expectedId := expectedIds[value]
		if si.SeriesID != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", si.SeriesID, expectedId)
		}
	}
}

func TestPGXInserterInsertData(t *testing.T) {
	testCases := []struct {
		name          string
		rows          map[string][]utils.SamplesInfo
		sqlQueries    []utils.SqlQuery
		metricsGetErr error
	}{
		{
			name: "Zero data",
			sqlQueries: []utils.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
			},
		},
		{
			name: "One data",
			rows: map[string][]utils.SamplesInfo{
				"metric_0": {{Samples: make([]prompb.Sample, 1)}},
			},
			sqlQueries: []utils.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: utils.RowResults{{"metric_0", true}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{int64(-1)},
					Results: utils.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
				{
					Sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					Args: []interface{}{
						[]time.Time{time.Unix(0, 0)},
						[]float64{0},
						[]int64{0},
					},
					Results: utils.RowResults{{pgconn.CommandTag{'1'}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Two data",
			rows: map[string][]utils.SamplesInfo{
				"metric_0": {
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: utils.RowResults{{"metric_0", true}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{int64(-1)},
					Results: utils.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
				{
					Sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					Args: []interface{}{
						[]time.Time{time.Unix(0, 0), time.Unix(0, 0)},
						[]float64{0, 0},
						[]int64{0, 0},
					},
					Results: utils.RowResults{{pgconn.CommandTag{'1'}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Create table error",
			rows: map[string][]utils.SamplesInfo{
				"metric_0": {
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: utils.RowResults{},
					Err:     fmt.Errorf("create table error"),
				},
			},
		},
		{
			name: "Epoch Error",
			rows: map[string][]utils.SamplesInfo{
				"metric_0": {{Samples: make([]prompb.Sample, 1)}},
			},
			sqlQueries: []utils.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: utils.RowResults{{"metric_0", true}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{int64(-1)},
					Results: utils.RowResults{{[]byte{}}},
					Err:     fmt.Errorf("epoch error"),
				},
				{
					Sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					Args: []interface{}{
						[]time.Time{time.Unix(0, 0)},
						[]float64{0},
						[]int64{0},
					},
					Results: utils.RowResults{},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Copy from error",
			rows: map[string][]utils.SamplesInfo{
				"metric_0": {
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
				},
			},

			sqlQueries: []utils.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: utils.RowResults{{"metric_0", true}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{int64(-1)},
					Results: utils.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
				{
					Sql: `INSERT INTO "prom_data"."metric_0"(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING`,
					Args: []interface{}{
						[]time.Time{time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0)},
						make([]float64, 5),
						make([]int64, 5),
					},
					Results: utils.RowResults{{pgconn.CommandTag{'1'}}},
					Err:     fmt.Errorf("some INSERT error"),
				},
			},
		},
		{
			name: "Can't find/create table in DB",
			rows: map[string][]utils.SamplesInfo{
				"metric_0": {
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
					{Samples: make([]prompb.Sample, 1)},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:  "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args: []interface{}{"metric_0"},
					// no results is deliberate
					Results: utils.RowResults{},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Metrics get error",
			rows: map[string][]utils.SamplesInfo{
				"metric_0": {{Samples: make([]prompb.Sample, 1)}},
			},
			metricsGetErr: fmt.Errorf("some metrics error"),
			sqlQueries: []utils.SqlQuery{
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
			},
		},
	}

	for _, co := range testCases {
		c := co
		t.Run(c.name, func(t *testing.T) {
			mock := utils.NewSqlRecorder(c.sqlQueries, t)

			metricCache := map[string]string{"metric_1": "metricTableName_1"}
			mockMetrics := &utils.MockMetricCache{
				MetricCache:  metricCache,
				GetMetricErr: c.metricsGetErr,
			}
			inserter, err := newPgxInserter(mock, mockMetrics, &Config{})
			if err != nil {
				t.Fatal(err)
			}

			_, err = inserter.InsertData(c.rows)

			var expErr error

			switch {
			case c.metricsGetErr != nil:
				expErr = c.metricsGetErr
			case c.name == "Can't find/create table in DB":
				expErr = utils.ErrMissingTableName
			default:
				for _, q := range c.sqlQueries {
					if q.Err != nil {
						expErr = q.Err
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
