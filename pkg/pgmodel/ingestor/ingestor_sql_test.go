// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	pgmodelErrs "github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/model/pgutf8str"
	"github.com/timescale/promscale/pkg/prompb"
	tput "github.com/timescale/promscale/pkg/util/throughput"
)

const (
	metricID  int64  = 1
	tableName string = "table name"
)

func getTestLabelArray(t *testing.T, l [][]int32) *pgtype.ArrayType {
	model.SetLabelArrayOIDForTest(0)
	labelArrayArray := model.GetCustomType(model.LabelArray)
	err := labelArrayArray.Set(l)
	require.NoError(t, err)
	return labelArrayArray
}

func init() {
	tput.InitWatcher(time.Second)
}

type sVisitor []model.Insertable

func (c sVisitor) VisitSeries(cb func(info *pgmodel.MetricInfo, s *pgmodel.Series) error) error {
	info := &pgmodel.MetricInfo{
		MetricID:  metricID,
		TableName: tableName,
	}
	for _, insertable := range c {
		err := cb(info, insertable.Series())
		if err != nil {
			return err
		}
	}
	return nil
}

func TestPGXInserterInsertSeries(t *testing.T) {
	// Set test env so that cache metrics uses a new registry and avoid panic on duplicate register.
	require.NoError(t, os.Setenv("IS_TEST", "true"))
	initialTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
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
					Results: model.RowResults{{initialTime}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: "SELECT * FROM _prom_catalog.get_or_create_label_ids($1, $2, $3, $4)",
					Args: []interface{}{
						"metric_1",
						tableName,
						[]string{"__name__", "name_1"},
						[]string{"metric_1", "value_1"},
					},
					Results: model.RowResults{
						{[]int32{1, 2}, []int32{1, 2}, []string{"__name__", "name_1"}, []string{"metric_1", "value_1"}},
					},
					Err: error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: seriesInsertSQL,
					Args: []interface{}{
						metricID,
						tableName,
						getTestLabelArray(t, [][]int32{{1, 2}}),
					},
					Results: model.RowResults{{int64(1), int64(1)}},
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
					{Name: "__name__", Value: "metric_1"},
				},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "BEGIN;"},
				{
					Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}(nil),
					Results: model.RowResults{{initialTime}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: "SELECT * FROM _prom_catalog.get_or_create_label_ids($1, $2, $3, $4)",
					Args: []interface{}{
						"metric_1",
						tableName,
						[]string{"__name__", "name_1", "name_2"},
						[]string{"metric_1", "value_1", "value_2"},
					},
					Results: model.RowResults{
						{[]int32{1, 2, 3}, []int32{1, 2, 3}, []string{"__name__", "name_1", "name_2"}, []string{"metric_1", "value_1", "value_2"}},
					},
					Err: error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: seriesInsertSQL,
					Args: []interface{}{
						metricID,
						tableName,
						getTestLabelArray(t, [][]int32{{1, 2}, {1, 0, 3}}),
					},
					Results: model.RowResults{{int64(1), int64(1)}, {int64(2), int64(2)}},
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
					{Name: "__name__", Value: "metric_1"}},
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
					Results: model.RowResults{{initialTime}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: "SELECT * FROM _prom_catalog.get_or_create_label_ids($1, $2, $3, $4)",
					Args: []interface{}{
						"metric_1",
						tableName,
						[]string{"__name__", "name_1", "name_2"},
						[]string{"metric_1", "value_1", "value_2"},
					},
					Results: model.RowResults{
						{[]int32{1, 2, 3}, []int32{1, 2, 3}, []string{"__name__", "name_1", "name_2"}, []string{"metric_1", "value_1", "value_2"}},
					},
					Err: error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: seriesInsertSQL,
					Args: []interface{}{
						metricID,
						tableName,
						getTestLabelArray(t, [][]int32{{1, 2}, {1, 0, 3}, {1, 2}}),
					},
					Results: model.RowResults{{int64(1), int64(1)}, {int64(2), int64(2)}, {int64(1), int64(1)}},
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
					{Name: "__name__", Value: "metric_1"},
				},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "BEGIN;"},
				{
					Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}(nil),
					Results: model.RowResults{{initialTime}},
					Err:     error(nil),
				},
				{Sql: "COMMIT;"},
				{Sql: "BEGIN;"},
				{
					Sql: "SELECT * FROM _prom_catalog.get_or_create_label_ids($1, $2, $3, $4)",
					Args: []interface{}{
						"metric_1",
						tableName,
						[]string{"__name__", "name_1", "name_2"},
						[]string{"metric_1", "value_1", "value_2"},
					},
					Results: model.RowResults{
						{int32(1), int32(1), "__name__", "metric_1"},
						{int32(1), int32(2), "__name__", "metric_2"},
						{int32(2), int32(3), "name_1", "value_1"},
						{int32(3), int32(4), "name_2", "value_2"},
					},
					Err: fmt.Errorf("some query error"),
				},
				{Sql: "COMMIT;"},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			for i := range c.sqlQueries {
				for j := range c.sqlQueries[i].Args {
					if _, ok := c.sqlQueries[i].Args[j].([]string); ok {
						tmp := &pgutf8str.TextArray{}
						err := tmp.Set(c.sqlQueries[i].Args[j])
						require.NoError(t, err)
						c.sqlQueries[i].Args[j] = tmp
					}
				}
			}
			mock := model.NewSqlRecorder(c.sqlQueries, t)
			scache := cache.NewSeriesCache(cache.DefaultConfig, nil)
			scache.Reset()
			lCache, _ := cache.NewInvertedLabelsCache(10)
			sw := NewSeriesWriter(mock, 0, lCache, scache)

			lsi := make([]model.Insertable, 0)
			for _, ser := range c.series {
				ls, err := scache.GetSeriesFromLabels(ser)
				if err != nil {
					t.Errorf("invalid labels %+v, %v", ls, err)
				}
				lsi = append(lsi, model.NewPromExemplars(ls, nil))
			}

			_, err := sw.PopulateOrCreateSeries(context.Background(), sVisitor(lsi))
			if err != nil {
				foundErr := false
				for _, q := range c.sqlQueries {
					if q.Err != nil {
						foundErr = true
						if !errors.Is(err, q.Err) {
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
					si, err := si.Series().GetSeriesID()
					require.NoError(t, err)
					require.True(t, si > 0, "series id not set")
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

	initialTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	newTime := time.Date(2022, 1, 1, 1, 0, 0, 0, time.UTC).Unix()

	sqlQueries := []model.SqlQuery{

		// first series cache fetch
		{Sql: "BEGIN;"},
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: model.RowResults{{initialTime}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},
		{Sql: "BEGIN;"},
		{
			Sql: "SELECT * FROM _prom_catalog.get_or_create_label_ids($1, $2, $3, $4)",
			Args: []interface{}{
				"metric_1",
				tableName,
				[]string{"__name__", "name_1", "name_1"},
				[]string{"metric_1", "value_1", "value_2"},
			},
			Results: model.RowResults{
				{[]int32{1, 2, 2}, []int32{1, 2, 3}, []string{"__name__", "name_1", "name_1"}, []string{"metric_1", "value_1", "value_2"}},
			},
			Err: error(nil),
		},
		{Sql: "COMMIT;"},
		{Sql: "BEGIN;"},
		{
			Sql: seriesInsertSQL,
			Args: []interface{}{
				metricID,
				tableName,
				getTestLabelArray(t, [][]int32{{1, 2}, {1, 3}}),
			},
			Results: model.RowResults{{int64(1), int64(1)}, {int64(2), int64(2)}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},

		// first labels cache refresh, does not trash
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: model.RowResults{{initialTime}},
			Err:     error(nil),
		},

		// second labels cache refresh, trash the cache
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: model.RowResults{{newTime}},
			Err:     error(nil),
		},
		{Sql: "BEGIN;"},

		// repopulate the cache
		{
			Sql:     "SELECT current_epoch FROM _prom_catalog.ids_epoch LIMIT 1",
			Args:    []interface{}(nil),
			Results: model.RowResults{{newTime}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},
		{Sql: "BEGIN;"},
		{
			Sql: "SELECT * FROM _prom_catalog.get_or_create_label_ids($1, $2, $3, $4)",
			Args: []interface{}{
				"metric_1",
				tableName,
				[]string{"__name__", "name_1", "name_1"},
				[]string{"metric_1", "value_1", "value_2"},
			},
			Results: model.RowResults{
				{[]int32{1, 2, 2}, []int32{1, 2, 3}, []string{"__name__", "name_1", "name_1"}, []string{"metric_1", "value_1", "value_2"}},
			},
			Err: error(nil),
		},
		{Sql: "COMMIT;"},
		{Sql: "BEGIN;"},
		{
			Sql: seriesInsertSQL,
			Args: []interface{}{
				metricID,
				tableName,
				getTestLabelArray(t, [][]int32{{1, 2}, {1, 3}}),
			},
			Results: model.RowResults{{int64(3), int64(1)}, {int64(4), int64(2)}},
			Err:     error(nil),
		},
		{Sql: "COMMIT;"},
	}

	for i := range sqlQueries {
		for j := range sqlQueries[i].Args {
			if _, ok := sqlQueries[i].Args[j].([]string); ok {
				tmp := &pgutf8str.TextArray{}
				err := tmp.Set(sqlQueries[i].Args[j])
				require.NoError(t, err)
				sqlQueries[i].Args[j] = tmp
			}
		}
	}

	mock := model.NewSqlRecorder(sqlQueries, t)
	scache := cache.NewSeriesCache(cache.DefaultConfig, nil)
	lcache, _ := cache.NewInvertedLabelsCache(10)
	sw := NewSeriesWriter(mock, 0, lcache, scache)
	inserter := pgxDispatcher{
		conn:                mock,
		scache:              scache,
		invertedLabelsCache: lcache,
	}

	makeSamples := func(series []labels.Labels) []model.Insertable {
		lsi := make([]model.Insertable, 0)
		for _, ser := range series {
			ls, err := scache.GetSeriesFromLabels(ser)
			if err != nil {
				t.Errorf("invalid labels %+v, %v", ls, err)
			}
			lsi = append(lsi, model.NewPromSamples(ls, nil))
		}
		return lsi
	}

	samples := makeSamples(series)
	_, err := sw.PopulateOrCreateSeries(context.Background(), sVisitor(samples))
	if err != nil {
		t.Fatal(err)
	}

	expectedIds := []model.SeriesID{
		model.SeriesID(1),
		model.SeriesID(2),
	}

	for index, si := range samples {
		_, _, ok := si.Series().NameValues()
		require.False(t, ok)
		expectedId := expectedIds[index]
		gotId, err := si.Series().GetSeriesID()
		require.NoError(t, err)
		if gotId != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", gotId, expectedId)
		}
	}

	// refreshing during the same epoch gives the same IDs without checking the DB
	inserter.refreshSeriesEpoch()
	require.NoError(t, err)

	samples = makeSamples(series)
	_, err = sw.PopulateOrCreateSeries(context.Background(), sVisitor(samples))
	if err != nil {
		t.Fatal(err)
	}

	for index, si := range samples {
		_, _, ok := si.Series().NameValues()
		require.False(t, ok)
		expectedId := expectedIds[index]
		gotId, err := si.Series().GetSeriesID()
		require.NoError(t, err)
		if gotId != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", gotId, expectedId)
		}
	}

	// trash the cache
	inserter.refreshSeriesEpoch()
	require.NoError(t, err)

	// retrying rechecks the DB and uses the new IDs
	samples = makeSamples(series)
	_, err = sw.PopulateOrCreateSeries(context.Background(), sVisitor(samples))
	if err != nil {
		t.Fatal(err)
	}

	expectedIds = []model.SeriesID{
		model.SeriesID(3),
		model.SeriesID(4),
	}

	for index, si := range samples {
		_, _, ok := si.Series().NameValues()
		require.False(t, ok)
		expectedId := expectedIds[index]
		gotId, err := si.Series().GetSeriesID()
		require.NoError(t, err)
		if gotId != expectedId {
			t.Errorf("incorrect ID:\ngot: %v\nexpected: %v", gotId, expectedId)
		}
	}
}

func TestPGXInserterInsertData(t *testing.T) {
	if err := os.Setenv("IS_TEST", "true"); err != nil {
		t.Fatal(err)
	}
	testTime := time.Now().Unix()
	makeLabel := func() *model.Series {
		l := &model.Series{}
		l.SetSeriesID(1)
		return l
	}

	testCases := []struct {
		name          string
		rows          map[string][]model.Insertable
		sqlQueries    []model.SqlQuery
		metricsGetErr error
	}{
		{
			name: "Zero data",
			sqlQueries: []model.SqlQuery{
				{Sql: "SELECT 'prom_api.label_array'::regtype::oid", Results: model.RowResults{{uint32(434)}}},
				{Sql: "SELECT 'prom_api.label_value_array'::regtype::oid", Results: model.RowResults{{uint32(435)}}},
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
			},
		},
		{
			name: "One data",
			rows: map[string][]model.Insertable{
				"metric_0": {model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1))},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "SELECT 'prom_api.label_array'::regtype::oid", Results: model.RowResults{{uint32(434)}}},
				{Sql: "SELECT 'prom_api.label_value_array'::regtype::oid", Results: model.RowResults{{uint32(435)}}},
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT id, table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{{int64(1), "metric_0", false}},
					Err:     error(nil),
				},
				{
					Copy: &model.Copy{
						Table: pgx.Identifier{"prom_data", "metric_0"},
						Data: [][]interface{}{
							{time.Unix(0, 0), float64(0), int64(1)},
						},
					},
					Results: model.RowResults{{int64(1)}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT CASE $1 <= delete_epoch WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{testTime},
					Results: model.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Two data",
			rows: map[string][]model.Insertable{
				"metric_0": {
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
				},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "SELECT 'prom_api.label_array'::regtype::oid", Results: model.RowResults{{uint32(434)}}},
				{Sql: "SELECT 'prom_api.label_value_array'::regtype::oid", Results: model.RowResults{{uint32(435)}}},
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT id, table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{{int64(1), "metric_0", false}},
					Err:     error(nil),
				},

				{
					Sql: "SELECT _prom_catalog.insert_metric_row($1, $2::TIMESTAMPTZ[], $3::DOUBLE PRECISION[], $4::BIGINT[])",
					Args: []interface{}{
						"metric_0",
						[]time.Time{time.Unix(0, 0), time.Unix(0, 0)},
						[]float64{0, 0},
						[]int64{1, 1},
					},
					Copy: &model.Copy{
						Table: pgx.Identifier{"prom_data", "metric_0"},
						Data: [][]interface{}{
							{time.Unix(0, 0), float64(0), int64(1)},
							{time.Unix(0, 0), float64(0), int64(1)},
						},
					},
					Results: model.RowResults{{int64(1)}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT CASE $1 <= delete_epoch WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{testTime},
					Results: model.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Create table error",
			rows: map[string][]model.Insertable{
				"metric_0": {
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
				},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "SELECT 'prom_api.label_array'::regtype::oid", Results: model.RowResults{{uint32(434)}}},
				{Sql: "SELECT 'prom_api.label_value_array'::regtype::oid", Results: model.RowResults{{uint32(435)}}},
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT id, table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{{int64(1), "metric_0", false}},
					Err:     fmt.Errorf("create table error"),
				},
			},
		},
		{
			name: "Epoch Error",
			rows: map[string][]model.Insertable{
				"metric_0": {
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
				},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "SELECT 'prom_api.label_array'::regtype::oid", Results: model.RowResults{{uint32(434)}}},
				{Sql: "SELECT 'prom_api.label_value_array'::regtype::oid", Results: model.RowResults{{uint32(435)}}},
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT id, table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{{int64(1), "metric_0", false}},
					Err:     error(nil),
				},

				{
					Copy: &model.Copy{
						Table: pgx.Identifier{"prom_data", "metric_0"},
						Data: [][]interface{}{
							{time.Unix(0, 0), float64(0), int64(1)},
						},
					},
					Results: model.RowResults{{int64(1)}},
					Err:     error(nil),
				},
				{
					//this is the attempt on the full batch
					Sql:     "SELECT CASE $1 <= delete_epoch WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{testTime},
					Results: model.RowResults{{[]byte{}}},
					Err:     fmt.Errorf("epoch error"),
				},
				{
					// on epoch error we go for on conflict insert approach
					Sql:     "SELECT _prom_catalog.create_ingest_temp_table($1, $2, $3)",
					Args:    []interface{}{"metric_0", "prom_data", "s1_"},
					Results: model.RowResults{{"s1_metric_0"}},
					Err:     error(nil),
				},
				{
					Copy: &model.Copy{
						Table: pgx.Identifier{"s1_metric_0"},
						Data: [][]interface{}{
							{time.Unix(0, 0), float64(0), int64(1)},
						},
					},
					Results: model.RowResults{{int64(1)}},
					Err:     error(nil),
				},
				// insert from temp table using on conflict
				{
					Sql:     "INSERT INTO prom_data.\"metric_0\"(time,value,series_id) SELECT time,value,series_id FROM \"s1_metric_0\" ON CONFLICT DO NOTHING",
					Results: model.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
				{
					//this is the attempt on the individual copyRequests
					Sql:     "SELECT CASE $1 <= delete_epoch WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{testTime},
					Results: model.RowResults{{[]byte{}}},
					Err:     fmt.Errorf("epoch error"),
				},
			},
		},
		{
			name: "Copy from error",
			rows: map[string][]model.Insertable{
				"metric_0": {
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
				},
			},

			sqlQueries: []model.SqlQuery{
				{Sql: "SELECT 'prom_api.label_array'::regtype::oid", Results: model.RowResults{{uint32(434)}}},
				{Sql: "SELECT 'prom_api.label_value_array'::regtype::oid", Results: model.RowResults{{uint32(435)}}},
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT id, table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{{int64(1), "metric_0", false}},
					Err:     error(nil),
				},

				{
					Copy: &model.Copy{
						Table: pgx.Identifier{"prom_data", "metric_0"},
						Data: [][]interface{}{
							{time.Unix(0, 0), float64(0), int64(1)},
							{time.Unix(0, 0), float64(0), int64(1)},
							{time.Unix(0, 0), float64(0), int64(1)},
							{time.Unix(0, 0), float64(0), int64(1)},
							{time.Unix(0, 0), float64(0), int64(1)},
						},
					},
					Results: model.RowResults{{int64(1)}},
					Err:     fmt.Errorf("some COPY error"),
				},
				{
					// on epoch error we go for on conflict insert approach
					Sql:     "SELECT _prom_catalog.create_ingest_temp_table($1, $2, $3)",
					Args:    []interface{}{"metric_0", "prom_data", "s1_"},
					Results: model.RowResults{{"s1_metric_0"}},
					Err:     error(nil),
				},
				// retry of insert of individual copy request
				{
					Copy: &model.Copy{
						Table: pgx.Identifier{"s1_metric_0"},
						Data: [][]interface{}{
							{time.Unix(0, 0), float64(0), int64(1)},
							{time.Unix(0, 0), float64(0), int64(1)},
							{time.Unix(0, 0), float64(0), int64(1)},
							{time.Unix(0, 0), float64(0), int64(1)},
							{time.Unix(0, 0), float64(0), int64(1)},
						},
					},
					Results: model.RowResults{{int64(1)}},
					Err:     fmt.Errorf("some COPY error"),
				},
			},
		},
		{
			name: "Can't find/create table in DB",
			rows: map[string][]model.Insertable{
				"metric_0": {
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
				},
			},
			sqlQueries: []model.SqlQuery{
				{Sql: "SELECT 'prom_api.label_array'::regtype::oid", Results: model.RowResults{{uint32(434)}}},
				{Sql: "SELECT 'prom_api.label_value_array'::regtype::oid", Results: model.RowResults{{uint32(435)}}},
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:  "SELECT id, table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args: []interface{}{"metric_0"},
					// no results is deliberate
					Results: model.RowResults{},
					Err:     error(nil),
				},
			},
		},
		{
			//cache errors get recovered from and the insert succeeds
			name: "Metrics cache get error",
			rows: map[string][]model.Insertable{
				"metric_0": {
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
				},
			},
			metricsGetErr: fmt.Errorf("some metrics error"),
			sqlQueries: []model.SqlQuery{
				{Sql: "SELECT 'prom_api.label_array'::regtype::oid", Results: model.RowResults{{uint32(434)}}},
				{Sql: "SELECT 'prom_api.label_value_array'::regtype::oid", Results: model.RowResults{{uint32(435)}}},
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT id, table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{{int64(1), "metric_0", true}},
					Err:     error(nil),
				},
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Copy: &model.Copy{
						Table: pgx.Identifier{"prom_data", "metric_0"},
						Data: [][]interface{}{
							{time.Unix(0, 0), float64(0), int64(1)},
						},
					},
					Results: model.RowResults{{int64(1)}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT CASE $1 <= delete_epoch WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{testTime},
					Results: model.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "On conflict error resolution",
			rows: map[string][]model.Insertable{
				"metric_0": {
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
					model.NewPromSamples(makeLabel(), make([]prompb.Sample, 1)),
				},
			},

			sqlQueries: []model.SqlQuery{
				{Sql: "SELECT 'prom_api.label_array'::regtype::oid", Results: model.RowResults{{uint32(434)}}},
				{Sql: "SELECT 'prom_api.label_value_array'::regtype::oid", Results: model.RowResults{{uint32(435)}}},
				{Sql: "CALL _prom_catalog.finalize_metric_creation()"},
				{
					Sql:     "SELECT id, table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"metric_0"},
					Results: model.RowResults{{int64(1), "metric_0", false}},
					Err:     error(nil),
				},

				{
					Copy: &model.Copy{
						Table: pgx.Identifier{"prom_data", "metric_0"},
						Data: [][]interface{}{
							{time.Unix(0, 0), float64(0), int64(1)},
							{time.Unix(0, 0), float64(0), int64(1)},
						},
					},
					Results: model.RowResults{{int64(1)}},
					Err: &pgconn.PgError{
						Code: "23505", // unique key violation
					},
				},
				// retry by creating temp table
				{
					Sql:     "SELECT _prom_catalog.create_ingest_temp_table($1, $2, $3)",
					Args:    []interface{}{"metric_0", "prom_data", "s1_"},
					Results: model.RowResults{{"s1_metric_0"}},
					Err:     error(nil),
				},
				// copy into created temp table
				{
					Copy: &model.Copy{
						Table: pgx.Identifier{"s1_metric_0"},
						Data: [][]interface{}{
							{time.Unix(0, 0), float64(0), int64(1)},
							{time.Unix(0, 0), float64(0), int64(1)},
						},
					},
					Results: model.RowResults{{int64(1)}},
					Err:     error(nil),
				},
				// insert from temp table using on conflict
				{
					Sql:     "INSERT INTO prom_data.\"metric_0\"(time,value,series_id) SELECT time,value,series_id FROM \"s1_metric_0\" ON CONFLICT DO NOTHING",
					Results: model.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
				// epoch check after insert from temp table
				{
					Sql:     "SELECT CASE $1 <= delete_epoch WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1",
					Args:    []interface{}{testTime},
					Results: model.RowResults{{[]byte{}}},
					Err:     error(nil),
				},
			},
		},
	}

	ctx := context.Background()

	for _, co := range testCases {
		c := co
		t.Run(c.name, func(t *testing.T) {
			mock := model.NewSqlRecorder(c.sqlQueries, t)
			scache := cache.NewSeriesCache(cache.DefaultConfig, nil)

			mockMetrics := &model.MockMetricCache{
				MetricCache:  make(map[string]model.MetricInfo),
				GetMetricErr: c.metricsGetErr,
			}
			err := mockMetrics.Set(
				"prom_data",
				"metric_1",
				model.MetricInfo{
					TableSchema: "prom_data",
					TableName:   "metricTableName_1",
					SeriesTable: "metric_1",
				}, false)
			if err != nil {
				t.Fatalf("error setting up mock cache: %s", err.Error())
			}
			inserter, err := newPgxDispatcher(mock, mockMetrics, scache, nil, &Cfg{DisableEpochSync: true, InvertedLabelsCacheSize: 10, NumCopiers: 2})
			if err != nil {
				t.Fatal(err)
			}
			defer inserter.Close()

			_, err = inserter.InsertTs(ctx, model.Data{Rows: c.rows})

			var expErr error
			switch {
			case c.metricsGetErr != nil:
				//cache errors recover
				expErr = nil
			case c.name == "Can't find/create table in DB":
				expErr = pgmodelErrs.ErrMissingTableName
			case c.name == "On conflict error resolution":
				expErr = nil
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
