// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestMetricTableName(t *testing.T) {
	testCases := []struct {
		name        string
		tableName   string
		errExpected bool
		sqlQueries  []model.SqlQuery
	}{
		{
			name:      "no error",
			tableName: "res1",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"t1"},
					Results: model.RowResults{{"res1", true}},
				},
			},
		},
		{
			name:      "no error2",
			tableName: "res2",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"t1"},
					Results: model.RowResults{{"res2", true}},
				},
			},
		},
		{
			name:        "error",
			tableName:   "res1",
			errExpected: true,
			sqlQueries: []model.SqlQuery{
				{
					Sql:  "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args: []interface{}{"t1"},
					Err:  fmt.Errorf("test"),
				},
			},
		},
		{
			name:        "empty table name",
			tableName:   "res2",
			errExpected: true,
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"t1"},
					Results: model.RowResults{{"", true}},
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mock := model.NewSqlRecorder(c.sqlQueries, t)

			name, _, err := metricTableName(mock, "t1")
			require.Equal(t, c.errExpected, err != nil)

			if err == nil {
				require.Equal(t, c.tableName, name)
			}
		})
	}
}

func TestOrderExemplarLabelValues(t *testing.T) {
	rawExemplars := []prompb.Exemplar{
		{
			Labels:    []prompb.Label{{Name: "TraceID", Value: "some_trace_id"}, {Name: "component", Value: "tester"}},
			Value:     1.5,
			Timestamp: 1,
		},
		{
			Labels:    []prompb.Label{{Name: "app", Value: "test"}, {Name: "component", Value: "tester"}},
			Value:     2.5,
			Timestamp: 3,
		},
		{
			Labels:    []prompb.Label{}, // No labels. A valid label according to Open Metrics.
			Value:     3.5,
			Timestamp: 5,
		},
	}
	exemplarSeriesLabels := []prompb.Label{{Name: "__name__", Value: "exemplar_test_metric"}, {Name: "component", Value: "test_infra"}}

	series := model.NewSeries("hash_key", exemplarSeriesLabels)
	insertables := make([]model.Insertable, 3) // Since 3 exemplars.

	for i, exemplar := range rawExemplars {
		insertable := model.NewInsertable(series, []prompb.Exemplar{exemplar}) // To be in line with write request behaviour.
		insertables[i] = insertable
	}

	mockConn := model.NewSqlRecorder([]model.SqlQuery{}, t)
	posCache := cache.NewExemplarLabelsPosCache(cache.Config{ExemplarCacheSize: 4})
	prepareExemplarPosCache(posCache)

	exemplarCatalog := &exemplarInfo{
		exemplarCache: posCache,
	}
	err := orderExemplarLabelValues(mockConn, exemplarCatalog, insertables)
	require.NoError(t, err)

	// Verify exemplar label value positioning.
	require.Equal(t, []prompb.Label{{Value: "some_trace_id"}, {Value: "tester"}, {Value: model.EmptyExemplarValues}}, insertables[0].At(0).ExemplarLabels())
	require.Equal(t, []prompb.Label{{Value: model.EmptyExemplarValues}, {Value: "tester"}, {Value: "test"}}, insertables[1].At(0).ExemplarLabels())
	require.Equal(t, []prompb.Label{{Value: model.EmptyExemplarValues}, {Value: model.EmptyExemplarValues}, {Value: model.EmptyExemplarValues}}, insertables[2].At(0).ExemplarLabels())
	// Verify if all label names are empty.
	for _, i := range insertables {
		lbls := i.At(0).ExemplarLabels()
		for j := range lbls {
			require.Equal(t, "", lbls[j].Name, i)
		}
	}
}

func prepareExemplarPosCache(posCache cache.PositionCache) {
	index := map[string]int{
		"TraceID":   1,
		"component": 2,
		"app":       3,
	}
	posCache.SetorUpdateLabelPositions("exemplar_test_metric", index)
}
