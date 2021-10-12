// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
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

func TestInitializeMetricBatcher(t *testing.T) {
	metricName := "mock_metric"
	metricTableName := "mock_metric_table_name"
	sqlQueries := []model.SqlQuery{
		{
			Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
			Args:    []interface{}{metricName},
			Results: model.RowResults{{metricTableName, true}},
		},
	}
	mock := model.NewSqlRecorder(sqlQueries, t)
	mockMetrics := &model.MockMetricCache{
		MetricCache: make(map[string]model.MetricInfo),
	}
	completeMetricCreation := make(chan struct{}, 1)

	tableName, err := initializeMetricBatcher(mock, metricName, completeMetricCreation, mockMetrics)
	require.Nil(t, err)
	require.Equal(t, metricTableName, tableName)

	// Double-check the cache was set properly.
	mInfo, err := mockMetrics.Get(schema.Data, metricName, false)
	require.Nil(t, err)
	require.Equal(t, schema.Data, mInfo.TableSchema)
	require.Equal(t, metricTableName, mInfo.TableName)
	require.Equal(t, metricTableName, mInfo.SeriesTable)

}

type insertableVisitor []model.Insertable

func (insertables insertableVisitor) VisitExemplar(callBack func(s *pgmodel.PromExemplars) error) error {
	for i := range insertables {
		exemplar, ok := insertables[i].(*pgmodel.PromExemplars)
		if ok {
			if err := callBack(exemplar); err != nil {
				return err
			}
		}
	}

	return nil
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
		insertable := model.NewPromExemplars(series, []prompb.Exemplar{exemplar}) // To be in line with write request behaviour.
		insertables[i] = insertable
	}

	mockConn := model.NewSqlRecorder([]model.SqlQuery{}, t)
	posCache := cache.NewExemplarLabelsPosCache(cache.Config{ExemplarKeyPosCacheSize: 4})
	prepareExemplarPosCache(posCache)

	elf := NewExamplarLabelFormatter(mockConn, posCache)
	err := elf.orderExemplarLabelValues(insertableVisitor(insertables))
	require.NoError(t, err)

	// Verify exemplar label value positioning.
	for i, insertable := range insertables {
		if !insertable.IsOfType(model.Exemplar) {
			continue
		}
		itr := insertable.Iterator().(model.ExemplarsIterator)
		// Note: Each insertable contains exactly a single exemplar sample.
		require.True(t, itr.HasNext())
		labels, _, _ := itr.Value()
		labelKeys, labelValues := separateLabelKeysValues(labels)
		require.True(t, allLabelKeysEmpty(labelKeys))
		switch i {
		case 0:
			require.Equal(t, []prompb.Label{{Value: "some_trace_id"}, {Value: "tester"}, {Value: model.EmptyExemplarValues}}, labelValues)
		case 1:
			require.Equal(t, []prompb.Label{{Value: model.EmptyExemplarValues}, {Value: "tester"}, {Value: "test"}}, labelValues)
		case 2:
			require.Equal(t, []prompb.Label{{Value: model.EmptyExemplarValues}, {Value: model.EmptyExemplarValues}, {Value: model.EmptyExemplarValues}}, labelValues)
		default:
			require.Fail(t, "count was not expected", i)
		}
		require.False(t, itr.HasNext())
	}
}

func separateLabelKeysValues(lbls []prompb.Label) (onlyLabelKeys, onlyLabelValues []prompb.Label) {
	onlyLabelKeys = make([]prompb.Label, len(lbls))
	onlyLabelValues = make([]prompb.Label, len(lbls))
	copy(onlyLabelKeys, lbls[:])
	copy(onlyLabelValues, lbls[:])
	for i := range onlyLabelKeys {
		onlyLabelKeys[i].Value = ""
	}
	for i := range onlyLabelValues {
		onlyLabelValues[i].Name = ""
	}
	return
}

func allLabelKeysEmpty(lbls []prompb.Label) bool {
	for i := range lbls {
		if lbls[i].Name != "" {
			return false
		}
	}
	return true
}

func prepareExemplarPosCache(posCache cache.PositionCache) {
	index := map[string]int{
		"TraceID":   1,
		"component": 2,
		"app":       3,
	}
	posCache.SetOrUpdateLabelPositions("exemplar_test_metric", index)
}
