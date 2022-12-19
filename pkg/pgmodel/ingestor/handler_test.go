// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

func getSeries(t *testing.T, scache *cache.SeriesCacheImpl, labels labels.Labels) *model.Series {
	series, err := scache.GetSeriesFromLabels(labels)
	require.NoError(t, err)
	return series
}

func makeLabelKey(l labels.Label) cache.LabelKey {
	return cache.LabelKey{MetricName: "metric", Name: l.Name, Value: l.Value}
}

func TestLabelArrayCreator(t *testing.T) {
	scache := cache.NewSeriesCache(cache.DefaultConfig, nil)
	metricNameLabel := labels.Label{Name: "__name__", Value: "metric"}
	valOne := labels.Label{Name: "key", Value: "one"}
	valTwo := labels.Label{Name: "key", Value: "two"}
	seriesSet := []*model.Series{
		getSeries(t, scache, labels.Labels{metricNameLabel, valOne}),
	}
	labelMap := map[cache.LabelKey]cache.LabelInfo{
		makeLabelKey(metricNameLabel): cache.NewLabelInfo(2, 1),
		makeLabelKey(valOne):          cache.NewLabelInfo(3, 2),
	}

	res, _, err := createLabelArrays(seriesSet, labelMap, 2)
	require.NoError(t, err)
	expected := model.ArrayOfLabelArray{
		model.LabelArray{
			{Int32: 2, Valid: true},
			{Int32: 3, Valid: true},
		},
	}
	require.Equal(t, res, expected)

	res, _, err = createLabelArrays(seriesSet, labelMap, 3)
	require.NoError(t, err)
	require.Equal(t, res, expected)

	labelMap[makeLabelKey(valOne)] = cache.NewLabelInfo(3, 3)
	res, _, err = createLabelArrays(seriesSet, labelMap, 3)
	require.NoError(t, err)
	expected = model.ArrayOfLabelArray{
		model.LabelArray{
			{Int32: 2, Valid: true},
			{Int32: 0, Valid: false},
			{Int32: 3, Valid: true},
		},
	}
	require.Equal(t, res, expected)

	/* test two series */
	seriesSet = []*model.Series{
		getSeries(t, scache, labels.Labels{metricNameLabel, valOne}),
		getSeries(t, scache, labels.Labels{metricNameLabel, valTwo}),
	}
	labelMap = map[cache.LabelKey]cache.LabelInfo{
		makeLabelKey(metricNameLabel): cache.NewLabelInfo(100, 1),
		makeLabelKey(valOne):          cache.NewLabelInfo(1, 5),
		makeLabelKey(valTwo):          cache.NewLabelInfo(2, 5),
	}

	res, ser, err := createLabelArrays(seriesSet, labelMap, 5)
	require.NoError(t, err)
	require.Equal(t, len(ser), 2)
	expected = model.ArrayOfLabelArray{
		model.LabelArray{
			{Int32: 100, Valid: true},
			{Int32: 0, Valid: false},
			{Int32: 0, Valid: false},
			{Int32: 0, Valid: false},
			{Int32: 1, Valid: true},
		},
		model.LabelArray{
			{Int32: 100, Valid: true},
			{Int32: 0, Valid: false},
			{Int32: 0, Valid: false},
			{Int32: 0, Valid: false},
			{Int32: 2, Valid: true},
		},
	}
	require.Equal(t, res, expected)

	/* test one series already set */
	setSeries := getSeries(t, scache, labels.Labels{metricNameLabel, valTwo})
	setSeries.SetSeriesID(5, 4)
	seriesSet = []*model.Series{
		getSeries(t, scache, labels.Labels{metricNameLabel, valOne}),
		setSeries,
	}
	labelMap = map[cache.LabelKey]cache.LabelInfo{
		makeLabelKey(metricNameLabel): cache.NewLabelInfo(100, 1),
		makeLabelKey(valOne):          cache.NewLabelInfo(1, 5),
		makeLabelKey(valTwo):          cache.NewLabelInfo(2, 5),
	}
	res, ser, err = createLabelArrays(seriesSet, labelMap, 5)
	require.NoError(t, err)
	require.Equal(t, len(ser), 1)
	expected = model.ArrayOfLabelArray{
		model.LabelArray{
			{Int32: 100, Valid: true},
			{Int32: 0, Valid: false},
			{Int32: 0, Valid: false},
			{Int32: 0, Valid: false},
			{Int32: 1, Valid: true},
		},
	}
	require.Equal(t, res, expected)
}
