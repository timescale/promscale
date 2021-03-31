// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

func getSeries(t *testing.T, scache *cache.SeriesCacheImpl, labels labels.Labels) *model.Series {
	series, _, err := scache.GetSeriesFromLabels(labels)
	require.NoError(t, err)
	return series
}

func TestLabelArrayCreator(t *testing.T) {
	scache := cache.NewSeriesCache(cache.DefaultConfig, nil)
	metricNameLabel := labels.Label{Name: "__name__", Value: "metric"}
	valOne := labels.Label{Name: "key", Value: "one"}
	valTwo := labels.Label{Name: "key", Value: "two"}
	seriesSet := []*model.Series{
		getSeries(t, scache, labels.Labels{metricNameLabel, valOne}),
	}
	labelMap := map[labels.Label]labelInfo{
		metricNameLabel: {2, 1},
		valOne:          {3, 2},
	}

	res, _, err := createLabelArrays(seriesSet, labelMap, 2)
	require.NoError(t, err)
	expected := [][]int32{{2, 3}}
	require.Equal(t, res, expected)

	res, _, err = createLabelArrays(seriesSet, labelMap, 3)
	require.NoError(t, err)
	expected = [][]int32{{2, 3}}
	require.Equal(t, res, expected)

	labelMap[valOne] = labelInfo{3, 3}
	res, _, err = createLabelArrays(seriesSet, labelMap, 3)
	require.NoError(t, err)
	expected = [][]int32{{2, 0, 3}}
	require.Equal(t, res, expected)

	/* test two series */
	seriesSet = []*model.Series{
		getSeries(t, scache, labels.Labels{metricNameLabel, valOne}),
		getSeries(t, scache, labels.Labels{metricNameLabel, valTwo}),
	}
	labelMap = map[labels.Label]labelInfo{
		metricNameLabel: {100, 1},
		valOne:          {1, 5},
		valTwo:          {2, 5},
	}

	res, ser, err := createLabelArrays(seriesSet, labelMap, 5)
	require.NoError(t, err)
	require.Equal(t, len(ser), 2)
	expected = [][]int32{
		{100, 0, 0, 0, 1},
		{100, 0, 0, 0, 2},
	}
	require.Equal(t, res, expected)

	/* test one series already set */
	setSeries := getSeries(t, scache, labels.Labels{metricNameLabel, valTwo})
	setSeries.SetSeriesID(5, 4)
	seriesSet = []*model.Series{
		getSeries(t, scache, labels.Labels{metricNameLabel, valOne}),
		setSeries,
	}
	labelMap = map[labels.Label]labelInfo{
		metricNameLabel: {100, 1},
		valOne:          {1, 5},
		valTwo:          {2, 5},
	}
	res, ser, err = createLabelArrays(seriesSet, labelMap, 5)
	require.NoError(t, err)
	require.Equal(t, len(ser), 1)
	expected = [][]int32{{100, 0, 0, 0, 1}}
	require.Equal(t, res, expected)
}
