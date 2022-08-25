// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"github.com/timescale/promscale/pkg/pgmodel/tmpcache"
	"github.com/timescale/promscale/pkg/prompb"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

// ConvertLabels converts a labels.Labels to a canonical Labels object
func ConvertLabels(ls labels.Labels) []prompb.Label {
	ll := make([]prompb.Label, len(ls))
	for i := range ls {
		ll[i].Name = ls[i].Name
		ll[i].Value = ls[i].Value
	}
	return ll
}

func getSeries(t *testing.T, scache cache.UnresolvedSeriesCache, labels labels.Labels) *model.UnresolvedSeries {
	pbLabels := ConvertLabels(labels)
	key, _, err := cache.GenerateKey(pbLabels)
	require.NoError(t, err)
	series, err := scache.GetSeries(key, pbLabels)
	require.NoError(t, err)
	return series
}

func makeLabelKey(l labels.Label) cache.LabelKey {
	return cache.LabelKey{MetricName: "metric", Name: l.Name, Value: l.Value}
}

func TestLabelArrayCreator(t *testing.T) {
	unresolvedSeriesCache := tmpcache.NewUnresolvedSeriesCache()
	metricNameLabel := labels.Label{Name: "__name__", Value: "metric"}
	valOne := labels.Label{Name: "key", Value: "one"}
	valTwo := labels.Label{Name: "key", Value: "two"}
	series := model.NewSeries(nil, getSeries(t, unresolvedSeriesCache, labels.Labels{metricNameLabel, valOne}), nil)
	seriesSet := []*model.Series{series}

	labelMap := map[cache.LabelKey]cache.LabelInfo{
		makeLabelKey(metricNameLabel): cache.NewLabelInfo(2, 1),
		makeLabelKey(valOne):          cache.NewLabelInfo(3, 2),
	}

	res, _, err := createLabelArrays(seriesSet, labelMap, 2)
	require.NoError(t, err)
	expected := [][]int32{{2, 3}}
	require.Equal(t, res, expected)

	res, _, err = createLabelArrays(seriesSet, labelMap, 3)
	require.NoError(t, err)
	expected = [][]int32{{2, 3}}
	require.Equal(t, res, expected)

	labelMap[makeLabelKey(valOne)] = cache.NewLabelInfo(3, 3)
	res, _, err = createLabelArrays(seriesSet, labelMap, 3)
	require.NoError(t, err)
	expected = [][]int32{{2, 0, 3}}
	require.Equal(t, res, expected)

	/* test two series */
	seriesSet = []*model.Series{
		model.NewSeries(nil, getSeries(t, unresolvedSeriesCache, labels.Labels{metricNameLabel, valOne}), nil),
		model.NewSeries(nil, getSeries(t, unresolvedSeriesCache, labels.Labels{metricNameLabel, valTwo}), nil),
	}
	labelMap = map[cache.LabelKey]cache.LabelInfo{
		makeLabelKey(metricNameLabel): cache.NewLabelInfo(100, 1),
		makeLabelKey(valOne):          cache.NewLabelInfo(1, 5),
		makeLabelKey(valTwo):          cache.NewLabelInfo(2, 5),
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
	//setSeries := getSeries(t, scache, labels.Labels{metricNameLabel, valTwo})
	//setSeries.SetSeriesID(5, model.NewSeriesEpoch(time.Now().Unix()))
	//seriesSet = []*model.Series{
	//	getSeries(t, scache, labels.Labels{metricNameLabel, valOne}),
	//	setSeries,
	//}
	//labelMap = map[cache.LabelKey]cache.LabelInfo{
	//	makeLabelKey(metricNameLabel): cache.NewLabelInfo(100, 1),
	//	makeLabelKey(valOne):          cache.NewLabelInfo(1, 5),
	//	makeLabelKey(valTwo):          cache.NewLabelInfo(2, 5),
	//}
	//res, ser, err = createLabelArrays(seriesSet, labelMap, 5)
	//require.NoError(t, err)
	//require.Equal(t, len(ser), 1)
	//expected = [][]int32{{100, 0, 0, 0, 1}}
	//require.Equal(t, res, expected)
}
