// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package querier

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/util"
)

func TestPrepareExemplarQueryResult(t *testing.T) {
	exemplarRows := []exemplarRow{
		{
			time: time.Unix(0, 0), value: 1, labelValues: []string{model.EmptyExemplarValues, "test", "generator"},
		},
		{
			time: time.Unix(1, 0), value: 2, labelValues: []string{"some_trace_id", "test", model.EmptyExemplarValues},
		},
		{
			time: time.Unix(2, 0), value: 3, labelValues: []string{model.EmptyExemplarValues, model.EmptyExemplarValues, model.EmptyExemplarValues},
		},
	}
	seriesRow := exemplarSeriesRow{
		metricName: "test_metric_exemplar",
		labelIds:   []*int64{util.Pointer(int64(1)), util.Pointer(int64(3))},
		data:       exemplarRows,
	}
	lrCache := newMockLabelsReader([]int64{1, 3}, []labels.Label{{Name: "__name__", Value: "test_metric_exemplar"}, {Name: "instance", Value: "localhost:9100"}})

	conn := model.NewSqlRecorder([]model.SqlQuery{}, t)
	exemplarCache := cache.NewExemplarLabelsPosCache(cache.Config{ExemplarKeyPosCacheSize: 3})
	exemplarCache.SetOrUpdateLabelPositions("test_metric_exemplar", getExemplarPosIndices())

	tools := &queryTools{
		conn:             conn,
		labelsReader:     lrCache,
		exemplarPosCache: exemplarCache,
	}
	result, err := prepareExemplarQueryResult(tools, seriesRow)
	require.NoError(t, err)

	bSlice, err := json.Marshal(result)
	require.NoError(t, err)
	require.Equal(t,
		`{"seriesLabels":{"__name__":"test_metric_exemplar","instance":"localhost:9100"},"exemplars":[{"labels":{"component":"test","job":"generator"},"value":1,"timestamp":0},{"labels":{"TraceID":"some_trace_id","component":"test"},"value":2,"timestamp":1000},{"labels":{},"value":3,"timestamp":2000}]}`,
		string(bSlice),
	)
}

func getExemplarPosIndices() map[string]int {
	return map[string]int{
		"TraceID":   1,
		"component": 2,
		"job":       3,
	}
}

type mockLabelsReader struct {
	items map[int64]labels.Label
}

func newMockLabelsReader(keys []int64, lbls []labels.Label) mockLabelsReader {
	if len(keys) != len(lbls) {
		panic("keys length and labels length must be same")
	}
	items := make(map[int64]labels.Label)
	l := len(keys)
	for i := 0; i < l; i++ {
		items[keys[i]] = lbls[i]
	}
	return mockLabelsReader{items}
}

func (m mockLabelsReader) LabelNames() ([]string, error) {
	return nil, nil
}

// LabelValues returns all the distinct values for a given label name.
func (m mockLabelsReader) LabelValues(_ string) ([]string, error) {
	return nil, nil
}

func (m mockLabelsReader) LabelsForIdMap(index map[int64]labels.Label) error {
	for seriesId := range index {
		if lbls, present := m.items[seriesId]; present {
			index[seriesId] = lbls
		}
	}
	return nil
}

// LabelsForIds returns label names and values for the supplied IDs.
func (m mockLabelsReader) LabelsForIds(ids []int64) (lls labels.Labels, err error) {
	return nil, nil
}
