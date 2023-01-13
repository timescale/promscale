// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package querier

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/timescale/promscale/pkg/pgmodel/cache"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const getExemplarLabelPositions = "SELECT * FROM _prom_catalog.get_exemplar_label_key_positions($1::TEXT)"

type exemplarSeriesRow struct {
	metricName string
	labelIds   []*int64
	data       []exemplarRow
}

type exemplarRow struct {
	time        time.Time
	value       float64
	labelValues []string // Exemplar label values.
}

// getExemplarSeriesRows returns exemplar series rows.
func getExemplarSeriesRows(metricName string, in pgxconn.PgxRows) (rows []exemplarSeriesRow, err error) {
	if in.Err() != nil {
		return rows, in.Err()
	}
	seriesRowMap := make(map[string]*exemplarSeriesRow)
	// Note: The rows of multiple exemplar query, are such that `rows` here contains different series, represented by
	// different label_ids array, that satisfy the given labels.
	for in.Next() {
		var (
			err      error
			row      exemplarRow
			labelIds []*int64
		)
		err = in.Scan(&labelIds, &row.time, &row.value, &row.labelValues)
		if err != nil {
			log.Error("err", err)
			return rows, err
		}

		key := labelIdsToKey(labelIds)
		if existingSeriesRow, exists := seriesRowMap[key]; exists {
			existingSeriesRow.data = append(existingSeriesRow.data, row)
			continue
		}
		// New exemplar series.
		seriesRow := &exemplarSeriesRow{
			metricName: metricName,
			labelIds:   labelIds,
			data:       make([]exemplarRow, 0, 1),
		}
		seriesRow.data = append(seriesRow.data, row)
		seriesRowMap[key] = seriesRow
	}
	return getExemplarSeriesSlice(seriesRowMap), in.Err()
}

func labelIdsToKey(labelIds []*int64) string {
	key := ""
	for _, v := range labelIds {
		if v == nil {
			key += "nil "
			continue
		}
		key += strconv.FormatInt(*v, 10) + " "
	}
	return key
}

func getExemplarSeriesSlice(m map[string]*exemplarSeriesRow) []exemplarSeriesRow {
	s := make([]exemplarSeriesRow, len(m))
	i := 0
	for _, v := range m {
		s[i] = *v
		i++
	}
	return s
}

// prepareExemplarQueryResult returns exemplar query result from the supplied exemplar series rows.
func prepareExemplarQueryResult(tools *queryTools, queryResult exemplarSeriesRow) (model.ExemplarQueryResult, error) {
	var (
		result   model.ExemplarQueryResult
		metric   = queryResult.metricName
		labelIds = queryResult.labelIds
	)
	index := make(map[int64]labels.Label)
	initLabelIdIndexForExemplars(index, queryResult.labelIds)

	labelsReader := tools.labelsReader
	err := labelsReader.LabelsForIdMap(index)
	if err != nil {
		return result, fmt.Errorf("fill labelIds map: %w", err)
	}
	lbls, err := getLabelsFromLabelIds(labelIds, index)
	if err != nil {
		return model.ExemplarQueryResult{}, fmt.Errorf("fetching promLabels for label-ids: %w", err)
	}
	sortedLabels := lbls
	sort.Sort(sortedLabels) // labels.Labels implements Less() function, that has < 0 comparison. This keeps __name__ label pair towards the first, providing better visibility.
	result.SeriesLabels = sortedLabels
	result.Exemplars = make([]model.ExemplarData, 0)

	keyPosCache := tools.exemplarPosCache
	keyPosIndex, err := getPositionIndex(tools, keyPosCache, metric)
	if err != nil {
		return model.ExemplarQueryResult{}, fmt.Errorf("get position index: %w", err)
	}
	// Let's create an inverse index of keyPosIndex since now we have array of exemplar label values, i.e., [key: index].
	// The index of this array needs to be used to get the key, and hence this can be done efficiently
	// by inverting the keyPosIndex to get a map of [index: key].
	keyIndex := makeInverseIndex(keyPosIndex)

	for i := range queryResult.data {
		row := queryResult.data[i]

		exemplarLabels := createPromLabelsFromExamplarLabels(keyIndex, row.labelValues)
		exemplarValue := row.value
		result.Exemplars = append(result.Exemplars, model.ExemplarData{
			Labels: exemplarLabels,
			Ts:     timestamp.FromTime(row.time),
			Value:  exemplarValue,
		})
	}
	return result, nil
}

func getPositionIndex(tools *queryTools, posCache cache.PositionCache, metric string) (map[string]int, error) {
	keyPosIndex, exists := posCache.GetLabelPositions(metric)
	if !exists {
		var index map[string]int
		if err := tools.conn.QueryRow(context.Background(), getExemplarLabelPositions, metric).Scan(&index); err != nil {
			return nil, fmt.Errorf("scanning exemplar key-position index: %w", err)
		}
		posCache.SetOrUpdateLabelPositions(metric, index)
		keyPosIndex = index
	}
	return keyPosIndex, nil
}

func initLabelIdIndexForExemplars(index map[int64]labels.Label, labelIds []*int64) {
	for _, labelId := range labelIds {
		if labelId == nil || *labelId == 0 {
			// no label to look-up for.
			continue
		}
		index[*labelId] = labels.Label{}
	}
}

func createPromLabelsFromExamplarLabels(index map[int]string, values []string) []labels.Label {
	var l []labels.Label
	for i := range values {
		value := values[i]
		if value == model.EmptyExemplarValues {
			// The value here is __promscale_no_value__, which means that it is empty.
			// Hence, skip this position and move to the next.
			continue
		}
		position := i + 1 // Values in postgres start from 1 and not 0. Hence, the same will be reflected in index. So, in order to access the right position, we need to add 1.
		key := index[position]
		l = append(l, labels.Label{Name: key, Value: value})
	}
	sort.Slice(l, func(i, j int) bool {
		return strings.Compare(l[i].Name, l[j].Name) < 0 // < 0 keeps the TraceID label pair towards the first.
	})
	return l
}

func makeInverseIndex(index map[string]int) map[int]string {
	inverseIndex := make(map[int]string, len(index))
	for k, v := range index {
		inverseIndex[v] = k
	}
	return inverseIndex
}
