// todo: header files.

package querier

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

const getExemplarLabelPositions = "SELECT * FROM " + schema.Catalog + ".get_exemplar_label_key_positions($1::TEXT)"

type exemplarSeriesRow struct {
	metricName string
	labelIds   []int64
	data       []exemplarRow
}

type exemplarRow struct {
	time        time.Time
	value       float64
	labelValues []string // Exemplar label values.
}

// appendExemplarRows adds new results rows to already existing result rows and
// returns the as a result.
func appendExemplarRows(metricName string, in pgx.Rows) (rows []exemplarSeriesRow, err error) {
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
			labelIds []int64
		)
		err = in.Scan(&labelIds, &row.time, &row.value, &row.labelValues)
		if err != nil {
			log.Error("err", err)
			return rows, err
		}

		key := fmt.Sprintf("%v", labelIds)
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
	return getExemplarSeriesRows(seriesRowMap), in.Err()
}

func getExemplarSeriesRows(m map[string]*exemplarSeriesRow) []exemplarSeriesRow {
	s := make([]exemplarSeriesRow, len(m))
	i := 0
	for _, v := range m {
		s[i] = *v
		i++
	}
	return s
}

// prepareExemplarQueryResult extracts the data from fetched pgx.Rows (or set of pgx.Rows
// in case returned selectors are multiple)
// Its more efficient to directly prepare results from rows than making an iterator
// since if you want to make iterator, then you first need to scan and keep all results
// in array of type A, and then use the cache (exemplar label values cache and labels cache)
// to convert it to type B.
func prepareExemplarQueryResult(conn pgxconn.PgxConn, lr lreader.LabelsReader, exemplarKeyPos cache.PositionCache, queryResult exemplarSeriesRow) (model.ExemplarQueryResult, error) {
	var (
		result   model.ExemplarQueryResult
		metric   = queryResult.metricName
		labelIds = queryResult.labelIds
	)
	promLabels, err := lr.PrompbLabelsForIds(labelIds)
	if err != nil {
		return model.ExemplarQueryResult{}, fmt.Errorf("fetching promLabels for label-ids: %w", err)
	}
	result.SeriesLabels = getLabels(promLabels)
	result.Exemplars = make([]model.ExemplarData, 0)

	keyPosIndex, exists := exemplarKeyPos.GetLabelPositions(metric)
	if !exists {
		var index map[string]int
		if err := conn.QueryRow(context.Background(), getExemplarLabelPositions, metric).Scan(&index); err != nil {
			return model.ExemplarQueryResult{}, fmt.Errorf("scanning exemplar key-position index: %w", err)
		}
		exemplarKeyPos.SetorUpdateLabelPositions(metric, index)
		keyPosIndex = index
	}
	// Let's create an inverse index of keyPosIndex since now we have array of exemplar label values, i.e., [key: index].
	// The index of this array needs to be used to get the key, and hence this can be done efficiently
	// by inverting the keyPosIndex to get a map of [index: key].
	keyIndex := makeInverseIndex(keyPosIndex)

	for i := range queryResult.data {
		row := queryResult.data[i]

		exemplarLabels := createPromLabels(keyIndex, row.labelValues)
		exemplarValue := row.value
		exemplarTs := float64(timestamp.FromTime(row.time)) / 1000 // Divide by 1000 to get in seconds. Keep the millisecond part after the decimal to be in compliant with Prometheus behaviour.
		result.Exemplars = append(result.Exemplars, model.ExemplarData{
			Labels: exemplarLabels,
			Ts:     exemplarTs,
			Value:  exemplarValue,
		})
	}
	return result, nil
}

func createPromLabels(index map[int]string, values []string) []labels.Label {
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
	return l
}

func makeInverseIndex(index map[string]int) map[int]string {
	inverseIndex := make(map[int]string, len(index))
	for k, v := range index {
		inverseIndex[v] = k
	}
	return inverseIndex
}

func getLabels(pLbls []prompb.Label) []labels.Label {
	lbls := make([]labels.Label, len(pLbls))
	for i := range pLbls {
		lbls[i].Name = pLbls[i].Name
		lbls[i].Value = pLbls[i].Value
	}
	return lbls
}
