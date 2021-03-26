// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package delete

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const queryDeleteSeries = "SELECT _prom_catalog.delete_series_from_metric($1, $2)"

// PgDelete deletes the series based on matchers.
type PgDelete struct {
	Conn pgxconn.PgxConn
}

// DeleteSeries deletes the series that matches the provided label_matchers.
func (pgDel *PgDelete) DeleteSeries(matchers []*labels.Matcher, _, _ time.Time) ([]string, []model.SeriesID, int, error) {
	var (
		deletedSeriesIDs []model.SeriesID
		totalRowsDeleted int
		err              error
		metricsTouched   = make(map[string]struct{})
	)
	metricNames, seriesIDMatrix, err := pgDel.getMetricNameSeriesIDFromMatchers(matchers)
	if err != nil {
		return nil, nil, -1, fmt.Errorf("delete-series: %w", err)
	}
	for metricIndex, metricName := range metricNames {
		seriesIDs := seriesIDMatrix[metricIndex]
		var rowsDeleted int
		if err = pgDel.Conn.QueryRow(
			context.Background(),
			queryDeleteSeries,
			metricName,
			convertSeriesIDsToInt64s(seriesIDs),
		).Scan(&rowsDeleted); err != nil {
			return getKeys(metricsTouched), deletedSeriesIDs, totalRowsDeleted, fmt.Errorf("deleting series with metric_name=%s and series_ids=%v : %w", metricName, seriesIDs, err)
		}
		if _, ok := metricsTouched[metricName]; !ok {
			metricsTouched[metricName] = struct{}{}
		}
		deletedSeriesIDs = append(deletedSeriesIDs, seriesIDs...)
		totalRowsDeleted += rowsDeleted
	}
	return getKeys(metricsTouched), deletedSeriesIDs, totalRowsDeleted, nil
}

// getMetricNameSeriesIDFromMatchers returns the metric name list and the corresponding series ID array
// as a matrix.
func (pgDel *PgDelete) getMetricNameSeriesIDFromMatchers(matchers []*labels.Matcher) ([]string, [][]model.SeriesID, error) {
	cb, err := querier.BuildSubQueries(matchers)
	if err != nil {
		return nil, nil, fmt.Errorf("delete series build subqueries: %w", err)
	}
	clauses, values, err := cb.Build(true)
	if err != nil {
		return nil, nil, fmt.Errorf("delete series build clauses: %w", err)
	}
	query := querier.BuildMetricNameSeriesIDQuery(clauses)
	rows, err := pgDel.Conn.Query(context.Background(), query, values...)
	if err != nil {
		return nil, nil, fmt.Errorf("build metric name series: %w", err)
	}
	metricNames, correspondingSeriesIDs, err := querier.GetSeriesPerMetric(rows)
	if err != nil {
		return nil, nil, fmt.Errorf("series per metric: %w", err)
	}
	return metricNames, correspondingSeriesIDs, nil
}

func convertSeriesIDsToInt64s(s []model.SeriesID) []int64 {
	temp := make([]int64, len(s))
	for i := range s {
		temp[i] = int64(s[i])
	}
	return temp
}

func getKeys(mapStr map[string]struct{}) (keys []string) {
	if mapStr == nil {
		return nil
	}
	keys = make([]string, 0, len(mapStr))
	for k := range mapStr {
		keys = append(keys, k)
	}
	return
}
