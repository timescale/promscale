package pgmodel

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/prometheus/pkg/labels"
)

const queryDeleteSeries = "SELECT _prom_catalog.delete_series_from_metric($1, $2)"

var (
	ErrTimeBasedDeletion = fmt.Errorf("time based series deletion is unsupported")
	MinTimeProm          = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	MaxTimeProm          = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()
)

// PgDelete deletes the series based on matchers.
type PgDelete struct {
	Conn *pgxpool.Pool
}

// DeleteSeries deletes the series that matches the provided label_matchers.
func (pgDel *PgDelete) DeleteSeries(matchers []*labels.Matcher, _, _ time.Time) ([]string, []SeriesID, int, error) {
	var (
		deletedSeriesIDs []SeriesID
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
func (pgDel *PgDelete) getMetricNameSeriesIDFromMatchers(matchers []*labels.Matcher) ([]string, [][]SeriesID, error) {
	_, clauses, values, err := buildSubQueries(matchers)
	if err != nil {
		return nil, nil, fmt.Errorf("delete series from matchers: %w", err)
	}
	query := buildMetricNameSeriesIDQuery(clauses)
	rows, err := pgDel.Conn.Query(context.Background(), query, values...)
	if err != nil {
		return nil, nil, fmt.Errorf("build metric name series: %w", err)
	}
	metricNames, correspondingSeriesIDs, err := getSeriesPerMetric(rows)
	if err != nil {
		return nil, nil, fmt.Errorf("series per metric: %w", err)
	}
	return metricNames, correspondingSeriesIDs, nil
}

func convertSeriesIDsToInt64s(s []SeriesID) []int64 {
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
