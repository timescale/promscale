package pgmodel

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	queryLabelID           = "select id from _prom_catalog.label where key=$1 and value=$2"
	querySeriesAndMetricID = "select id, metric_id from _prom_catalog.series where labels=$1"
	queryMetricName        = "select metric_name from _prom_catalog.metric where id=$1"
	queryDeleteSeries      = "select _prom_catalog.delete_series_from_metric($1, $2)"
)

type DBDeleter struct {
	conn *pgxpool.Pool
}

// NewPgDeleter returns a series deleter that is dependent on the provided label set.
func NewPgxDeleter(conn *pgxpool.Pool) *DBDeleter {
	return &DBDeleter{conn}
}

// PgDelete
type PgDelete struct {
	conn *pgxpool.Pool
}

// PgDeleter returns a new PgDelete that can be used to delete series. It is goroutine safe.
func (pgDelr *DBDeleter) PgDeleter() *PgDelete {
	return &PgDelete{pgDelr.conn}
}

// getMetricNameSeriesIDFromMatchers returns the metric name list and the corresponding series ID array
// as a matrix.
func (pgDel *PgDelete) getMetricNameSeriesIDFromMatchers(matchers []*labels.Matcher) ([]string, [][]SeriesID, error) {
	_, clauses, values, err := buildSubQueries(matchers)
	if err != nil {
		return nil, nil, fmt.Errorf("delete series from matchers: %w", err)
	}
	query := buildMetricNameSeriesIDQuery(clauses)
	rows, err := pgDel.conn.Query(context.Background(), query, values...)
	if err != nil {
		return nil, nil, fmt.Errorf("build metric name series: %w", err)
	}
	metricNames, correspondingSeriesIDs, err := getSeriesPerMetric(rows)
	if err != nil {
		return nil, nil, fmt.Errorf("series per metric: %w", err)
	}
	return metricNames, correspondingSeriesIDs, nil
}

// DeleteSeries deletes the series that matches the provided label_matchers.
func (pgDel *PgDelete) DeleteSeries(matchers []*labels.Matcher) ([]string, []SeriesID, int, error) {
	var (
		deletedSeriesIDs   []SeriesID
		totalChunksTouched int
		err                error
		metricsTouched     = make(map[string]struct{})
	)
	metricNames, seriesIDMatrix, err := pgDel.getMetricNameSeriesIDFromMatchers(matchers)
	if err != nil {
		return nil, nil, -1, fmt.Errorf("delete-series: %w", err)
	}
	for metricIndex, metricName := range metricNames {
		seriesIDs := seriesIDMatrix[metricIndex]
		for _, seriesID := range seriesIDs {
			var chunksTouched int
			if err = pgDel.conn.QueryRow(
				context.Background(),
				queryDeleteSeries,
				seriesID,
				fmt.Sprintf("prom_data.%s", metricName),
			).Scan(&chunksTouched); err != nil {
				return getKeys(metricsTouched), deletedSeriesIDs, totalChunksTouched, fmt.Errorf("deleting series with metric_name=%s and series_id=%d : %w", metricName, seriesID, err)
			}
			if _, ok := metricsTouched[metricName]; !ok {
				metricsTouched[metricName] = struct{}{}
			}
			deletedSeriesIDs = append(deletedSeriesIDs, seriesID)
			totalChunksTouched += chunksTouched
		}
	}
	return getKeys(metricsTouched), deletedSeriesIDs, totalChunksTouched, nil
}

func getKeys(mapStr map[string]struct{}) (keys []string) {
	if mapStr == nil {
		return nil
	}
	for k := range mapStr {
		keys = append(keys, k)
	}
	return
}
