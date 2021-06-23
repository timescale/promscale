package querier

import (
	"context"
	"fmt"

	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	// "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	metricsContainingExemplars = "SELECT unnest($1::TEXT[]) INTERSECT SELECT metric_name FROM " + schema.Catalog + ".exemplar"
)

// func (q *pgxQuerier) getExemplars(mint, maxt int64, metrics []string, seriesIDs [][]model.SeriesID) error {
// 	ingestedExemplarMetrics, err := getMetricsContainingExemplars(q.conn, metrics)
// 	if err != nil {
// 		return fmt.Errorf("get metrics containing exemplars: %w", err)
// 	}

// }

func getMetricsContainingExemplars(conn pgxconn.PgxConn, metrics []string) ([]string, error) {
	var exemplarMetrics []string
	rows, err := conn.Query(context.Background(), metricsContainingExemplars, metrics)
	if err != nil {
		return nil, fmt.Errorf("error querying ingested exemplar metrics: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var metric string
		err := rows.Scan(&metric)
		if err != nil {
			return nil, fmt.Errorf("error querying valid exemplar metrics: %w", err)
		}
		exemplarMetrics = append(exemplarMetrics, metric)
	}
	return exemplarMetrics, nil
}
