package querier

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
)

const (
	exemplarsBySeriesIDsSQLFormat = `SELECT s.labels, m.time, m.value, m.exemplar_label_values
	FROM %[1]s m
	INNER JOIN %[2]s s
	ON m.series_id = s.id
	WHERE m.series_id IN (%[3]s)
	AND time >= '%[4]s'
	AND time <= '%[5]s'
	GROUP BY s.id, m.time, m.value, m.exemplar_label_values`

	exemplarByMetricSQLFormat = `SELECT series.labels, result.time, result.value, result.exemplar_label_values
	FROM %[2]s series
	INNER JOIN LATERAL (
		SELECT time, value, exemplar_label_values
		FROM %[1]s metric
		WHERE metric.series_id = series.id
		AND time >= '%[4]s'
		AND time <= '%[5]s'
		ORDER BY time
	) as result ON (result.value is not null)
	WHERE
		%[3]s`
)

func buildSingleMetricExemplarsQuery(metadata *evalMetadata) string {
	filter := metadata.timeFilter
	finalSQL := fmt.Sprintf(exemplarByMetricSQLFormat,
		pgx.Identifier{schema.PromDataExemplar, filter.metric}.Sanitize(),
		pgx.Identifier{schema.PromDataSeries, filter.metric}.Sanitize(),
		strings.Join(metadata.clauses, " AND "),
		filter.start,
		filter.end,
	)
	return finalSQL
}

func buildMultipleMetricExemplarsQuery(filter timeFilter, series []pgmodel.SeriesID) (string, error) {
	s := make([]string, len(series))
	for i, sID := range series {
		s[i] = fmt.Sprintf("%d", sID)
	}
	baseQuery := exemplarsBySeriesIDsSQLFormat
	return fmt.Sprintf(
		baseQuery,
		pgx.Identifier{schema.PromDataExemplar, filter.metric}.Sanitize(),
		pgx.Identifier{schema.PromDataSeries, filter.metric}.Sanitize(),
		strings.Join(s, ","),
		filter.start,
		filter.end,
	), nil
}
