package querier

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
)

const (
	timeseriesBySeriesIDsSQLFormat = `SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)
	FROM %[1]s m
	INNER JOIN %[2]s s
	ON m.series_id = s.id
	WHERE m.series_id IN (%[3]s)
	AND time >= '%[4]s'
	AND time <= '%[5]s'
	GROUP BY s.id`

	/* SINGLE METRIC PATH (common, performance critical case) */
	/* The simpler query (which isn't used):
			SELECT s.labels, array_agg(m.time ORDER BY time) as time_array, array_agg(m.value ORDER BY time)
			FROM
				"prom_data"."demo_api_request_duration_seconds_bucket" m
			INNER JOIN
				"prom_data_series"."demo_api_request_duration_seconds_bucket" s ON m.series_id = s.id
			WHERE
				labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = '__name__' and l.value = 'demo_api_request_duration_seconds_bucket')
				AND  labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = 'job' and l.value = 'demo')
				AND time >= '2020-08-10 10:34:56.828+00'
				AND time <= '2020-08-10 11:39:11.828+00'
			GROUP BY s.id;

			Is not used because it has performance issues:
			1) If the series are scanned using the gin index, then the nested loop is not ordered by s.id. That means
				the scan coming out of the metric table needs to be sorted by s.id with a sort node.
			2) In any case, the array_agg have to sort things explicitly by time, wasting the series_id, time column index on the metric table
				and incurring sort overhead.

	Instead we use the following query, which avoids both the sorts above:
			SELECT
				s.labels, result.time_array, result.value_array
			FROM
				"prom_data_series"."demo_api_request_duration_seconds_bucket" s
			INNER JOIN LATERAL
			(
				SELECT array_agg(time) as time_array, array_agg(value) as value_array
				FROM
				(
					SELECT time, value
					FROM
						"prom_data"."demo_api_request_duration_seconds_bucket" m
					WHERE
						m.series_id = s.id
						AND time >= '2020-08-10 10:34:56.828+00'
						AND time <= '2020-08-10 11:39:11.828+00'
					ORDER BY time
				) as rows
			) as result  ON (result.time_array is not null)
			WHERE
				labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = 'job' and l.value = 'demo');
	*/
	timeseriesByMetricSQLFormat = `SELECT series.labels,  %[7]s
	FROM %[2]s series
	INNER JOIN LATERAL (
		SELECT %[6]s
		FROM
		(
			SELECT time, value
			FROM %[1]s metric
			WHERE metric.series_id = series.id
			AND time >= '%[4]s'
			AND time <= '%[5]s'
			%[8]s
		) as time_ordered_rows
	) as result ON (result.value_array is not null)
	WHERE
	     %[3]s`

	/* optimized for no clauses besides __name__
	   uses a inner join without a lateral to allow for better parallel execution
	*/
	timeseriesByMetricSQLFormatNoClauses = `SELECT series.labels,  %[7]s
	FROM %[2]s series
	INNER JOIN (
		SELECT series_id, %[6]s
		FROM
		(
			SELECT series_id, time, value
			FROM %[1]s metric
			WHERE
			time >= '%[4]s'
			AND time <= '%[5]s'
			%[8]s
		) as time_ordered_rows
		GROUP BY series_id
	) as result ON (result.value_array is not null AND result.series_id = series.id)`

	defaultColumnName = "value"
	exemplarFormat = "result.exemplar_label_values"
)

func buildSingleMetricSamplesQuery(metadata *evalMetadata) (string, []interface{}, parser.Node, TimestampSeries, error) {
	// Aggregators are not in exemplar queries. In sample query, we have aggregations since they are
	// to serve promql evaluations. But, exemplar queries are fetch-only queries. Their responses are not meant to be
	// evaluated by any PromQL function.
	qf, node, err := getAggregators(metadata.promqlMetadata)
	if err != nil {
		return "", nil, nil, nil, err
	}

	var (
		selectors, selectorClauses []string
		values                     []interface{}
	)

	if qf.timeClause != "" {
		var timeClauseBound string
		timeClauseBound, values, err = setParameterNumbers(qf.timeClause, metadata.values, qf.timeParams...)
		if err != nil {
			return "", nil, nil, nil, err
		}
		selectors = append(selectors, "result.time_array")
		selectorClauses = append(selectorClauses, timeClauseBound+" as time_array")
	}
	valueClauseBound, values, err := setParameterNumbers(qf.valueClause, values, qf.valueParams...)
	if err != nil {
		return "", nil, nil, nil, err
	}
	selectors = append(selectors, "result.value_array")
	selectorClauses = append(selectorClauses, valueClauseBound+" as value_array")

	orderByClause := "ORDER BY time"
	if qf.unOrdered {
		orderByClause = ""
	}
	template := timeseriesByMetricSQLFormat
	cases := metadata.clauses
	if len(cases) == 1 && cases[0] == "TRUE" {
		template = timeseriesByMetricSQLFormatNoClauses
		if !qf.unOrdered {
			orderByClause = "ORDER BY series_id, time"
		}
	}
	tFilter := metadata.timeFilter
	finalSQL := fmt.Sprintf(template,
		pgx.Identifier{schema.Data, tFilter.metric}.Sanitize(),
		pgx.Identifier{schema.DataSeries, tFilter.metric}.Sanitize(),
		strings.Join(cases, " AND "),
		tFilter.start,
		tFilter.end,
		strings.Join(selectorClauses, ", "),
		strings.Join(selectors, ", "),
		orderByClause,
	)
	return finalSQL, values, node, qf.tsSeries, nil
}

func buildMultipleMetricSamplesQuery(filter timeFilter, series []pgmodel.SeriesID) (string, error) {
	s := make([]string, len(series))
	for i, sID := range series {
		s[i] = fmt.Sprintf("%d", sID)
	}
	return fmt.Sprintf(
		timeseriesBySeriesIDsSQLFormat,
		pgx.Identifier{filter.schema, filter.metric}.Sanitize(),
		pgx.Identifier{schema.DataSeries, filter.seriesTable}.Sanitize(),
		strings.Join(s, ","),
		filter.startTime,
		filter.endTime,
	)
}

/* MULTIPLE METRIC PATH (less common case) */
/* The following two sql statements are for queries where the metric name is unknown in the query. The first query gets the
* metric name and series_id array and the second queries individual metrics while passing down the array */
const metricNameSeriesIDSQLFormat = `SELECT m.metric_name, array_agg(s.id) FROM _prom_catalog.series s
INNER JOIN _prom_catalog.metric m ON (m.id = s.metric_id)
WHERE %s GROUP BY m.metric_name ORDER BY m.metric_name`

func buildMetricNameSeriesIDQuery(cases []string) string {
	return fmt.Sprintf(metricNameSeriesIDSQLFormat, strings.Join(cases, " AND "))
}