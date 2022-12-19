package querier

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
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
	timeseriesByMetricSQLFormat = `SELECT series.labels, %[7]s
	FROM %[2]s series
	INNER JOIN LATERAL (
		SELECT %[6]s
		FROM
		(
			SELECT time, %[9]s as value
			FROM %[1]s metric
			WHERE metric.series_id = series.id
			AND time >= '%[4]s'
			AND time <= '%[5]s'
			%[8]s
		) as time_ordered_rows
	) as result ON (result.value_array is not null)
	WHERE
	     %[3]s`

	// This is optimized for no clauses besides __name__, uses an inner join
	// without a lateral to allow for better parallel execution.
	timeseriesByMetricSQLFormatNoClauses = `SELECT series.labels, %[7]s
	FROM %[2]s series
	INNER JOIN (
		SELECT series_id, %[6]s
		FROM
		(
			SELECT series_id, time, %[9]s as value
			FROM %[1]s metric
			WHERE
			time >= '%[4]s'
			AND time <= '%[5]s'
			%[8]s
		) as time_ordered_rows
		GROUP BY series_id
	) as result ON (result.value_array is not null AND result.series_id = series.id)`

	defaultColumnName = "value"
)

// buildSingleMetricSamplesQuery builds a SQL query which fetches the data for
// one metric.
func buildSingleMetricSamplesQuery(metadata *evalMetadata) (string, []interface{}, parser.Node, TimestampSeries, error) {
	// The basic structure of the SQL query which this function produces is:
	//		SELECT
	//		  series.labels
	//		, <array_aggregator>(metric.value ORDER BY time) as value_array
	//		[, <array_aggregator>(metric.time ORDER BY time) as time_array] (optional)
	//		FROM
	//			<metric name> metric
	//		INNER JOIN
	//			<series name> series ON metric.series_id = series.id
	//		WHERE
	//			<some WHERE clauses>
	//		GROUP BY series.id;
	//
	// The <array_aggregator> produces an array of values, so each result row
	// consists of an array of labels, an array of values, and optionally an
	// array of timestamps.
	//
	// In the absence of available pushdowns, the <array_aggregator> is the
	// `array_agg` Postgres function, and the `time_array` result set is
	// returned.
	// When pushdowns are available, the <array_aggregator> is a pushdown
	// function which the promscale extension provides.

	qf, node := getAggregators(metadata.promqlMetadata)

	var selectors, selectorClauses []string
	values := metadata.values

	if qf.timeClause != "" {
		var timeClauseBound string
		var err error
		timeClauseBound, values, err = setParameterNumbers(qf.timeClause, values, qf.timeParams...)
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

	// On query time ranges in Prometheus:
	//
	// When we receive a range query the [`result_start`, `result_end`] range
	// specifies the timespan that we are expected to deliver results for.
	// Similarly, when we receive an instant query, the `time` parameter
	// specifies the instant  that we are expected to deliver a result for.
	//
	// Depending on the query, we may need to look further back in time than
	// the `result_start` to deliver results from `result_start` onwards. For
	// instance, a range query such as `rate(metric_one[5m])` in the range
	// `(T1, T2)` will deliver results between T1 and T2, but it has a 5-minute
	// range on `metric_one`. In order to deliver a result for time T1, we need
	// to get `metric_one`'s values at (T1 - 5m) and T1. Similarly, an instant
	// query for `metric_one` at timestamp T3 requires looking back the
	// lookback time (the default is 5 minutes) to find the most recent samples.
	// We call this point in time `scan_start`: the point in time at which we
	// need to start scanning the underlying data to calculate the result. This
	// could look as follows:
	//
	//     scan_start   result_start         result_end
	//         |-------------|--------------------|
	//
	// How do the following time ranges relate to scan_start, result_start, and
	// result_end:
	// - metadata.timeFilter
	// - metadata.selectHints.{Start, End}
	//
	// The timeFilter's time range is determined by `findMinMaxTime` on the
	// expression being evaluated, and is the widest span of _all_
	// subexpressions (effectively [min(scan_start), result_end]).
	// This means that, for instance, if the following range selector and
	// instant selector are combined: rate(metric_one[1m]) / metric_one, the
	// timeFilter.start is T1 - 5m (assuming default lookback time of 5m).
	// Note: This is problematic because for metric_one[1m] we actually want to
	// query over [T1 - 1m, end], not [T1 - 5m, end].
	//
	// The selectHints' time range is calculated by `getTimeRangesForSelector`,
	// which determines the correct `scan_start` for the current expression.

	filter := metadata.timeFilter
	sh := metadata.selectHints
	var start, end string
	// selectHints are non-nil when the query was initiated through the `query`
	// or `query_range` endpoints. They are nil when the query was initiated
	// through the `read` (remote read) endpoint.
	if sh != nil {
		start, end = toRFC3339Nano(sh.Start), toRFC3339Nano(sh.End)
	} else {
		start, end = metadata.timeFilter.start, metadata.timeFilter.end
	}

	finalSQL := fmt.Sprintf(template,
		pgx.Identifier{filter.schema, filter.metric}.Sanitize(),
		pgx.Identifier{schema.PromDataSeries, filter.seriesTable}.Sanitize(),
		strings.Join(cases, " AND "),
		start,
		end,
		strings.Join(selectorClauses, ", "),
		strings.Join(selectors, ", "),
		orderByClause,
		pgx.Identifier{filter.column}.Sanitize(),
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
		pgx.Identifier{schema.PromDataSeries, filter.seriesTable}.Sanitize(),
		strings.Join(s, ","),
		filter.start,
		filter.end,
	), nil
}

/* MULTIPLE METRIC PATH (less common case) */
/* The following two sql statements are for queries where the metric name is unknown in the query. The first query gets the
* metric name and series_id array and the second queries individual metrics while passing down the array */
const metricNameSeriesIDSQLFormat = `SELECT m.table_schema, m.metric_name, array_agg(s.id)
FROM _prom_catalog.series s
INNER JOIN _prom_catalog.metric m
ON (m.id = s.metric_id)
WHERE %s
GROUP BY m.metric_name, m.table_schema
ORDER BY m.metric_name, m.table_schema`

func buildMetricNameSeriesIDQuery(cases []string) string {
	return fmt.Sprintf(metricNameSeriesIDSQLFormat, strings.Join(cases, " AND "))
}
