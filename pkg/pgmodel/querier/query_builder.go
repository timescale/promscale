// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package querier

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

const (
	subQueryEQ            = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value = $%d)"
	subQueryEQMatchEmpty  = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value != $%d)"
	subQueryNEQ           = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value != $%d)"
	subQueryNEQMatchEmpty = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value = $%d)"
	subQueryRE            = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value ~ $%d)"
	subQueryREMatchEmpty  = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value !~ $%d)"
	subQueryNRE           = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value !~ $%d)"
	subQueryNREMatchEmpty = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value ~ $%d)"

	/* MULTIPLE METRIC PATH (less common case) */
	/* The following two sql statements are for queries where the metric name is unknown in the query. The first query gets the
	* metric name and series_id array and the second queries individual metrics while passing down the array */
	metricNameSeriesIDSQLFormat = `SELECT m.metric_name, array_agg(s.id)
	FROM _prom_catalog.series s
	INNER JOIN _prom_catalog.metric m
	ON (m.id = s.metric_id)
	WHERE %s
	GROUP BY m.metric_name
	ORDER BY m.metric_name`

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
	Future optimizations:
	  - different query if scanning entire metric (not labels matchers besides __name__)
	*/
	timeseriesByMetricSQLFormat = `SELECT series.labels,  result.time_array, result.value_array
	FROM %[2]s series
	INNER JOIN LATERAL (
		SELECT %[6]s as time_array, %[7]s as value_array
		FROM
		(
			SELECT time, value
			FROM %[1]s metric
			WHERE metric.series_id = series.id
			AND time >= '%[4]s'
			AND time <= '%[5]s'
			ORDER BY time
		) as time_ordered_rows
	) as result ON (result.value_array is not null)
	WHERE
	     %[3]s`
)

var (
	minTime = timestamp.FromTime(time.Unix(math.MinInt64/1000+62135596801, 0).UTC())
	maxTime = timestamp.FromTime(time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC())
)

func BuildSubQueries(matchers []*labels.Matcher) (*clauseBuilder, error) {
	var err error
	cb := &clauseBuilder{}

	for _, m := range matchers {
		// From the PromQL docs: "Label matchers that match
		// empty label values also select all time series that
		// do not have the specific label set at all."
		matchesEmpty := m.Matches("")

		switch m.Type {
		case labels.MatchEqual:
			if m.Name == pgmodel.MetricNameLabelName {
				cb.SetMetricName(m.Value)
				continue
			}
			sq := subQueryEQ
			if matchesEmpty {
				sq = subQueryEQMatchEmpty
			}
			err = cb.addClause(sq, m.Name, m.Value)
		case labels.MatchNotEqual:
			sq := subQueryNEQ
			if matchesEmpty {
				sq = subQueryNEQMatchEmpty
			}
			err = cb.addClause(sq, m.Name, m.Value)
		case labels.MatchRegexp:
			sq := subQueryRE
			if matchesEmpty {
				sq = subQueryREMatchEmpty
			}
			err = cb.addClause(sq, m.Name, anchorValue(m.Value))
		case labels.MatchNotRegexp:
			sq := subQueryNRE
			if matchesEmpty {
				sq = subQueryNREMatchEmpty
			}
			err = cb.addClause(sq, m.Name, anchorValue(m.Value))
		}

		if err != nil {
			return nil, err
		}
	}

	return cb, err
}

/* Given a clause with %d placeholder for parameter numbers, and the existing and new parameters, return a clause with the parameters set to the appropriate $index and the full set of parameter values */
func setParameterNumbers(clause string, existingArgs []interface{}, newArgs ...interface{}) (string, []interface{}, error) {
	argIndex := len(existingArgs) + 1
	argCountInClause := strings.Count(clause, "%d")

	if argCountInClause != len(newArgs) {
		return "", nil, fmt.Errorf("invalid number of args: in sql %d vs args %d", argCountInClause, len(newArgs))
	}

	argIndexes := make([]interface{}, 0, argCountInClause)

	for argCountInClause > 0 {
		argIndexes = append(argIndexes, argIndex)
		argIndex++
		argCountInClause--
	}

	newSQL := fmt.Sprintf(clause, argIndexes...)
	resArgs := append(existingArgs, newArgs...)
	return newSQL, resArgs, nil
}

type clauseBuilder struct {
	metricName    string
	contradiction bool
	clauses       []string
	args          []interface{}
}

func (c *clauseBuilder) SetMetricName(name string) {
	if c.metricName == "" {
		c.metricName = name
		return
	}

	/* Impossible to have 2 different metric names at same time */
	if c.metricName != name {
		c.contradiction = true
	}
}

func (c *clauseBuilder) GetMetricName() string {
	return c.metricName
}

func (c *clauseBuilder) addClause(clause string, args ...interface{}) error {
	clauseWithParameters, newArgs, err := setParameterNumbers(clause, c.args, args...)
	if err != nil {
		return err
	}

	c.clauses = append(c.clauses, clauseWithParameters)
	c.args = newArgs
	return nil
}

func (c *clauseBuilder) Build(includeMetricName bool) ([]string, []interface{}, error) {
	if c.contradiction {
		return []string{"FALSE"}, nil, nil
	}

	/* no support for queries across all data */
	if len(c.clauses) == 0 && c.metricName == "" {
		return nil, nil, errors.ErrNoClausesGen
	}

	if includeMetricName && c.metricName != "" {
		nameClause, newArgs, err := setParameterNumbers(subQueryEQ, c.args, pgmodel.MetricNameLabelName, c.metricName)
		if err != nil {
			return nil, nil, err
		}
		return append(c.clauses, nameClause), newArgs, err
	}

	if len(c.clauses) == 0 {
		return []string{"TRUE"}, nil, nil
	}
	return c.clauses, c.args, nil
}

func buildTimeSeries(rows []timescaleRow, lr lreader.LabelsReader) ([]*prompb.TimeSeries, error) {
	results := make([]*prompb.TimeSeries, 0, len(rows))

	for _, row := range rows {
		if row.err != nil {
			return nil, row.err
		}

		if len(row.times.Elements) != len(row.values.Elements) {
			return nil, errors.ErrQueryMismatchTimestampValue
		}

		promLabels, err := lr.PrompbLabelsForIds(row.labelIds)
		if err != nil {
			return nil, err
		}

		sort.Slice(promLabels, func(i, j int) bool {
			return promLabels[i].Name < promLabels[j].Name
		})

		result := &prompb.TimeSeries{
			Labels:  promLabels,
			Samples: make([]prompb.Sample, 0, len(row.times.Elements)),
		}

		for i := range row.times.Elements {
			result.Samples = append(result.Samples, prompb.Sample{
				Timestamp: pgmodel.TimestamptzToMs(row.times.Elements[i]),
				Value:     row.values.Elements[i].Float,
			})
		}

		results = append(results, result)
	}

	return results, nil
}

func BuildMetricNameSeriesIDQuery(cases []string) string {
	return fmt.Sprintf(metricNameSeriesIDSQLFormat, strings.Join(cases, " AND "))
}

func buildTimeseriesBySeriesIDQuery(filter metricTimeRangeFilter, series []pgmodel.SeriesID) string {
	s := make([]string, 0, len(series))
	for _, sID := range series {
		s = append(s, fmt.Sprintf("%d", sID))
	}
	return fmt.Sprintf(
		timeseriesBySeriesIDsSQLFormat,
		pgx.Identifier{schema.Data, filter.metric}.Sanitize(),
		pgx.Identifier{schema.DataSeries, filter.metric}.Sanitize(),
		strings.Join(s, ","),
		filter.startTime,
		filter.endTime,
	)
}

func buildTimeseriesByLabelClausesQuery(filter metricTimeRangeFilter, cases []string, values []interface{},
	hints *storage.SelectHints, path []parser.Node) (string, []interface{}, parser.Node, error) {
	qf, node, err := getAggregators(hints, path)
	if err != nil {
		return "", nil, nil, err
	}

	timeClauseBound, values, err := setParameterNumbers(qf.timeClause, values, qf.timeParams...)
	if err != nil {
		return "", nil, nil, err
	}
	valueClauseBound, values, err := setParameterNumbers(qf.valueClause, values, qf.valueParams...)
	if err != nil {
		return "", nil, nil, err
	}

	finalSQL := fmt.Sprintf(timeseriesByMetricSQLFormat,
		pgx.Identifier{schema.Data, filter.metric}.Sanitize(),
		pgx.Identifier{schema.DataSeries, filter.metric}.Sanitize(),
		strings.Join(cases, " AND "),
		filter.startTime,
		filter.endTime,
		timeClauseBound,
		valueClauseBound,
	)

	return finalSQL, values, node, nil
}

func hasSubquery(path []parser.Node) bool {
	for _, node := range path {
		switch node.(type) {
		case *parser.SubqueryExpr:
			return true
		}
	}
	return false
}

type aggregators struct {
	timeClause  string
	timeParams  []interface{}
	valueClause string
	valueParams []interface{}
}

/* The path is the list of ancestors (direct parent last) returned node is the most-ancestral node processed by the pushdown */
func getAggregators(hints *storage.SelectHints, path []parser.Node) (*aggregators, parser.Node, error) {
	if extension.ExtensionIsInstalled && path != nil && hints != nil && len(path) >= 2 && !hasSubquery(path) {
		var topNode parser.Node

		node := path[len(path)-2]
		switch n := node.(type) {
		case *parser.Call:
			if n.Func.Name == "delta" {
				topNode = node
				queryStart := hints.Start + hints.Range
				queryEnd := hints.End
				stepDuration := time.Second
				rangeDuration := time.Duration(hints.Range) * time.Millisecond

				if hints.Step > 0 {
					stepDuration = time.Duration(hints.Step) * time.Millisecond
				} else {
					if queryStart != queryEnd {
						panic("query start should equal query end")
					}
				}
				qf := aggregators{
					timeClause:  "ARRAY(SELECT generate_series($%d::timestamptz, $%d::timestamptz, $%d))",
					timeParams:  []interface{}{model.Time(queryStart).Time(), model.Time(queryEnd).Time(), stepDuration},
					valueClause: "prom_delta($%d, $%d,$%d, $%d, time, value)",
					valueParams: []interface{}{model.Time(hints.Start).Time(), model.Time(queryEnd).Time(), int64(stepDuration.Milliseconds()), int64(rangeDuration.Milliseconds())},
				}
				return &qf, topNode, nil
			}
		default:
			//No pushdown optimization by default
		}
	}

	qf := aggregators{
		timeClause:  "array_agg(time)",
		valueClause: "array_agg(value)",
	}
	return &qf, nil, nil
}

func GetSeriesPerMetric(rows pgx.Rows) ([]string, [][]pgmodel.SeriesID, error) {
	metrics := make([]string, 0)
	series := make([][]pgmodel.SeriesID, 0)

	for rows.Next() {
		var (
			metricName string
			seriesIDs  []int64
		)
		if err := rows.Scan(&metricName, &seriesIDs); err != nil {
			return nil, nil, err
		}

		sIDs := make([]pgmodel.SeriesID, 0, len(seriesIDs))

		for _, v := range seriesIDs {
			sIDs = append(sIDs, pgmodel.SeriesID(v))
		}

		metrics = append(metrics, metricName)
		series = append(series, sIDs)
	}

	return metrics, series, nil
}

// anchorValue adds anchors to values in regexps since PromQL docs
// states that "Regex-matches are fully anchored."
func anchorValue(str string) string {
	l := len(str)

	if l == 0 {
		return "^$"
	}

	if str[0] == '^' && str[l-1] == '$' {
		return str
	}

	if str[0] == '^' {
		return fmt.Sprintf("%s$", str)
	}

	if str[l-1] == '$' {
		return fmt.Sprintf("^%s", str)
	}

	return fmt.Sprintf("^%s$", str)
}

func toMilis(t time.Time) int64 {
	return t.UnixNano() / 1e6
}

func toRFC3339Nano(milliseconds int64) string {
	if milliseconds == minTime {
		return "-Infinity"
	}
	if milliseconds == maxTime {
		return "Infinity"
	}
	sec := milliseconds / 1000
	nsec := (milliseconds - (sec * 1000)) * 1000000
	return time.Unix(sec, nsec).UTC().Format(time.RFC3339Nano)
}
