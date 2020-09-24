// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

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

	metricNameSeriesIDSQLFormat = `SELECT m.metric_name, array_agg(s.id)
	FROM _prom_catalog.series s
	INNER JOIN _prom_catalog.metric m
	ON (m.id = s.metric_id)
	WHERE %s
	GROUP BY m.metric_name
	ORDER BY m.metric_name`

	timeseriesByMetricSQLFormat = `
	FROM %[1]s m
	INNER JOIN %[2]s s
	ON m.series_id = s.id
	WHERE %[3]s
	AND time >= '%[4]s'
	AND time <= '%[5]s'
	GROUP BY s.id`

	timeseriesBySeriesIDsSQLFormat = `SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)
	FROM %[1]s m
	INNER JOIN %[2]s s
	ON m.series_id = s.id
	WHERE m.series_id IN (%[3]s)
	AND time >= '%[4]s'
	AND time <= '%[5]s'
	GROUP BY s.id`
)

var (
	minTime = timestamp.FromTime(time.Unix(math.MinInt64/1000+62135596801, 0).UTC())
	maxTime = timestamp.FromTime(time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC())
)

func buildSubQueries(matchers []*labels.Matcher) (string, []string, []interface{}, error) {
	var err error
	metric := ""
	metricMatcherCount := 0
	cb := clauseBuilder{}

	if err != nil {
		return "", nil, nil, err
	}

	for _, m := range matchers {
		// From the PromQL docs: "Label matchers that match
		// empty label values also select all time series that
		// do not have the specific label set at all."
		matchesEmpty := m.Matches("")

		switch m.Type {
		case labels.MatchEqual:
			if m.Name == MetricNameLabelName {
				metricMatcherCount++
				metric = m.Value
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
			return "", nil, nil, err
		}

		// Empty value (default case) is ignored.
	}

	// We can be certain that we want a single metric only if we find a single metric name matcher.
	// Note: possible future optimization for this case, since multiple metric names would exclude
	// each other and give empty result.
	if metricMatcherCount > 1 {
		metric = ""
	}
	clauses, values := cb.build()

	if len(clauses) == 0 {
		err = fmt.Errorf("no clauses generated")
	}

	return metric, clauses, values, err
}

func fillInParameters(query string, existingArgs []interface{}, newArgs ...interface{}) (string, []interface{}, error) {
	argIndex := len(existingArgs) + 1
	argCountInClause := strings.Count(query, "%d")

	if argCountInClause != len(newArgs) {
		return "", nil, fmt.Errorf("invalid number of args: in sql %d vs args %d", argCountInClause, len(newArgs))
	}

	argIndexes := make([]interface{}, 0, argCountInClause)

	for argCountInClause > 0 {
		argIndexes = append(argIndexes, argIndex)
		argIndex++
		argCountInClause--
	}

	newSQL := fmt.Sprintf(query, argIndexes...)
	resArgs := append(existingArgs, newArgs...)
	return newSQL, resArgs, nil
}

type clauseBuilder struct {
	clauses []string
	args    []interface{}
}

func (c *clauseBuilder) addClause(clause string, args ...interface{}) error {
	clauseWithParameters, newArgs, err := fillInParameters(clause, c.args, args...)
	if err != nil {
		return err
	}

	c.clauses = append(c.clauses, clauseWithParameters)
	c.args = newArgs
	return nil
}

func (c *clauseBuilder) build() ([]string, []interface{}) {
	return c.clauses, c.args
}

func buildTimeSeries(rows []timescaleRow, q *pgxQuerier) ([]*prompb.TimeSeries, error) {
	results := make([]*prompb.TimeSeries, 0, len(rows))

	for _, row := range rows {
		if row.err != nil {
			return nil, row.err
		}

		if len(row.times.Elements) != len(row.values.Elements) {
			return nil, fmt.Errorf("query returned a mismatch in timestamps and values")
		}

		promLabels, err := q.getPrompbLabelsForIds(row.labelIds)
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
				Timestamp: timestamptzToMs(row.times.Elements[i]),
				Value:     row.values.Elements[i].Float,
			})
		}

		results = append(results, result)
	}

	return results, nil
}

func buildMetricNameSeriesIDQuery(cases []string) string {
	return fmt.Sprintf(metricNameSeriesIDSQLFormat, strings.Join(cases, " AND "))
}

func buildTimeseriesBySeriesIDQuery(filter metricTimeRangeFilter, series []SeriesID) string {
	s := make([]string, 0, len(series))
	for _, sID := range series {
		s = append(s, fmt.Sprintf("%d", sID))
	}
	return fmt.Sprintf(
		timeseriesBySeriesIDsSQLFormat,
		pgx.Identifier{dataSchema, filter.metric}.Sanitize(),
		pgx.Identifier{dataSeriesSchema, filter.metric}.Sanitize(),
		strings.Join(s, ","),
		filter.startTime,
		filter.endTime,
	)
}

func buildTimeseriesByLabelClausesQuery(filter metricTimeRangeFilter, cases []string, values []interface{},
	hints *storage.SelectHints, path []parser.Node) (string, []interface{}, parser.Node, error) {
	restOfQuery := fmt.Sprintf(
		timeseriesByMetricSQLFormat,
		pgx.Identifier{dataSchema, filter.metric}.Sanitize(),
		pgx.Identifier{dataSeriesSchema, filter.metric}.Sanitize(),
		strings.Join(cases, " AND "),
		filter.startTime,
		filter.endTime,
	)

	qf, node, err := getQueryFinalizer(restOfQuery, values, hints, path)
	if err != nil {
		return "", nil, nil, err
	}

	query, newValues, err := qf.Finalize()
	if err != nil {
		return "", nil, nil, err
	}
	return query, newValues, node, nil
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

type queryFinalizer struct {
	timeClause        string
	timeParams        []interface{}
	valueClause       string
	valueParams       []interface{}
	restOfQuery       string
	restOfQueryParams []interface{}
}

func (t *queryFinalizer) Finalize() (string, []interface{}, error) {
	fullQuery := `SELECT s.labels, ` + t.timeClause + `, ` + t.valueClause + t.restOfQuery
	newParams := append(t.timeParams, t.valueParams...)
	return fillInParameters(fullQuery, t.restOfQueryParams, newParams...)
}

/* The path is the list of ancestors (direct parent last) returned node is the most-ancestral node processed by the pushdown */
func getQueryFinalizer(otherClauses string, values []interface{}, hints *storage.SelectHints, path []parser.Node) (*queryFinalizer, parser.Node, error) {
	if ExtensionIsInstalled && path != nil && hints != nil && len(path) >= 2 && !hasSubquery(path) {
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
				qf := queryFinalizer{
					timeClause:        "ARRAY(SELECT generate_series($%d::timestamptz, $%d::timestamptz, $%d))",
					timeParams:        []interface{}{model.Time(queryStart).Time(), model.Time(queryEnd).Time(), stepDuration},
					valueClause:       "prom_delta($%d, $%d,$%d, $%d, time, value ORDER BY time ASC)",
					valueParams:       []interface{}{model.Time(hints.Start).Time(), model.Time(queryEnd).Time(), int64(stepDuration.Milliseconds()), int64(rangeDuration.Milliseconds())},
					restOfQuery:       otherClauses,
					restOfQueryParams: values,
				}
				return &qf, topNode, nil
			}
		default:
			//No pushdown optimization by default
		}
	}

	qf := queryFinalizer{
		timeClause:        "array_agg(m.time ORDER BY time) as time_array",
		valueClause:       "array_agg(m.value ORDER BY time)",
		restOfQuery:       otherClauses,
		restOfQueryParams: values,
	}
	return &qf, nil, nil
}

func getSeriesPerMetric(rows pgx.Rows) ([]string, [][]SeriesID, error) {
	metrics := make([]string, 0)
	series := make([][]SeriesID, 0)

	for rows.Next() {
		var (
			metricName string
			seriesIDs  []int64
		)
		if err := rows.Scan(&metricName, &seriesIDs); err != nil {
			return nil, nil, err
		}

		sIDs := make([]SeriesID, 0, len(seriesIDs))

		for _, v := range seriesIDs {
			sIDs = append(sIDs, SeriesID(v))
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
