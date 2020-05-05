// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
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

	timeseriesByMetricSQLFormat = `SELECT (key_value_array(s.labels)).*, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)
	FROM %[1]s m
	INNER JOIN %[2]s s
	ON m.series_id = s.id
	WHERE %[3]s
	AND time >= '%[4]s'
	AND time <= '%[5]s'
	GROUP BY s.id`

	timeseriesBySeriesIDsSQLFormat = `SELECT (key_value_array(s.labels)).*, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)
	FROM %[1]s m
	INNER JOIN %[2]s s
	ON m.series_id = s.id
	WHERE m.series_id IN (%[3]s)
	AND time >= '%[4]s'
	AND time <= '%[5]s'
	GROUP BY s.id`
)

func buildSubQueries(query *prompb.Query) (string, []string, []interface{}, error) {
	var err error
	metric := ""
	metricMatcherCount := 0
	cb := clauseBuilder{}
	matchers, err := fromLabelMatchers(query.Matchers)

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

// fromLabelMatchers parses protobuf label matchers to Prometheus label matchers.
// TODO: This is a copy of a function in github.com/prometheus/prometheus/storage/remote
// package b/c it was causing build issues. We should remove it and resolve the build issues.
func fromLabelMatchers(matchers []*prompb.LabelMatcher) ([]*labels.Matcher, error) {
	result := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		var mtype labels.MatchType
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			mtype = labels.MatchEqual
		case prompb.LabelMatcher_NEQ:
			mtype = labels.MatchNotEqual
		case prompb.LabelMatcher_RE:
			mtype = labels.MatchRegexp
		case prompb.LabelMatcher_NRE:
			mtype = labels.MatchNotRegexp
		default:
			return nil, errors.New("invalid matcher type")
		}
		matcher, err := labels.NewMatcher(mtype, matcher.Name, matcher.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, matcher)
	}
	return result, nil
}

type clauseBuilder struct {
	clauses []string
	args    []interface{}
}

func (c *clauseBuilder) addClause(clause string, args ...interface{}) error {
	argIndex := len(c.args) + 1
	argCountInClause := strings.Count(clause, "%d")

	if argCountInClause != len(args) {
		return fmt.Errorf("invalid number of args")
	}

	argIndexes := make([]interface{}, 0, argCountInClause)

	for argCountInClause > 0 {
		argIndexes = append(argIndexes, argIndex)
		argIndex++
		argCountInClause--
	}

	c.clauses = append(c.clauses, fmt.Sprintf(clause, argIndexes...))
	c.args = append(c.args, args...)

	return nil
}

func (c *clauseBuilder) build() ([]string, []interface{}) {
	return c.clauses, c.args
}

func buildTimeSeries(rows pgx.Rows) ([]*prompb.TimeSeries, error) {
	results := make([]*prompb.TimeSeries, 0)

	for rows.Next() {
		var (
			keys       []string
			vals       []string
			timestamps []time.Time
			values     []float64
		)
		err := rows.Scan(&keys, &vals, &timestamps, &values)

		if err != nil {
			return nil, err
		}

		if len(timestamps) != len(values) {
			return nil, fmt.Errorf("query returned a mismatch in timestamps and values")
		}

		if len(keys) != len(vals) {
			return nil, fmt.Errorf("query returned a mismatch in label keys and values")
		}

		promLabels := make([]prompb.Label, 0, len(keys))

		for i, k := range keys {
			promLabels = append(promLabels, prompb.Label{
				Name:  k,
				Value: vals[i],
			})
		}

		sort.Slice(promLabels, func(i, j int) bool {
			return promLabels[i].Name < promLabels[j].Name
		})

		result := &prompb.TimeSeries{
			Labels:  promLabels,
			Samples: make([]prompb.Sample, 0, len(timestamps)),
		}

		for i := range timestamps {
			result.Samples = append(result.Samples, prompb.Sample{
				Timestamp: toMilis(timestamps[i]),
				Value:     values[i],
			})
		}

		results = append(results, result)
	}

	return results, nil
}

func buildMetricNameSeriesIDQuery(cases []string) string {
	return fmt.Sprintf(metricNameSeriesIDSQLFormat, strings.Join(cases, " AND "))
}

func buildTimeseriesByLabelClausesQuery(filter metricTimeRangeFilter, cases []string) string {
	return fmt.Sprintf(
		timeseriesByMetricSQLFormat,
		pgx.Identifier{dataSchema, filter.metric}.Sanitize(),
		pgx.Identifier{dataSeriesSchema, filter.metric}.Sanitize(),
		strings.Join(cases, " AND "),
		filter.startTime,
		filter.endTime,
	)
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
	sec := milliseconds / 1000
	nsec := (milliseconds - (sec * 1000)) * 1000000
	return time.Unix(sec, nsec).UTC().Format(time.RFC3339Nano)
}
