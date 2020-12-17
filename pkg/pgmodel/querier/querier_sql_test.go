// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package querier

import (
	"fmt"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/utils"
	"reflect"
	"testing"
	"time"

	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestPGXQuerierQuery(t *testing.T) {
	testCases := []struct {
		name       string
		query      *prompb.Query
		result     []*prompb.TimeSeries
		err        error
		sqlQueries []utils.SqlQuery // XXX whitespace in these is significant
	}{
		{
			name: "Error metric name value",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: utils.MetricNameLabelName, Value: "bar"},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"__name__", "bar"},
					Results: utils.RowResults{{1, []int64{}}},
					Err:     error(nil),
				},
			},
			err: fmt.Errorf("wrong value type int"),
		},
		{
			name: "Error first query",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: utils.MetricNameLabelName, Value: "bar"},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"__name__", "bar"},
					Results: utils.RowResults{{"{}", []time.Time{}, []float64{}}},
					Err:     fmt.Errorf("some error"),
				},
			},
		},
		{
			name: "Error second query",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: "foo", Value: "bar"},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"foo", "bar"},
					Results: utils.RowResults{{`foo`, []int64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"foo"},
					Results: utils.RowResults{{"foo"}},
					Err:     fmt.Errorf("some error 2"),
				},
			},
		},
		{
			name: "Error third query",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: "foo", Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"foo", "bar"},
					Results: utils.RowResults{{"foo", []int64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"foo"},
					Results: utils.RowResults{{"foo"}},
					Err:     error(nil),
				},
				{
					Sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"foo\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"foo\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					Args:    []interface{}(nil),
					Results: utils.RowResults(nil),
					Err:     fmt.Errorf("some error 3")}},
		},
		{
			name: "Error scan values",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: utils.MetricNameLabelName, Value: "bar"},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"__name__", "bar"},
					Results: utils.RowResults{{0}},
					Err:     error(nil),
				},
			},
			err: fmt.Errorf("mock scanning error, missing results for scanning: got 1 []interface {}{0}\nwanted 2"),
		},
		{
			name:   "Empty query",
			result: []*prompb.TimeSeries{},
		},
		{
			name: "Simple query, no result",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"foo", "bar"},
					Results: utils.RowResults(nil),
					Err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, metric doesn't exist",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: utils.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{},
			sqlQueries: []utils.SqlQuery{
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"bar"},
					Results: utils.RowResults(nil),
					Err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, exclude matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: utils.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "foo"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"__name__", "bar"},
					Results: utils.RowResults{{"foo", []int64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"foo"},
					Results: utils.RowResults{{"foo"}},
					Err:     error(nil),
				},
				{
					Sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"foo\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"foo\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{[]int64{1}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT (labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{1}},
					Results: utils.RowResults{{[]int64{1}, []string{"__name__"}, []string{"foo"}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, metric name matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: utils.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "bar"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"bar"},
					Results: utils.RowResults{{"bar"}},
					Err:     error(nil),
				},
				{
					Sql: "SELECT s.labels, array_agg(m.time ORDER BY time) as time_array, array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"bar\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"bar\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					Args:    []interface{}{"__name__", "bar"},
					Results: utils.RowResults{{[]int64{2}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT (labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{2}},
					Results: utils.RowResults{{[]int64{2}, []string{"__name__"}, []string{"bar"}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, empty metric name matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_RE, Name: utils.MetricNameLabelName, Value: ""},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "foo"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "bar"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value !~ $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"__name__", "^$"},
					Results: utils.RowResults{{"foo", []int64{1}}, {"bar", []int64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"foo"},
					Results: utils.RowResults{{"foo"}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"bar"},
					Results: utils.RowResults{{"bar"}},
					Err:     error(nil),
				},
				{
					Sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"foo\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"foo\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{[]int64{3}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					Err:     error(nil),
				},
				{
					Sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"bar\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"bar\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{[]int64{4}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT (labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{3}},
					Results: utils.RowResults{{[]int64{3}, []string{"__name__"}, []string{"foo"}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT (labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{4}},
					Results: utils.RowResults{{[]int64{4}, []string{"__name__"}, []string{"bar"}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, double metric name matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: utils.MetricNameLabelName, Value: "foo"},
					{Type: prompb.LabelMatcher_EQ, Name: utils.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "foo"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
				{
					Labels:  []prompb.Label{{Name: utils.MetricNameLabelName, Value: "bar"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2) AND labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $3 and l.value = $4)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"__name__", "foo", "__name__", "bar"},
					Results: utils.RowResults{{"foo", []int64{1}}, {"bar", []int64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"foo"},
					Results: utils.RowResults{{"foo"}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"bar"},
					Results: utils.RowResults{{"bar"}},
					Err:     error(nil),
				},
				{
					Sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"foo\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"foo\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{[]int64{5}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					Err:     error(nil),
				},
				{
					Sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"bar\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"bar\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{[]int64{6}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT (labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{5}},
					Results: utils.RowResults{{[]int64{5}, []string{"__name__"}, []string{"foo"}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT (labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{6}},
					Results: utils.RowResults{{[]int64{6}, []string{"__name__"}, []string{"bar"}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, no metric name matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: "foo", Value: "bar"}},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"foo", "bar"},
					Results: utils.RowResults{{"metric", []int64{1, 99, 98}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"metric"},
					Results: utils.RowResults{{"metric"}},
					Err:     error(nil),
				},
				{
					Sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"metric\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"metric\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1,99,98)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{[]int64{7}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT (labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{7}},
					Results: utils.RowResults{{[]int64{7}, []string{"foo"}, []string{"bar"}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Complex query, multiple matchers",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
					{Type: prompb.LabelMatcher_NEQ, Name: "foo1", Value: "bar1"},
					{Type: prompb.LabelMatcher_RE, Name: "foo2", Value: "^bar2"},
					{Type: prompb.LabelMatcher_NRE, Name: "foo3", Value: "bar3$"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "foo", Value: "bar"},
						{Name: "foo2", Value: "bar2"},
					},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $3 and l.value = $4) AND labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $5 and l.value ~ $6) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $7 and l.value ~ $8)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"foo", "bar", "foo1", "bar1", "foo2", "^bar2$", "foo3", "^bar3$"},
					Results: utils.RowResults{{"metric", []int64{1, 4, 5}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"metric"},
					Results: utils.RowResults{{"metric"}},
					Err:     error(nil),
				},
				{
					Sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"metric\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"metric\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1,4,5)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{[]int64{8, 9}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT (labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{9, 8}},
					Results: utils.RowResults{{[]int64{8, 9}, []string{"foo", "foo2"}, []string{"bar", "bar2"}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Complex query, empty equal matchers",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "foo", Value: ""},
					{Type: prompb.LabelMatcher_NEQ, Name: "foo1", Value: "bar1"},
					{Type: prompb.LabelMatcher_RE, Name: "foo2", Value: "^bar2$"},
					{Type: prompb.LabelMatcher_NRE, Name: "foo3", Value: "bar3"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "foo2", Value: "bar2"},
					},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []utils.SqlQuery{
				{
					Sql: "SELECT m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value != $2) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $3 and l.value = $4) AND labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $5 and l.value ~ $6) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $7 and l.value ~ $8)\n\t" +
						"GROUP BY m.metric_name\n\t" +
						"ORDER BY m.metric_name",
					Args:    []interface{}{"foo", "", "foo1", "bar1", "foo2", "^bar2$", "foo3", "^bar3$"},
					Results: utils.RowResults{{"metric", []int64{1, 2}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists($1)",
					Args:    []interface{}{"metric"},
					Results: utils.RowResults{{"metric"}},
					Err:     error(nil),
				},
				{
					Sql: "SELECT s.labels, array_agg(m.time ORDER BY time), array_agg(m.value ORDER BY time)\n\t" +
						"FROM \"prom_data\".\"metric\" m\n\t" +
						"INNER JOIN \"prom_data_series\".\"metric\" s\n\t" +
						"ON m.series_id = s.id\n\t" +
						"WHERE m.series_id IN (1,2)\n\t" +
						"AND time >= '1970-01-01T00:00:01Z'\n\t" +
						"AND time <= '1970-01-01T00:00:02Z'\n\t" +
						"GROUP BY s.id",
					Args:    []interface{}(nil),
					Results: utils.RowResults{{[]int64{10}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT (labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{10}},
					Results: utils.RowResults{{[]int64{10}, []string{"foo2"}, []string{"bar2"}}},
					Err:     error(nil),
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mock := newSqlRecorder(c.sqlQueries, t)
			metricCache := map[string]string{"metric_1": "metricTableName_1"}
			mockMetrics := &mockMetricCache{
				metricCache: metricCache,
			}
			querier := pgmodel.pgxQuerier{conn: mock, metricTableNames: mockMetrics, labelsReader: utils.NewLabelsReader(mock, clockcache.WithMax(0))}

			result, err := querier.Query(c.query)

			if err != nil {
				switch {
				case c.err == nil:
					found := false
					for _, q := range c.sqlQueries {
						if err == q.err {
							found = true
							break
						}
						if q.err != nil {
							t.Errorf("unexpected error:\ngot\n\t%v\nwanted\n\t%v", err, q.err)
						}
					}
					if !found {
						t.Errorf("unexpected error for query: %v", err)
					}
				case c.err != nil:
					if err.Error() != c.err.Error() {
						t.Errorf("unexpected error:\ngot\n\t%v\nwanted\n\t%v", err, c.err)
					}
				}
			} else if !reflect.DeepEqual(result, c.result) {
				t.Errorf("unexpected result:\ngot\n%#v\nwanted\n%+v", result, c.result)
			}
		})
	}
}
