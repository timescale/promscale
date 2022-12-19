// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package querier

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/jackc/pgx/v5"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/tenancy"
	"github.com/timescale/promscale/pkg/util"
)

func TestPGXQuerierQuery(t *testing.T) {
	testCases := []struct {
		name       string
		query      *prompb.Query
		result     []*prompb.TimeSeries
		err        error
		sqlQueries []model.SqlQuery // XXX whitespace in these is significant
	}{
		{
			name: "Error metric name value",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: model.MetricNameLabelName, Value: "bar"},
				},
			},
			sqlQueries: []model.SqlQuery{
				{
					Sql: "SELECT m.table_schema, m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name, m.table_schema\n\t" +
						"ORDER BY m.metric_name, m.table_schema",
					Args:    []interface{}{"__name__", "bar"},
					Results: model.RowResults{{1, 1, []int64{}}},
					Err:     error(nil),
				},
			},
			err: fmt.Errorf("wrong value type int for scan of *string"),
		},
		{
			name: "Error first query",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: model.MetricNameLabelName, Value: "bar"},
				},
			},
			sqlQueries: []model.SqlQuery{
				{
					Sql: "SELECT m.table_schema, m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name, m.table_schema\n\t" +
						"ORDER BY m.metric_name, m.table_schema",
					Args:    []interface{}{"__name__", "bar"},
					Results: model.RowResults{{"{}", []time.Time{}, []float64{}}},
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
			sqlQueries: []model.SqlQuery{
				{
					Sql: "SELECT m.table_schema, m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name, m.table_schema\n\t" +
						"ORDER BY m.metric_name, m.table_schema",
					Args:    []interface{}{"foo", "bar"},
					Results: model.RowResults{{"prom_data", "foo", []int64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"prom_data", "foo"},
					Results: model.RowResults{{int64(1), "prom_data", "foo", "foo"}},
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
			sqlQueries: []model.SqlQuery{
				{
					Sql: "SELECT m.table_schema, m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name, m.table_schema\n\t" +
						"ORDER BY m.metric_name, m.table_schema",
					Args:    []interface{}{"foo", "bar"},
					Results: model.RowResults{{"prom_data", "foo", []int64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"prom_data", "foo"},
					Results: model.RowResults{{int64(1), "prom_data", "foo", "foo"}},
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
					Results: model.RowResults(nil),
					Err:     fmt.Errorf("some error 3")}},
		},
		{
			name: "Error scan values",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: model.MetricNameLabelName, Value: "bar"},
				},
			},
			sqlQueries: []model.SqlQuery{
				{
					Sql: "SELECT m.table_schema, m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name, m.table_schema\n\t" +
						"ORDER BY m.metric_name, m.table_schema",
					Args:    []interface{}{"__name__", "bar"},
					Results: model.RowResults{{0}},
					Err:     error(nil),
				},
			},
			err: fmt.Errorf("mock scanning error, missing results for scanning: got 1 []interface {}{0}\nwanted 3"),
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
			sqlQueries: []model.SqlQuery{
				{
					Sql: "SELECT m.table_schema, m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name, m.table_schema\n\t" +
						"ORDER BY m.metric_name, m.table_schema",
					Args:    []interface{}{"foo", "bar"},
					Results: model.RowResults(nil),
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
					{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{},
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"", "bar"},
					Results: model.RowResults{nil},
					Err:     pgx.ErrNoRows,
				},
			},
		},
		{
			name: "Simple query, exclude matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_NEQ, Name: model.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: "foo"}},
					Samples: []prompb.Sample{{Timestamp: timestamp.FromTime(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []model.SqlQuery{
				{
					Sql: "SELECT m.table_schema, m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name, m.table_schema\n\t" +
						"ORDER BY m.metric_name, m.table_schema",
					Args:    []interface{}{"__name__", "bar"},
					Results: model.RowResults{{"prom_data", "foo", []int64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"prom_data", "foo"},
					Results: model.RowResults{{int64(1), "prom_data", "foo", "foo"}},
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
					Results: model.RowResults{{[]*int64{util.Pointer(int64(1))}, []time.Time{time.Unix(0, 0)}, []float64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT (prom_api.labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{1}},
					Results: model.RowResults{{[]int64{1}, []string{"__name__"}, []string{"foo"}}},
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
					{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: "bar"}},
					Samples: []prompb.Sample{{Timestamp: timestamp.FromTime(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"", "bar"},
					Results: model.RowResults{{int64(1), "prom_data", "bar", "bar"}},
					Err:     error(nil),
				},
				{
					Sql: `SELECT series.labels, result.time_array, result.value_array
					FROM "prom_data_series"."bar" series
					INNER JOIN (
						SELECT series_id, array_agg(time) as time_array, array_agg(value) as value_array
						FROM ( SELECT series_id, time, "value" as value FROM "prom_data"."bar" metric
						WHERE time >= '1970-01-01T00:00:01Z' AND time <= '1970-01-01T00:00:02Z'
						ORDER BY series_id, time ) as time_ordered_rows
						GROUP BY series_id
						) as result ON (result.value_array is not null AND result.series_id = series.id)`,
					Args: nil,
					Results: model.RowResults{
						{[]*int64{util.Pointer(int64(2))}, []time.Time{time.Unix(0, 0)}, []float64{1}},
					},
					Err: error(nil),
				},
				{
					Sql:     "SELECT (prom_api.labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{2}},
					Results: model.RowResults{{[]int64{2}, []string{"__name__"}, []string{"bar"}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, metric name matcher, custom schema",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabelName, Value: "custom"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "custom"},
						{Name: model.SchemaNameLabelName, Value: "custom_schema"},
					},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"", "custom"},
					Results: model.RowResults{{int64(1), "custom_schema", "custom", "bar"}},
					Err:     error(nil),
				},
				{
					Sql: `SELECT series.labels, result.time_array, result.value_array
					FROM "prom_data_series"."bar" series
					INNER JOIN (
						SELECT series_id, array_agg(time) as time_array, array_agg(value) as value_array
						FROM ( SELECT series_id, time, "value" as value FROM "custom_schema"."custom" metric
						WHERE time >= '1970-01-01T00:00:01Z' AND time <= '1970-01-01T00:00:02Z'
						ORDER BY series_id, time ) as time_ordered_rows
						GROUP BY series_id
						) as result ON (result.value_array is not null AND result.series_id = series.id)`,
					Args: nil,
					Results: model.RowResults{
						{
							[]*int64{util.Pointer(int64(2))},
							[]time.Time{time.Unix(0, 0)},
							[]float64{1},
						},
					},
					Err: error(nil),
				},
				{
					Sql:     "SELECT (prom_api.labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{2}},
					Results: model.RowResults{{[]int64{2}, []string{"__name__"}, []string{"bar"}}},
					Err:     error(nil),
				},
			},
		},
		{
			name: "Simple query, metric name matcher, custom column",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: model.ColumnNameLabelName, Value: "max"},
					{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.ColumnNameLabelName, Value: "max"},
						{Name: model.MetricNameLabelName, Value: "bar"},
					},
					Samples: []prompb.Sample{{Timestamp: toMilis(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"", "bar"},
					Results: model.RowResults{{int64(1), "prom_data", "bar", "bar"}},
					Err:     error(nil),
				},
				{
					Sql: `SELECT series.labels, result.time_array, result.value_array
					FROM "prom_data_series"."bar" series
					INNER JOIN (
						SELECT series_id, array_agg(time) as time_array, array_agg(value) as value_array
						FROM ( SELECT series_id, time, "max" as value FROM "prom_data"."bar" metric
						WHERE time >= '1970-01-01T00:00:01Z' AND time <= '1970-01-01T00:00:02Z'
						ORDER BY series_id, time ) as time_ordered_rows
						GROUP BY series_id
						) as result ON (result.value_array is not null AND result.series_id = series.id)`,
					Args: nil,
					Results: model.RowResults{
						{
							[]*int64{util.Pointer(int64(2))},
							[]time.Time{time.Unix(0, 0)},
							[]float64{1},
						},
					},
					Err: error(nil),
				},
				{
					Sql:     "SELECT (prom_api.labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{2}},
					Results: model.RowResults{{[]int64{2}, []string{"__name__"}, []string{"bar"}}},
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
					{Type: prompb.LabelMatcher_RE, Name: model.MetricNameLabelName, Value: ""},
				},
			},
			result: []*prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: "foo"}},
					Samples: []prompb.Sample{{Timestamp: timestamp.FromTime(time.Unix(0, 0)), Value: 1}},
				},
				{
					Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: "bar"}},
					Samples: []prompb.Sample{{Timestamp: timestamp.FromTime(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []model.SqlQuery{
				{
					Sql: "SELECT m.table_schema, m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value !~ $2)\n\t" +
						"GROUP BY m.metric_name, m.table_schema\n\t" +
						"ORDER BY m.metric_name, m.table_schema",
					Args:    []interface{}{"__name__", "^(?:)$"},
					Results: model.RowResults{{"prom_data", "foo", []int64{1}}, {"prom_data", "bar", []int64{1}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"prom_data", "foo"},
					Results: model.RowResults{{int64(1), "prom_data", "foo", "foo"}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"prom_data", "bar"},
					Results: model.RowResults{{int64(1), "prom_data", "bar", "bar"}},
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
					Args: []interface{}(nil),
					Results: model.RowResults{
						{
							[]*int64{util.Pointer(int64(3))},
							[]time.Time{time.Unix(0, 0)},
							[]float64{1},
						},
					},
					Err: error(nil),
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
					Args: []interface{}(nil),
					Results: model.RowResults{
						{
							[]*int64{util.Pointer(int64(4))},
							[]time.Time{time.Unix(0, 0)},
							[]float64{1},
						},
					},
					Err: error(nil),
				},
				{
					Sql:           "SELECT (prom_api.labels_info($1::int[])).*",
					Args:          []interface{}{[]int64{3, 4}},
					ArgsUnordered: true,
					Results:       model.RowResults{{[]int64{3, 4}, []string{"__name__", "__name__"}, []string{"foo", "bar"}}},
					Err:           error(nil),
				},
			},
		},
		{
			name: "Simple query, double metric name matcher",
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabelName, Value: "foo"},
					{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabelName, Value: "bar"},
				},
			},
			result: []*prompb.TimeSeries{},
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"", "foo"},
					Results: model.RowResults{{int64(1), "prom_data", "foo", "foo"}},
					Err:     error(nil),
				},
				{
					Sql: `SELECT series.labels, result.time_array, result.value_array FROM "prom_data_series"."foo" series
					INNER JOIN LATERAL
					( SELECT array_agg(time) as time_array, array_agg(value) as value_array FROM ( SELECT time, "value" as value FROM "prom_data"."foo" metric WHERE metric.series_id = series.id AND time >= '1970-01-01T00:00:01Z' AND time <= '1970-01-01T00:00:02Z' ORDER BY time ) as time_ordered_rows ) as result ON (result.value_array is not null)
					WHERE FALSE`,
					Args:    []interface{}(nil),
					Results: model.RowResults{},
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
					Samples: []prompb.Sample{{Timestamp: timestamp.FromTime(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []model.SqlQuery{
				{
					Sql: "SELECT m.table_schema, m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2)\n\t" +
						"GROUP BY m.metric_name, m.table_schema\n\t" +
						"ORDER BY m.metric_name, m.table_schema",
					Args:    []interface{}{"foo", "bar"},
					Results: model.RowResults{{"prom_data", "metric", []int64{1, 99, 98}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"prom_data", "metric"},
					Results: model.RowResults{{int64(1), "prom_data", "metric", "metric"}},
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
					Args: []interface{}(nil),
					Results: model.RowResults{
						{
							[]*int64{util.Pointer(int64(7))},
							[]time.Time{time.Unix(0, 0)},
							[]float64{1},
						},
					},
					Err: error(nil),
				},
				{
					Sql:     "SELECT (prom_api.labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{7}},
					Results: model.RowResults{{[]int64{7}, []string{"foo"}, []string{"bar"}}},
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
					Samples: []prompb.Sample{{Timestamp: timestamp.FromTime(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []model.SqlQuery{
				{
					Sql: "SELECT m.table_schema, m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value = $2) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $3 and l.value = $4) AND labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $5 and l.value ~ $6) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $7 and l.value ~ $8)\n\t" +
						"GROUP BY m.metric_name, m.table_schema\n\t" +
						"ORDER BY m.metric_name, m.table_schema",
					Args:    []interface{}{"foo", "bar", "foo1", "bar1", "foo2", "^(?:^bar2)$", "foo3", "^(?:bar3$)$"},
					Results: model.RowResults{{"prom_data", "metric", []int64{1, 4, 5}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"prom_data", "metric"},
					Results: model.RowResults{{int64(1), "prom_data", "metric", "metric"}},
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
					Args: []interface{}(nil),
					Results: model.RowResults{
						{
							[]*int64{util.Pointer(int64(8)), util.Pointer(int64(9))},
							[]time.Time{time.Unix(0, 0)},
							[]float64{1},
						},
					},
					Err: error(nil),
				},
				{
					Sql:           "SELECT (prom_api.labels_info($1::int[])).*",
					Args:          []interface{}{[]int64{9, 8}},
					ArgsUnordered: true,
					Results:       model.RowResults{{[]int64{8, 9}, []string{"foo", "foo2"}, []string{"bar", "bar2"}}},
					Err:           error(nil),
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
					Samples: []prompb.Sample{{Timestamp: timestamp.FromTime(time.Unix(0, 0)), Value: 1}},
				},
			},
			sqlQueries: []model.SqlQuery{
				{
					Sql: "SELECT m.table_schema, m.metric_name, array_agg(s.id)\n\t" +
						"FROM _prom_catalog.series s\n\t" +
						"INNER JOIN _prom_catalog.metric m\n\t" +
						"ON (m.id = s.metric_id)\n\t" +
						"WHERE NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $1 and l.value != $2) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $3 and l.value = $4) AND labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $5 and l.value ~ $6) AND NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $7 and l.value ~ $8)\n\t" +
						"GROUP BY m.metric_name, m.table_schema\n\t" +
						"ORDER BY m.metric_name, m.table_schema",
					Args:    []interface{}{"foo", "", "foo1", "bar1", "foo2", "^(?:^bar2$)$", "foo3", "^(?:bar3)$"},
					Results: model.RowResults{{"prom_data", "metric", []int64{1, 2}}},
					Err:     error(nil),
				},
				{
					Sql:     "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)",
					Args:    []interface{}{"prom_data", "metric"},
					Results: model.RowResults{{int64(1), "prom_data", "metric", "metric"}},
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
					Args: []interface{}(nil),
					Results: model.RowResults{
						{
							[]*int64{util.Pointer(int64(10))},
							[]time.Time{time.Unix(0, 0)},
							[]float64{1},
						},
					},
					Err: error(nil),
				},
				{
					Sql:     "SELECT (prom_api.labels_info($1::int[])).*",
					Args:    []interface{}{[]int64{10}},
					Results: model.RowResults{{[]int64{10}, []string{"foo2"}, []string{"bar2"}}},
					Err:     error(nil),
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mock := model.NewSqlRecorder(c.sqlQueries, t)
			mockMetrics := &model.MockMetricCache{
				MetricCache: make(map[string]model.MetricInfo),
			}
			err := mockMetrics.Set(
				"prom_data",
				"metric_1",
				model.MetricInfo{
					TableSchema: "prom_data",
					TableName:   "metricTableName_1",
					SeriesTable: "metric_1",
				}, false,
			)
			if err != nil {
				t.Fatalf("error setting up mock cache: %s", err.Error())
			}
			querier := pgxQuerier{&queryTools{conn: mock, metricTableNames: mockMetrics, labelsReader: lreader.NewLabelsReader(mock, clockcache.WithMax(0), tenancy.NewNoopAuthorizer().ReadAuthorizer())}}

			result, err := querier.RemoteReadQuerier(context.Background()).Query(c.query)

			if err != nil {
				switch {
				case c.err == nil:
					found := false
					for _, q := range c.sqlQueries {
						if err == q.Err {
							found = true
							break
						}
						if q.Err != nil {
							t.Errorf("unexpected error:\ngot\n\t%v\nwanted\n\t%v", err, q.Err)
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
				t.Errorf("unexpected result:\ngot\n%+v\nwanted\n%+v", result, c.result)
			}
		})
	}
}
