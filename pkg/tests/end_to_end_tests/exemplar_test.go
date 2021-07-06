// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/exemplar"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/query"
)

var rawExemplar_1 = []prompb.Exemplar{
	{Timestamp: 1, Value: 0, Labels: []prompb.Label{{Name: "TraceID", Value: "abcde"}}},
	{Timestamp: 2, Value: 1, Labels: []prompb.Label{{Name: "TraceID", Value: "abcdef"}, {Name: "component", Value: "E2E"}}},
	{Timestamp: 3, Value: 2, Labels: []prompb.Label{}}, // Empty labels valid according to Open Metrics.
	{Timestamp: 4, Value: 3, Labels: []prompb.Label{{Name: "component", Value: "tests"}, {Name: "instance", Value: "localhost:9100"}}},
	{Timestamp: 5, Value: 4, Labels: []prompb.Label{{Name: "job", Value: "generator"}}},
	{Timestamp: 6, Value: 5, Labels: []prompb.Label{}},
}

var rawExmplar_2 = []prompb.Exemplar{
	{Timestamp: 0, Value: 0, Labels: []prompb.Label{}},
	{Timestamp: 1, Value: 1, Labels: []prompb.Label{{Name: "id", Value: "xyz"}}},
	{Timestamp: 2, Value: 2, Labels: []prompb.Label{}}, // Empty labels valid according to Open Metrics.
	{Timestamp: 3, Value: 3, Labels: []prompb.Label{}},
	{Timestamp: 4, Value: 4, Labels: []prompb.Label{{Name: "id", Value: "id-abc"}, {Name: "severity", Value: "high"}}},
	{Timestamp: 5, Value: 5, Labels: []prompb.Label{}},
}

var (
	metric_1 = "test_metric_1"
	metric_2 = "test_metric_2_histogram"
)

var exemplarTS_1 = []prompb.TimeSeries{ // Like what Prometheus sends.
	{
		Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_1}, {Name: "job", Value: "generator"}},
		Samples: []prompb.Sample{{Timestamp: 0, Value: 0}},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_1}, {Name: "job", Value: "generator"}},
		Exemplars: []prompb.Exemplar{rawExemplar_1[0]},
	},
	{
		Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "1"}},
		Samples: []prompb.Sample{{Timestamp: 1, Value: 1}},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "1"}},
		Exemplars: []prompb.Exemplar{rawExemplar_1[1]},
	},
	{
		Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "10"}},
		Samples: []prompb.Sample{{Timestamp: 2, Value: 2}},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "10"}},
		Exemplars: []prompb.Exemplar{rawExemplar_1[2]},
	},
	{
		Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "100"}},
		Samples: []prompb.Sample{{Timestamp: 3, Value: 3}},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "100"}},
		Exemplars: []prompb.Exemplar{rawExemplar_1[3]},
	},
}

var exemplarTS_2 = []prompb.TimeSeries{
	{
		Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_1}, {Name: "job", Value: "generator"}},
		Samples: generateSamples(100),
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_1}, {Name: "job", Value: "generator"}},
		Exemplars: rawExmplar_2,
	},
}

type exemplarTableRow struct {
	ts                  float64 // Epoch in postgres is decimal.
	seriesId            int64
	exemplarLabelValues []string
	val                 float64
}

func TestExemplarIngestion(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		db = testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		rowsInserted, err := ingestor.Ingest(newWriteRequestWithTs(exemplarTS_1))
		require.NoError(t, err)
		require.Equal(t, 8, int(rowsInserted))

		// Check inserted samples tables.
		rows, err := db.Query(context.Background(), "SELECT metric_name FROM _prom_catalog.exemplar")
		require.NoError(t, err)
		var ingestedMetrics []string
		for rows.Next() {
			var metricName string
			err := rows.Scan(&metricName)
			require.NoError(t, err)
			ingestedMetrics = append(ingestedMetrics, metricName)
		}
		sort.Strings(ingestedMetrics)
		require.Equal(t, []string{metric_1, metric_2}, ingestedMetrics)

		// Check num inserted exemplar_key_position.
		var count int
		err = db.QueryRow(context.Background(), "SELECT count(pos) FROM _prom_catalog.exemplar_label_key_position").
			Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 3, count)

		// Check inserted exemplars.
		expectedRows := []exemplarTableRow{
			{0.004, 4, []string{"tests", "localhost:9100"}, 3},
			{0.003, 3, []string{model.EmptyExemplarValues, model.EmptyExemplarValues}, 2},
			{0.002, 2, []string{"E2E", "abcdef"}, 1},
		}
		rows, err = db.Query(context.Background(), "SELECT extract(epoch FROM time), exemplar_label_values, value FROM prom_data_exemplar."+metric_2+" ORDER BY time DESC")
		require.NoError(t, err)
		i := 0
		for rows.Next() {
			var (
				ts          float64
				labelValues []string
				val         float64
			)
			err = rows.Scan(&ts, &labelValues, &val)
			require.NoError(t, err)

			r := expectedRows[i]

			require.Equal(t, r.ts, ts)
			sort.Strings(labelValues)
			sort.Strings(r.exemplarLabelValues)
			require.Equal(t, r.exemplarLabelValues, labelValues)
			require.Equal(t, r.val, val)
			i++
		}
	})
}

func TestExemplarQueryingAPI(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		db = testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		rowsInserted, err := ingestor.Ingest(newWriteRequestWithTs(exemplarTS_1))
		require.NoError(t, err)
		require.Equal(t, 8, int(rowsInserted))

		labelsReader := lreader.NewLabelsReader(pgxconn.NewPgxConn(db), cache.NewLabelsCache(cache.DefaultConfig))
		r := querier.NewQuerier(
			pgxconn.NewPgxConn(db),
			cache.NewMetricCache(cache.DefaultConfig),
			labelsReader,
			cache.NewExemplarLabelsPosCache(cache.DefaultConfig), nil)
		queryable := query.NewQueryable(r, labelsReader)

		// Just query all exemplars corresponding to metric_2 histogram.
		results, err := exemplar.QueryExemplar(context.Background(), metric_2, queryable, time.Unix(0, 0), time.Unix(1, 0))
		require.NoError(t, err)

		bSlice, err := json.Marshal(results)
		require.NoError(t, err)
		require.Equal(t,
			`[{"seriesLabels":{"__name__":"test_metric_2_histogram","job":"generator","le":"10"},"exemplars":[{"labels":{},"value":2,"timestamp":0.003}]},{"seriesLabels":{"__name__":"test_metric_2_histogram","job":"generator","le":"100"},"exemplars":[{"labels":{"component":"tests","instance":"localhost:9100"},"value":3,"timestamp":0.004}]},{"seriesLabels":{"__name__":"test_metric_2_histogram","job":"generator","le":"1"},"exemplars":[{"labels":{"TraceID":"abcdef","component":"E2E"},"value":1,"timestamp":0.002}]}]`,
			string(bSlice))

		// Query all exemplars of all metrics that we inserted.
		results, err = exemplar.QueryExemplar(context.Background(), `{__name__=~"test_metric_.*"}`, queryable, time.Unix(0, 0), time.Unix(1, 0))
		require.NoError(t, err)

		bSlice, err = json.Marshal(results)
		require.NoError(t, err)
		require.Equal(t,
			`[{"seriesLabels":{"__name__":"test_metric_2_histogram","job":"generator","le":"1"},"exemplars":[{"labels":{"TraceID":"abcdef","component":"E2E"},"value":1,"timestamp":0.002}]},{"seriesLabels":{"__name__":"test_metric_2_histogram","job":"generator","le":"10"},"exemplars":[{"labels":{},"value":2,"timestamp":0.003}]},{"seriesLabels":{"__name__":"test_metric_2_histogram","job":"generator","le":"100"},"exemplars":[{"labels":{"component":"tests","instance":"localhost:9100"},"value":3,"timestamp":0.004}]},{"seriesLabels":{"__name__":"test_metric_1","job":"generator"},"exemplars":[{"labels":{"TraceID":"abcde"},"value":0,"timestamp":0.001}]}]`,
			string(bSlice))
	})
}
