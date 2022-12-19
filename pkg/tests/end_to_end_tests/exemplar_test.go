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

	"github.com/jackc/pgx/v5/pgxpool"
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
	"github.com/timescale/promscale/pkg/tenancy"
)

var rawExemplar = []prompb.Exemplar{
	{Timestamp: 1, Value: 0, Labels: []prompb.Label{{Name: "TraceID", Value: "abcde"}}},
	{Timestamp: 2, Value: 1, Labels: []prompb.Label{{Name: "TraceID", Value: "abcdef"}, {Name: "component", Value: "E2E"}}},
	{Timestamp: 3, Value: 2, Labels: []prompb.Label{}}, // Empty labels valid according to Open Metrics.
	{Timestamp: 4, Value: 3, Labels: []prompb.Label{{Name: "component", Value: "tests"}, {Name: "instance", Value: "localhost:9100"}}},
	{Timestamp: 5, Value: 4, Labels: []prompb.Label{{Name: "job", Value: "generator"}}},
	{Timestamp: 6, Value: 5, Labels: []prompb.Label{}},
	{Timestamp: 7, Value: 6, Labels: []prompb.Label{}},
	{Timestamp: 8, Value: 7, Labels: []prompb.Label{{Name: "instance", Value: "localhost:9100"}}},
}

var (
	metric_1 = "test_metric_1"
	metric_2 = "test_metric_2_histogram"
	metric_3 = "test_metric_3_total"
)

var exemplarTS_1 = []prompb.TimeSeries{ // Like what Prometheus sends.
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_1}, {Name: "job", Value: "generator"}},
		Samples:   []prompb.Sample{{Timestamp: 0, Value: 0}},
		Exemplars: []prompb.Exemplar{rawExemplar[0]},
	},
	{
		// Use duplicate data to ensure temporary tables for samples do not conflict with the temporary tables of exemplars declared in copier.
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_1}, {Name: "job", Value: "generator"}},
		Samples:   []prompb.Sample{{Timestamp: 0, Value: 0}},
		Exemplars: []prompb.Exemplar{rawExemplar[0]},
	},
	{
		Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "1"}},
		Samples: []prompb.Sample{{Timestamp: 1, Value: 1}},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "1"}},
		Exemplars: []prompb.Exemplar{rawExemplar[1]},
	},
	{
		Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "10"}},
		Samples: []prompb.Sample{{Timestamp: 2, Value: 2}},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "10"}},
		Exemplars: []prompb.Exemplar{rawExemplar[2]},
	},
	{
		Labels:  []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "100"}},
		Samples: []prompb.Sample{{Timestamp: 3, Value: 3}},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "100"}},
		Exemplars: []prompb.Exemplar{rawExemplar[3]},
	},
}

var exemplarTS_2 = []prompb.TimeSeries{ // If timeseries are sent with exemplars in same alloc.
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_3}, {Name: "job", Value: "generator"}, {Name: "le", Value: "1"}},
		Samples:   []prompb.Sample{{Timestamp: 1, Value: 1}},
		Exemplars: []prompb.Exemplar{rawExemplar[4]},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_3}},
		Samples:   []prompb.Sample{{Timestamp: 0, Value: 0}},
		Exemplars: []prompb.Exemplar{rawExemplar[5]},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_3}, {Name: "job", Value: "generator"}},
		Samples:   []prompb.Sample{{Timestamp: 0, Value: 0}},
		Exemplars: []prompb.Exemplar{rawExemplar[6]},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "10"}},
		Samples:   []prompb.Sample{{Timestamp: 2, Value: 2}},
		Exemplars: []prompb.Exemplar{rawExemplar[5]},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_2}, {Name: "job", Value: "generator"}, {Name: "le", Value: "10"}},
		Samples:   []prompb.Sample{{Timestamp: 3, Value: 3}},
		Exemplars: []prompb.Exemplar{rawExemplar[6]},
	},
	{
		Labels:    []prompb.Label{{Name: model.MetricNameLabelName, Value: metric_1}, {Name: "job", Value: "generator"}, {Name: "le", Value: "100"}},
		Samples:   []prompb.Sample{{Timestamp: 4, Value: 4}},
		Exemplars: []prompb.Exemplar{rawExemplar[7]},
	},
}

type exemplarTableRow struct {
	ts  float64 // Epoch in postgres is decimal.
	val float64

	exemplarLabelValues []string
}

func TestExemplarIngestion(t *testing.T) {
	withDB(t, *testDatabase, func(_ *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		insertablesIngested, metadataIngested, err := ingestor.IngestMetrics(context.Background(), newWriteRequestWithTs(exemplarTS_1))
		require.NoError(t, err)
		require.Equal(t, 10, int(insertablesIngested))
		require.Equal(t, 0, int(metadataIngested))

		// Check inserted samples tables.
		rows, err := db.Query(context.Background(), "SELECT metric_name FROM _prom_catalog.exemplar")
		require.NoError(t, err)
		defer rows.Close()
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
		err = db.QueryRow(context.Background(), "SELECT count(*) FROM _prom_catalog.exemplar_label_key_position").
			Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 4, count)

		// Check inserted exemplars.
		expectedRows := []exemplarTableRow{
			{0.004, 3, []string{model.EmptyExemplarValues, "tests", "localhost:9100"}},
			{0.003, 2, []string{}},
			{0.002, 1, []string{"E2E", "abcdef"}},
		}
		rows, err = db.Query(context.Background(), "SELECT extract(epoch FROM time), exemplar_label_values, value FROM prom_data_exemplar."+metric_2+" ORDER BY time DESC")
		require.NoError(t, err)
		defer rows.Close()
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
	//todo: test
	withDB(t, *testDatabase, func(_ *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		insertablesIngested, metadataIngested, err := ingestor.IngestMetrics(context.Background(), newWriteRequestWithTs(exemplarTS_2))
		require.NoError(t, err)
		require.Equal(t, 12, int(insertablesIngested))
		require.Equal(t, 0, int(metadataIngested))
		// We do not check num of insertablesIngested and metadataIngested returned above from ingestor.IngestMetrics,
		// since the return will be 0, as they have already been ingested by TestExemplarIngestion.

		labelsReader := lreader.NewLabelsReader(pgxconn.NewPgxConn(db), cache.NewLabelsCache(cache.DefaultConfig), tenancy.NewNoopAuthorizer().ReadAuthorizer())
		r := querier.NewQuerier(
			pgxconn.NewPgxConn(db),
			cache.NewMetricCache(cache.DefaultConfig),
			labelsReader,
			cache.NewExemplarLabelsPosCache(cache.DefaultConfig), nil)
		queryable := query.NewQueryable(r, labelsReader)

		// Query all exemplars corresponding to metric_2 histogram.
		results, err := exemplar.QueryExemplar(context.Background(), metric_2, queryable, time.Unix(0, 0), time.Unix(1, 0))
		require.NoError(t, err)

		bSlice, err := json.Marshal(results)
		require.NoError(t, err)
		require.Equal(t,
			`[{"seriesLabels":{"__name__":"test_metric_2_histogram","job":"generator","le":"10"},"exemplars":[{"labels":{},"value":5,"timestamp":6},{"labels":{},"value":6,"timestamp":7}]}]`,
			string(bSlice))

		// Query all exemplars corresponding to metric_3 histogram.
		results, err = exemplar.QueryExemplar(context.Background(), metric_3, queryable, time.Unix(0, 0), time.Unix(1, 0))
		require.NoError(t, err)

		bSlice, err = json.Marshal(results)
		require.NoError(t, err)
		require.Equal(t,
			`[{"seriesLabels":{"__name__":"test_metric_3_total"},"exemplars":[{"labels":{},"value":5,"timestamp":6}]},{"seriesLabels":{"__name__":"test_metric_3_total","job":"generator"},"exemplars":[{"labels":{},"value":6,"timestamp":7}]},{"seriesLabels":{"__name__":"test_metric_3_total","job":"generator","le":"1"},"exemplars":[{"labels":{"job":"generator"},"value":4,"timestamp":5}]}]`,
			string(bSlice))

		// Query all exemplars of all metrics that we inserted.
		results, err = exemplar.QueryExemplar(context.Background(), `{__name__=~"test_metric_.*"}`, queryable, time.Unix(0, 0), time.Unix(1, 0))
		require.NoError(t, err)

		bSlice, err = json.Marshal(results)
		require.NoError(t, err)
		require.Equal(t,
			`[{"seriesLabels":{"__name__":"test_metric_1","job":"generator","le":"100"},"exemplars":[{"labels":{"instance":"localhost:9100"},"value":7,"timestamp":8}]},{"seriesLabels":{"__name__":"test_metric_2_histogram","job":"generator","le":"10"},"exemplars":[{"labels":{},"value":5,"timestamp":6},{"labels":{},"value":6,"timestamp":7}]},{"seriesLabels":{"__name__":"test_metric_3_total"},"exemplars":[{"labels":{},"value":5,"timestamp":6}]},{"seriesLabels":{"__name__":"test_metric_3_total","job":"generator"},"exemplars":[{"labels":{},"value":6,"timestamp":7}]},{"seriesLabels":{"__name__":"test_metric_3_total","job":"generator","le":"1"},"exemplars":[{"labels":{"job":"generator"},"value":4,"timestamp":5}]}]`,
			string(bSlice))
	})
}

func TestInsertExemplars(t *testing.T) {
	ts := []prompb.TimeSeries{
		{
			Samples: []prompb.Sample{{Timestamp: 0, Value: 1}, {Timestamp: 1, Value: 2}},
			Labels:  []prompb.Label{{Name: "__name__", Value: "test_metric"}},
		},
		{
			Samples: []prompb.Sample{{Timestamp: 2, Value: 3}, {Timestamp: 3, Value: 4}},
			Labels:  []prompb.Label{{Name: "__name__", Value: "test_metric"}},
		},
		{
			Samples: []prompb.Sample{{Timestamp: 4, Value: 5}, {Timestamp: 5, Value: 6}},
			Labels:  []prompb.Label{{Name: "__name__", Value: "test_metric"}},
		},
		{
			Samples: []prompb.Sample{{Timestamp: 6, Value: 7}, {Timestamp: 7, Value: 8}},
			Labels:  []prompb.Label{{Name: "__name__", Value: "test_metric"}},
		},
	}
	output := insertExemplars(ts, 2)
	require.Equal(t, nonNullTsExemplar(output), 2)
}

func nonNullTsExemplar(ts []prompb.TimeSeries) (num int) {
	for _, t := range ts {
		if t.Exemplars != nil {
			num++
		}
	}
	return
}
