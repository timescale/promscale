// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

func getSingleSampleValue(t testing.TB, resp []*prompb.TimeSeries) float64 {
	if len(resp) != 1 {
		t.Fatal("Expect one timeseries")
	}
	samples := resp[0].GetSamples()
	if len(samples) != 1 {
		t.Fatal("Expect one sample")
	}
	return samples[0].GetValue()
}

func getBooleanSQLResult(t testing.TB, db *pgxpool.Pool, sql string, args ...interface{}) bool {
	var res *bool
	err := db.QueryRow(context.Background(), sql, args...).Scan(&res)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatalf("NULL found")
	}
	return *res
}

func TestSQLStaleNaN(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		metricName := "StaleMetric"
		metrics := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: metricName},
					{Name: "foo", Value: "bar"},
					{Name: "common", Value: "tag"},
					{Name: "empty", Value: ""},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 20, Value: math.Float64frombits(value.StaleNaN)},
					{Timestamp: 30, Value: math.NaN()},
					{Timestamp: 40, Value: 0.4},
				},
			},
		}

		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db))
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(metrics), ingstr.NewWriteRequest())

		if err != nil {
			t.Fatalf("unexpected error while ingesting test dataset: %s", err)
		}

		matchers := []*prompb.LabelMatcher{
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  model.MetricNameLabelName,
				Value: metricName,
			},
		}

		query := []struct {
			query      *prompb.Query
			isNaN      bool
			isStaleNaN bool
		}{
			{
				isStaleNaN: true,
				query: &prompb.Query{
					Matchers:         matchers,
					StartTimestampMs: 19,
					EndTimestampMs:   21,
				},
			},
			{
				isNaN: true,
				query: &prompb.Query{
					Matchers:         matchers,
					StartTimestampMs: 29,
					EndTimestampMs:   31,
				},
			},
			{
				query: &prompb.Query{
					Matchers:         matchers,
					StartTimestampMs: 39,
					EndTimestampMs:   41,
				},
			},
		}

		for _, c := range query {
			mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
			lCache := clockcache.WithMax(100)
			dbConn := pgxconn.NewPgxConn(db)
			labelsReader := model.NewLabelsReader(dbConn, lCache)
			r := querier.NewQuerier(dbConn, mCache, labelsReader)
			resp, err := r.Query(c.query)
			startMs := c.query.StartTimestampMs
			endMs := c.query.EndTimestampMs
			timeClause := "time >= 'epoch'::timestamptz + $1 AND time <= 'epoch'::timestamptz + $2"

			if err != nil {
				t.Fatalf("unexpected error while ingesting test dataset: %s", err)
			}
			answer := getSingleSampleValue(t, resp)
			isStaleNaN := getBooleanSQLResult(t, db,
				fmt.Sprintf(
					`SELECT is_stale_marker(value)
					 FROM prom_data."StaleMetric"
					 WHERE %s
				`, timeClause), time.Duration(int64(time.Millisecond)*startMs), time.Duration(int64(time.Millisecond)*endMs))
			isNormalNaN := getBooleanSQLResult(t, db,
				fmt.Sprintf(
					`SELECT is_normal_nan(value)
						 FROM prom_data."StaleMetric"
						 WHERE %s
					`, timeClause), time.Duration(int64(time.Millisecond)*startMs), time.Duration(int64(time.Millisecond)*endMs))
			if c.isStaleNaN {
				if !value.IsStaleNaN(answer) {
					t.Fatal("Expected stale NaN, got:", answer)
				}
				if !isStaleNaN {
					t.Fatal("Expected is_stale_marker to return true")
				}
			} else {
				if value.IsStaleNaN(answer) {
					t.Fatal("Got an unexpected stale NaN")
				}
				if isStaleNaN {
					t.Fatal("Expected is_stale_marker to return false")
				}
			}
			if c.isNaN {
				if math.Float64bits(answer) != value.NormalNaN {
					t.Fatal("Expected NaN, got:", answer)
				}
				if !isNormalNaN {
					t.Fatal("Expected is_normal_nan to return true")
				}
			} else {
				if math.Float64bits(answer) == value.NormalNaN {
					t.Fatal("Got an unexpected NaN")
				}
				if isNormalNaN {
					t.Fatal("Expected is_normal_nan to return false")
				}
			}
		}
	})
}
