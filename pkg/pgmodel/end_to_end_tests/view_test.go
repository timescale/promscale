package end_to_end_tests

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/timescale/promscale/pkg/prompb"

	. "github.com/timescale/promscale/pkg/pgmodel"
)

func getViewRowCount(t testing.TB, db *pgxpool.Pool, view string, where string, expected int) {
	var count int
	err := db.QueryRow(context.Background(), fmt.Sprintf("SELECT count(*) FROM %s %s", view, where)).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("unexpected view count: view %s, where clause %s, got %d expected %d", view, where, count, expected)
	}
}

func TestSQLView(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		metrics := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "no tags"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "reserved tags"},
					{Name: "foo", Value: "bar"},
					{Name: "labels", Value: "val1"},
					{Name: "series_id", Value: "vaL2"},
					{Name: "time", Value: "val1"},
					{Name: "value", Value: "val3"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 40, Value: 0.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "long tags names"},
					{Name: "foo", Value: "bar"},
					{Name: strings.Repeat("long", 20) + "suffix1", Value: "val1"},
					{Name: strings.Repeat("long", 20) + "suffix2", Value: "val2"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 40, Value: 0.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "tag and tag id"},
					{Name: "foo", Value: "bar"},
					{Name: "foo_id", Value: "bar2"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 40, Value: 0.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "add tag key"},
					{Name: "foo", Value: "bar"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 40, Value: 0.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "add tag key"},
					{Name: "foo", Value: "bar"},
					{Name: "baz", Value: "bar2"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 40, Value: 0.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "cpu_usage"},
					{Name: "namespace", Value: "production"},
					{Name: "node", Value: "brain"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.5},
					{Timestamp: 40, Value: 0.6},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "cpu_usage"},
					{Name: "namespace", Value: "dev"},
					{Name: "node", Value: "pinky"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 45, Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "cpu_total"},
					{Name: "namespace", Value: "production"},
					{Name: "node", Value: "brain"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.5},
					{Timestamp: 40, Value: 0.6},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "cpu_total"},
					{Name: "namespace", Value: "dev"},
					{Name: "node", Value: "pinky"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 45, Value: 0.2},
				},
			},
		}

		ingestor, err := NewPgxIngestor(db)
		if err != nil {
			t.Fatal(err)
		}

		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(metrics), NewWriteRequest())

		if err != nil {
			t.Fatal(err)
		}

		for _, ts := range metrics {
			name := ""
			for _, label := range ts.Labels {
				if label.Name == MetricNameLabelName {
					name = label.Value
				}
			}
			seriesCount := 0
			pointCount := 0
			for _, ts2 := range metrics {
				for _, label := range ts2.Labels {
					if label.Name == MetricNameLabelName && label.Value == name {
						seriesCount++
						pointCount += len(ts2.Samples)
					}
				}
			}
			getViewRowCount(t, db, "prom_series.\""+name+"\"", "", seriesCount)
			getViewRowCount(t, db, "prom_metric.\""+name+"\"", "", pointCount)
		}
	})
}

func TestSQLViewSelectors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		metrics := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "cpu_usage"},
					{Name: "namespace", Value: "production"},
					{Name: "node", Value: "brain"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.5},
					{Timestamp: 40, Value: 0.6},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "cpu_usage"},
					{Name: "namespace", Value: "dev"},
					{Name: "node", Value: "pinky"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 45, Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "cpu_usage"},
					{Name: "namespace", Value: "dev"},
					{Name: "node", Value: "brain"},
					{Name: "new_tag", Value: "value"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 60, Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "cpu_total"},
					{Name: "namespace", Value: "production"},
					{Name: "node", Value: "brain"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.5},
					{Timestamp: 40, Value: 0.6},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "cpu_total"},
					{Name: "namespace", Value: "dev"},
					{Name: "node", Value: "pinky"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 45, Value: 0.2},
				},
			},
		}

		ingestor, err := NewPgxIngestor(db)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(metrics), NewWriteRequest())

		if err != nil {
			t.Fatal(err)
		}

		queries := []struct {
			where string
			rows  int
		}{
			{
				where: "WHERE labels ? ('namespace' == 'dev')",
				rows:  3,
			},
			{
				where: "WHERE labels ? ('namespace' !== 'dev')",
				rows:  2,
			},
			{
				where: "WHERE labels ? ('node' == 'dev')",
				rows:  0,
			},
			{
				where: "WHERE labels ? ('namespace' ==~ 'de.*')",
				rows:  3,
			},
			{
				where: "WHERE labels ? ('namespace' !=~ 'de.*')",
				rows:  2,
			},
			{
				where: "WHERE labels ? ('namespace' == 'dev') AND labels ? ('node' == 'brain')",
				rows:  1,
			},
			{
				where: `WHERE eq(labels, jsonb '{"namespace":"dev", "node":"brain", "new_tag":"value"}')`,
				rows:  1,
			},
			{
				where: `WHERE eq(labels, jsonb '{"namespace":"dev"}')`,
				rows:  0, //eq is not contain
			},
			{
				where: `WHERE labels @> jsonb '{"namespace":"dev"}'`,
				rows:  3,
			},
			{
				where: "WHERE labels ? ('new_tag' == 'value')",
				rows:  1,
			},
			{
				where: "WHERE labels ? ('new_tag' !== 'value')",
				rows:  4,
			},
			{
				where: "WHERE labels ? ('namespace' == 'not_exist')",
				rows:  0,
			},
			{
				where: "WHERE labels ? ('not_exist' == 'not_exist')",
				rows:  0,
			},
			{
				where: "WHERE labels ? ('namespace' !== 'not_exist')",
				rows:  5,
			},
			{
				where: "WHERE labels ? ('namespace' !== 'pinky')",
				rows:  5,
			},
			{
				where: "WHERE labels ? ('not_exist' !== 'not_exist')",
				rows:  5,
			},
			{
				where: "WHERE labels ? ('namespace' ==~ 'not_exist.*')",
				rows:  0,
			},
			{
				where: "WHERE labels ? ('not_exist' ==~ 'not_exist.*')",
				rows:  0,
			},
			{
				where: "WHERE labels ? ('namespace' !=~ 'not_exist.*')",
				rows:  5,
			},
			{
				where: "WHERE labels ? ('not_exist' !=~ 'not_exist.*')",
				rows:  5,
			},
		}

		for _, q := range queries {
			getViewRowCount(t, db, "prom_metric.\"cpu_usage\"", q.where, q.rows)
		}
	})
}
