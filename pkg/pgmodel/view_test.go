package pgmodel

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/prometheus/prometheus/prompb"
)

func getViewRowCount(t *testing.T, db *pgxpool.Pool, view string, expected int) {
	var count int
	err := db.QueryRow(context.Background(), fmt.Sprintf("SELECT count(*) FROM %s", view)).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("unexpected view count: view %s, got %d expected %d", view, count, expected)
	}
}

func TestSQLView(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		metrics := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "no tags"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "reserved tags"},
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
					{Name: metricNameLabelName, Value: "long tags names"},
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
					{Name: metricNameLabelName, Value: "tag and tag id"},
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
					{Name: metricNameLabelName, Value: "add tag key"},
					{Name: "foo", Value: "bar"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 40, Value: 0.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "add tag key"},
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
					{Name: metricNameLabelName, Value: "cpu_usage"},
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
					{Name: metricNameLabelName, Value: "cpu_usage"},
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
					{Name: metricNameLabelName, Value: "cpu_total"},
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
					{Name: metricNameLabelName, Value: "cpu_total"},
					{Name: "namespace", Value: "dev"},
					{Name: "node", Value: "pinky"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 45, Value: 0.2},
				},
			},
		}

		ingestor := NewPgxIngestor(db)
		defer ingestor.Close()
		_, err := ingestor.Ingest(metrics)

		if err != nil {
			t.Fatal(err)
		}

		for _, ts := range metrics {
			name := ""
			for _, label := range ts.Labels {
				if label.Name == metricNameLabelName {
					name = label.Value
				}
			}
			seriesCount := 0
			pointCount := 0
			for _, ts2 := range metrics {
				for _, label := range ts2.Labels {
					if label.Name == metricNameLabelName && label.Value == name {
						seriesCount++
						pointCount += len(ts2.Samples)
					}
				}
			}
			getViewRowCount(t, db, "prom_series.\""+name+"\"", seriesCount)
			getViewRowCount(t, db, "prom_metric.\""+name+"\"", pointCount)
		}
	})
}
