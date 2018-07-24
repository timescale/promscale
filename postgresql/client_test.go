package pgprometheus

import (
	"database/sql"
	"flag"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

var (
	database = flag.Bool("database", false, "run database integration tests")
)

func TestBuildCommand(t *testing.T) {
	c := &Client{
		cfg: &Config{
			table: "metrics",
			pgPrometheusNormalize: true,
		},
	}

	q := &prompb.Query{
		StartTimestampMs: 0,
		EndTimestampMs:   20000,
		Matchers: []*prompb.LabelMatcher{
			&prompb.LabelMatcher{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "cpu_usage",
			},
			&prompb.LabelMatcher{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "job",
				Value: "nginx",
			},
			&prompb.LabelMatcher{
				Type:  prompb.LabelMatcher_RE,
				Name:  "host",
				Value: "local.*",
			},
		},
	}

	cmd, err := c.buildCommand(q)

	if err != nil {
		t.Fatal(err)
	}

	t.Log(cmd)
}

func TestWriteCommand(t *testing.T) {
	flag.Parse()
	if !*database {
		t.Skip()
	}

	db, err := sql.Open("postgres", "host=localhost user=postgres sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("DROP DATABASE IF EXISTS metrics_test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("CREATE DATABASE metrics_test")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	db, err = sql.Open("postgres", "host=localhost user=postgres dbname=metrics_test sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	c := &Client{
		db: db,
		cfg: &Config{
			table:                 "metrics",
			copyTable:             "metrics_copy",
			pgPrometheusNormalize: true,
		},
	}

	c.setupPgPrometheus()

	sample := []*model.Sample{
		&model.Sample{
			Metric: model.Metric{
				model.MetricNameLabel: "test_metric",
				"label1":              "1",
			},
			Value:     123.1,
			Timestamp: 1234567,
		},
		&model.Sample{
			Metric: model.Metric{
				model.MetricNameLabel: "test_metric",
				"label1":              "1",
			},
			Value:     123.2,
			Timestamp: 1234568,
		},
		&model.Sample{
			Metric: model.Metric{
				model.MetricNameLabel: "test_metric",
			},
			Value:     123.2,
			Timestamp: 1234569,
		},
		&model.Sample{
			Metric: model.Metric{
				model.MetricNameLabel: "test_metric_2",
				"label1":              "1",
			},
			Value:     123.4,
			Timestamp: 1234570,
		},
	}

	c.Write(sample)

	var cnt int
	err = db.QueryRow("SELECT count(*) FROM metrics").Scan(&cnt)
	if err != nil {
		t.Fatal(err)
	}

	if cnt != 4 {
		t.Fatal("Wrong cnt: ", cnt)
	}
}
