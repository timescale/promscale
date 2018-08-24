package pgprometheus

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/prometheus-postgresql-adapter/log"
	"testing"
)

var (
	database = flag.String("database", "", "database to run integration tests on")
)

func init() {
	log.Init("debug")
}

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
	if len(*database) == 0 {
		t.Skip()
	}

	db, err := sql.Open("postgres", "host=localhost user=postgres sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", *database))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", *database))
	if err != nil {
		t.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	cfg := &Config{}
	ParseFlags(cfg)
	cfg.database = *database

	c := NewClient(cfg)

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

	db, err = sql.Open("postgres", fmt.Sprintf("host=localhost dbname=%s user=postgres sslmode=disable", *database))
	if err != nil {
		t.Fatal(err)
	}

	var cnt int
	err = db.QueryRow("SELECT count(*) FROM metrics").Scan(&cnt)
	if err != nil {
		t.Fatal(err)
	}

	if cnt != 4 {
		t.Fatal("Wrong cnt: ", cnt)
	}
}
