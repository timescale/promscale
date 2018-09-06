package pgprometheus

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/prometheus-postgresql-adapter/log"
	"github.com/timescale/prometheus-postgresql-adapter/util"
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
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "cpu_usage",
			},
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "job",
				Value: "nginx",
			},
			{
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
	dbSetup(t)

	cfg := &Config{}
	ParseFlags(cfg)
	cfg.database = *database

	c := NewClient(cfg)

	sample := []*model.Sample{
		{
			Metric: model.Metric{
				model.MetricNameLabel: "test_metric",
				"label1":              "1",
			},
			Value:     123.1,
			Timestamp: 1234567,
		},
		{
			Metric: model.Metric{
				model.MetricNameLabel: "test_metric",
				"label1":              "1",
			},
			Value:     123.2,
			Timestamp: 1234568,
		},
		{
			Metric: model.Metric{
				model.MetricNameLabel: "test_metric",
			},
			Value:     123.2,
			Timestamp: 1234569,
		},
		{
			Metric: model.Metric{
				model.MetricNameLabel: "test_metric_2",
				"label1":              "1",
			},
			Value:     123.4,
			Timestamp: 1234570,
		},
	}

	c.Write(sample)

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost dbname=%s user=postgres sslmode=disable", *database))
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

func TestPgAdvisoryLock(t *testing.T) {
	db := dbSetup(t)
	lock, err := util.NewPgAdvisoryLock(1, db)
	if err != nil {
		t.Fatal(err)
	}
	if !lock.Locked() {
		t.Error("Couldn't obtain the lock")
	}

	newLock, err := util.NewPgAdvisoryLock(1, db)
	if err != nil {
		t.Fatal(err)
	}
	if newLock.Locked() {
		t.Error("Lock should have already been taken")
	}

	if err = lock.Release(); err != nil {
		t.Errorf("Failed to release a lock. Error: %v", err)
	}

	if lock.Locked() {
		t.Error("Should be unlocked after release")
	}

	newLock.TryLock()

	if !newLock.Locked() {
		t.Error("New lock should take over")
	}
}

func TestElector(t *testing.T) {
	db := dbSetup(t)
	lock1, err := util.NewPgAdvisoryLock(1, db)
	if err != nil {
		t.Error(err)
	}
	elector1 := util.NewElector(lock1, false)
	leader, _ := elector1.Elect()
	if !leader {
		t.Error("Failed to become a leader")
	}

	lock2, err := util.NewPgAdvisoryLock(1, db)
	if err != nil {
		t.Error(err)
	}
	elector2 := util.NewElector(lock2, false)
	leader, _ = elector2.Elect()
	if leader {
		t.Error("Shouldn't be possible")
	}

	elector1.Resign()
	leader, _ = elector2.Elect()
	if !leader {
		t.Error("Should become a leader")
	}

}

func dbSetup(t *testing.T) *sql.DB {
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
	return db
}
