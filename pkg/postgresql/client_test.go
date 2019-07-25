package pgprometheus

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/prometheus-postgresql-adapter/pkg/log"
	"github.com/timescale/prometheus-postgresql-adapter/pkg/util"
	"testing"
	"time"
)

var (
	database = flag.String("database", "", "database to run integration tests on")
)

func assertEqual(t *testing.T, s1 string, s2 string) {
	if s1 != s2 {
		t.Errorf("Assertion failure: expected %s, got %s", s1, s2)
	}
}

func init() {
	log.Init("debug")
}

func TestBuildCommand(t *testing.T) {
	c := &Client{
		cfg: &Config{
			table:                 "metrics",
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

func TestEscaping(t *testing.T) {
	c := &Client{
		cfg: &Config{
			table:                 "metrics",
			pgPrometheusNormalize: true,
		},
	}

	assertEqual(t, "foobar", escapeValue("foobar"))
	assertEqual(t, "foo''bar", escapeValue("foo'bar"))
	assertEqual(t, "foo''''bar", escapeValue("foo''bar"))
	assertEqual(t, `foo\bar`, escapeValue(`foo\bar`))

	q := &prompb.Query{
		StartTimestampMs: 0,
		EndTimestampMs:   20000,
		Matchers: []*prompb.LabelMatcher{
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "cpu'_usage",
			},
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "j'ob",
				Value: "ng'inx",
			},
			{
				Type:  prompb.LabelMatcher_RE,
				Name:  "ho'st",
				Value: "lo'cal.*",
			},
		},
	}

	cmd, err := c.buildCommand(q)

	if err != nil {
		t.Fatal(err)
	}
	assertEqual(t, `SELECT time, name, value, labels FROM metrics WHERE name = 'cpu''_usage' AND labels->>'ho''st' ~ '^lo''cal.*$' AND time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T00:00:20Z'  AND labels @> '{"j''ob":"ng''inx"}' ORDER BY time`, cmd)

	t.Log(cmd)
}

func TestWriteCommand(t *testing.T) {
	withDB(t, func(db *sql.DB, t *testing.T) {
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

		var cnt int
		err := c.DB.QueryRow("SELECT count(*) FROM metrics").Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}

		if cnt != 4 {
			t.Fatal("Wrong cnt: ", cnt)
		}
		c.Close()
	})
}

func TestPgAdvisoryLock(t *testing.T) {
	withDB(t, func(db *sql.DB, t *testing.T) {
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
	})
}

func TestElector(t *testing.T) {
	withDB(t, func(db *sql.DB, t *testing.T) {
		lock1, err := util.NewPgAdvisoryLock(2, db)
		if err != nil {
			t.Error(err)
		}
		elector1 := util.NewElector(lock1)
		leader, _ := elector1.BecomeLeader()
		if !leader {
			t.Error("Failed to become a leader")
		}

		lock2, err := util.NewPgAdvisoryLock(2, db)
		if err != nil {
			t.Error(err)
		}
		elector2 := util.NewElector(lock2)
		leader, _ = elector2.BecomeLeader()
		if leader {
			t.Error("Shouldn't be possible")
		}

		elector1.Resign()
		leader, _ = elector2.BecomeLeader()
		if !leader {
			t.Error("Should become a leader")
		}
	})
}

func TestPrometheusLivenessCheck(t *testing.T) {
	withDB(t, func(db *sql.DB, t *testing.T) {
		lock1, err := util.NewPgAdvisoryLock(3, db)
		if err != nil {
			t.Error(err)
		}
		elector := util.NewScheduledElector(lock1)
		leader, _ := elector.Elect()
		if !leader {
			t.Error("Failed to become a leader")
		}
		elector.PrometheusLivenessCheck(0, 0)
		leader, _ = lock1.IsLeader()
		if leader {
			t.Error("Shouldn't be a leader")
		}
		if !elector.IsPausedScheduledElection() {
			t.Error("Scheduled election should be paused")
		}
		elector.PrometheusLivenessCheck(time.Now().UnixNano(), time.Hour)
		if elector.IsPausedScheduledElection() {
			t.Error("Scheduled election shouldn't be paused anymore")
		}
	})
}

func withDB(t *testing.T, f func(db *sql.DB, t *testing.T)) {
	db := dbSetup(t)
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	f(db, t)
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
