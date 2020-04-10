// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package util

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/timescale/timescale-prometheus/pkg/internal/testhelpers"
	"github.com/timescale/timescale-prometheus/pkg/log"
)

var (
	useDocker        = flag.Bool("use-docker", true, "start database using a docker container")
	database         = flag.String("database", "tmp_db_timescale_migrate_test", "database to run integration tests on")
	electionInterval = flag.Duration("election-interval", 5*time.Second, "Scheduled election interval")
)

func TestRestElection(t *testing.T) {
	http.DefaultServeMux = new(http.ServeMux)
	re := NewRestElection()
	if leader, _ := re.IsLeader(); leader {
		t.Error("Initially there is no leader")
	}
	if leader, _ := re.BecomeLeader(); !leader {
		t.Error("Failed to elect")
	}
	if leader, _ := re.IsLeader(); !leader {
		t.Error("Failed to elect")
	}
	err := re.Resign()
	if err != nil {
		t.Fatal(err)
	}
	if leader, _ := re.IsLeader(); leader {
		t.Error("Failed to resign")
	}
}

func TestRESTApi(t *testing.T) {
	http.DefaultServeMux = new(http.ServeMux)
	re := NewRestElection()
	becomeLeaderReq, err := http.NewRequest("PUT", "/admin/leader", bytes.NewReader([]byte("1")))
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	re.handleLeader().ServeHTTP(recorder, becomeLeaderReq)

	if recorder.Code != 200 {
		t.Error("Expected HTTP 200 Status Code")
	}
	if recorder.Body.String() != "true" {
		t.Error("Failed to become a leader")
	}

	leaderCheckReq, err := http.NewRequest("GET", "/admin/leader", nil)
	if err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	re.handleLeader().ServeHTTP(recorder, leaderCheckReq)
	if recorder.Body.String() != "true" {
		t.Error("Instance should be leader")
	}

	resignReq, err := http.NewRequest("PUT", "/admin/leader", bytes.NewReader([]byte("0")))
	if err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	re.handleLeader().ServeHTTP(recorder, resignReq)

	if recorder.Code != 200 {
		t.Error("Expected HTTP 200 Status Code")
	}
	if recorder.Body.String() != "true" {
		t.Error("Failed to resign")
	}
}

func TestPgAdvisoryLock(t *testing.T) {
	testhelpers.WithDB(t, *database, func(pool *pgxpool.Pool, t *testing.T, connectURL string) {
		lock, err := NewPgAdvisoryLock(1, connectURL)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		if !lock.Locked() {
			t.Error("Couldn't obtain the lock")
		}

		newLock, err := NewPgAdvisoryLock(1, connectURL)
		if err != nil {
			t.Fatal(err)
		}
		defer newLock.Close()
		if newLock.Locked() {
			t.Error("Lock should have already been taken")
		}

		if err = lock.Release(); err != nil {
			t.Errorf("Failed to release a lock. Error: %v", err)
		}

		if lock.Locked() {
			t.Error("Should be unlocked after release")
		}

		_, err = newLock.TryLock()
		if err != nil {
			t.Fatal(err)
		}

		if !newLock.Locked() {
			t.Error("New lock should take over")
		}
	})
}

func TestElector(t *testing.T) {
	testhelpers.WithDB(t, *database, func(pool *pgxpool.Pool, t *testing.T, connectURL string) {
		lock1, err := NewPgAdvisoryLock(2, connectURL)
		if err != nil {
			t.Error(err)
		}
		defer lock1.Close()
		elector1 := NewElector(lock1)
		leader, _ := elector1.BecomeLeader()
		if !leader {
			t.Error("Failed to become a leader")
		}

		lock2, err := NewPgAdvisoryLock(2, connectURL)
		if err != nil {
			t.Error(err)
		}
		defer lock2.Close()
		elector2 := NewElector(lock2)
		leader, _ = elector2.BecomeLeader()
		if leader {
			t.Error("Shouldn't be possible")
		}

		err = elector1.Resign()
		if err != nil {
			t.Fatal(err)
		}
		leader, _ = elector2.BecomeLeader()
		if !leader {
			t.Error("Should become a leader")
		}
	})
}

func TestPrometheusLivenessCheck(t *testing.T) {
	testhelpers.WithDB(t, *database, func(pool *pgxpool.Pool, t *testing.T, connectURL string) {
		lock1, err := NewPgAdvisoryLock(3, connectURL)
		if err != nil {
			t.Error(err)
		}
		defer lock1.Close()
		lock2, err := NewPgAdvisoryLock(3, connectURL)
		if err != nil {
			t.Error(err)
		}
		defer lock2.Close()
		elector1 := NewScheduledElector(lock1, *electionInterval)
		elector2 := NewScheduledElector(lock2, *electionInterval)
		leader1 := elector1.elect()
		if !leader1 {
			t.Error("Failed to become a leader")
		}
		leader2 := elector2.elect()
		if leader2 {
			t.Error("Two leaders")
		}
		elector1.PrometheusLivenessCheck(0, 0)
		leader2 = elector2.elect()
		if !leader2 {
			t.Error("Failed to become a leader after live fail")
		}
		leader1, _ = lock1.IsLeader()
		if leader1 {
			t.Error("Shouldn't be a leader")
		}
		if !elector1.isScheduledElectionPaused() {
			t.Error("Scheduled election should be paused")
		}
		elector1.PrometheusLivenessCheck(time.Now().UnixNano(), time.Hour)
		if elector1.isScheduledElectionPaused() {
			t.Error("Scheduled election shouldn't be paused anymore")
		}
	})
}
func TestMain(m *testing.M) {
	flag.Parse()
	err := log.Init("debug")
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if !testing.Short() && *useDocker {
		container, err := testhelpers.StartPGContainer(ctx)
		if err != nil {
			fmt.Println("Error setting up container", err)
			os.Exit(1)
		}
		defer func() {
			err := container.Terminate(ctx)
			if err != nil {
				panic(err)
			}
		}()
	}
	code := m.Run()
	os.Exit(code)
}
