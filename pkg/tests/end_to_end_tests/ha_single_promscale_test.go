// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	promModel "github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/ha"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

type haTestInput struct {
	replica string
	minT    time.Time
	maxT    time.Time
}

type haTestOutput struct {
	expectedNumRowsInDb    int
	expectedMaxTimeInDb    time.Time
	expectedLeaseStateInDb leaseState
}

type haTestCaseStep struct {
	desc  string
	input haTestInput
	// expected output - defines expected db state
	output haTestOutput
	// should ticker for sync routine be ticked after
	// db state has been checked
	tickSyncRoutine bool
	// if set lease in database will be explicitly set
	// to this state, after sync routine has been ticked
	explicitLease *leaseState
}

type haTestCase struct {
	db    string
	steps []haTestCaseStep
}

func runHATest(t *testing.T, testCase haTestCase) {
	withDB(t, testCase.db, func(db *pgxpool.Pool, t testing.TB) {
		ticker, ing, err := prepareIngestorWithHa(db, t)
		if err != nil {
			return
		}
		defer ing.Close()
		for _, step := range testCase.steps {
			t.Log(step.desc)
			samples := generateHASamples(step.input)
			rows, err := ing.Ingest(samples, ingestor.NewWriteRequest())
			t.Logf("Num rows ingested: %d\n", rows)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
				return
			}
			if !checkDbState(t, db, step.output) {
				return
			}

			if step.tickSyncRoutine {
				ticker.Tick()
				// sleep this routine so the sync routine has had time
				// to be scheduled and read the ticker
				// and the try_change_leader routine has also been
				// scheduled
				time.Sleep(time.Second)
			}

			if err := setLeaseExplicitly(db, step.explicitLease); err != nil {
				t.Fatalf("could not change lease state from outside: %v", err)
				return
			}

		}
	})
}

func prepareIngestorWithHa(db *pgxpool.Pool, t testing.TB) (*manualTicker, *ingestor.DBIngestor, error) {
	// manuel ticker, ticked when we want
	// to explicitly let the state sync routine run
	ticker := &manualTicker{c: make(chan time.Time, 1)}
	// function that returns time that is always in the future
	// so the calls to ha.Service.checkLeaseUntilAndLastWrite
	// depends only on the max time seen by an instance
	// and try_change_leader is called when we want
	tooFarInTheFutureNowFn := func() time.Time { return time.Now().Add(time.Hour) }
	haService, err := ha.NewHAServiceWith(pgxconn.NewPgxConn(db), ticker, tooFarInTheFutureNowFn)
	if err != nil {
		t.Fatalf("could not set up test: %v", err)
		return nil, nil, err
	}
	haParser := ha.NewHAParser(haService)
	cach := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
	ing, err := ingestor.NewPgxIngestor(pgxconn.NewPgxConn(db), cach, haParser, &ingestor.Cfg{})
	if err != nil {
		t.Fatalf("could not create ingestor: %v", err)
	}
	return ticker, ing, err
}

func TestHALeaderChangeDueToInactivity(t *testing.T) {
	testCase := haTestCase{
		db: "ha_leader_change_due_to_inactivity",
		steps: []haTestCaseStep{
			{
				desc: "1 becomes leader",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(0, 0),
					maxT:    time.Unix(0, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 1,
					expectedMaxTimeInDb: time.Unix(0, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(0, 0),
						leaseUntil: time.Unix(60, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 sends data in lease, no insert",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(0, 0),
					maxT:    time.Unix(0, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 1,
					expectedMaxTimeInDb: time.Unix(0, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(0, 0),
						leaseUntil: time.Unix(60, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 sends data after lease, no insert -> triggers leader change",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(61, 0),
					maxT:    time.Unix(61, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 1,
					expectedMaxTimeInDb: time.Unix(0, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(0, 0),
						leaseUntil: time.Unix(60, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 sends data again, is leader now",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(62, 0),
					maxT:    time.Unix(62, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 2,
					expectedMaxTimeInDb: time.Unix(62, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(60, 0),
						// leader was updated with maxTimeSeen in previous step 61 + 60 (lease refresh)
						leaseUntil: time.Unix(121, 0),
					},
				},
				tickSyncRoutine: true,
			},
		}}
	runHATest(t, testCase)
}

func TestHANoLeaderChange(t *testing.T) {
	testCase := haTestCase{
		db: "ha_no_leader_change",
		steps: []haTestCaseStep{
			{
				desc: "1 becomes leader",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(0, 0),
					maxT:    time.Unix(100, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 101,
					expectedMaxTimeInDb: time.Unix(100, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(0, 0),
						leaseUntil: time.Unix(160, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 shouldn't insert, data still in lease, no leader change trigger",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(0, 0),
					maxT:    time.Unix(120, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 101,
					expectedMaxTimeInDb: time.Unix(100, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(0, 0),
						leaseUntil: time.Unix(160, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "1 sends data again, updates lease",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(101, 0),
					maxT:    time.Unix(200, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 201,
					expectedMaxTimeInDb: time.Unix(200, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(0, 0),
						leaseUntil: time.Unix(260, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 sends data, in new lease, rejected, no trigger",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(120, 0),
					maxT:    time.Unix(240, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 201,
					expectedMaxTimeInDb: time.Unix(200, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(0, 0),
						leaseUntil: time.Unix(260, 0),
					},
				},
				tickSyncRoutine: true,
			},
		}}
	runHATest(t, testCase)
}

func TestHALeaderChangeDueToDrift(t *testing.T) {
	testCase := haTestCase{
		db: "ha_leader_change_due_to_drift",
		steps: []haTestCaseStep{
			{
				desc: "1 becomes leader",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(1, 0),
					maxT:    time.Unix(100, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 100,
					expectedMaxTimeInDb: time.Unix(100, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(1, 0),
						leaseUntil: time.Unix(160, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 sends data in lease, no insert, no trigger",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(1, 0),
					maxT:    time.Unix(120, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 100,
					expectedMaxTimeInDb: time.Unix(100, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(1, 0),
						leaseUntil: time.Unix(160, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "1 sends data again, doesn't update lease",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(101, 0),
					maxT:    time.Unix(105, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 105,
					expectedMaxTimeInDb: time.Unix(105, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(1, 0),
						leaseUntil: time.Unix(160, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 sends data again, rejected, trigger leader change",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(120, 0),
					maxT:    time.Unix(240, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 105,
					expectedMaxTimeInDb: time.Unix(105, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(1, 0),
						leaseUntil: time.Unix(160, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "1 sends data, rejected, no longer leader",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(106, 0),
					maxT:    time.Unix(140, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 105,
					expectedMaxTimeInDb: time.Unix(105, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(160, 0),
						leaseUntil: time.Unix(300, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 sends data, is accepted",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(241, 0),
					maxT:    time.Unix(360, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 225,
					expectedMaxTimeInDb: time.Unix(360, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(160, 0),
						leaseUntil: time.Unix(420, 0),
					},
				},
				tickSyncRoutine: true,
			},
		}}
	runHATest(t, testCase)
}

func TestHAMultipleChecksBetweenTicks(t *testing.T) {
	testCase := haTestCase{
		db: "ha_multiple_checks_between_ticks",
		steps: []haTestCaseStep{
			{
				desc: "1 becomes leader",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(1, 0),
					maxT:    time.Unix(100, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 100,
					expectedMaxTimeInDb: time.Unix(100, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(1, 0),
						leaseUntil: time.Unix(160, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 sends data in lease, no insert, no trigger",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(1, 0),
					maxT:    time.Unix(120, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 100,
					expectedMaxTimeInDb: time.Unix(100, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(1, 0),
						leaseUntil: time.Unix(160, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "1 sends data again, updates lease, but doesn't tick -> db has new lease",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(101, 0),
					maxT:    time.Unix(200, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 200,
					expectedMaxTimeInDb: time.Unix(200, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(1, 0),
						leaseUntil: time.Unix(260, 0),
					},
				},
				tickSyncRoutine: false,
			}, {
				desc: "2 sends data again, out of lease, becomes candidate leader, but will not tick",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(200, 0),
					maxT:    time.Unix(261, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 200,
					expectedMaxTimeInDb: time.Unix(200, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(1, 0),
						leaseUntil: time.Unix(260, 0),
					},
				},
				tickSyncRoutine: false,
			}, {
				desc: "1 sends, accepted, leader wasn't changed, out of lease -> update lease in db, no tick",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(201, 0),
					maxT:    time.Unix(300, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 300,
					expectedMaxTimeInDb: time.Unix(300, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(1, 0),
						leaseUntil: time.Unix(360, 0),
					},
				},
				tickSyncRoutine: false,
			}, {
				desc: "2 sends, lease updated without tick, rejected",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(241, 0),
					maxT:    time.Unix(360, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 300,
					expectedMaxTimeInDb: time.Unix(300, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(1, 0),
						leaseUntil: time.Unix(360, 0),
					},
				},
				tickSyncRoutine: true,
			},
		}}
	runHATest(t, testCase)
}

func checkDbState(t testing.TB, db *pgxpool.Pool, output haTestOutput) bool {
	row := db.QueryRow(context.Background(), "SELECT max(time), count(*) FROM metric")
	var maxTimeInDb time.Time
	var rowsInDb int
	if err := row.Scan(&maxTimeInDb, &rowsInDb); err != nil {
		t.Fatalf("could not check db state: %v", err)
		return false
	}
	if maxTimeInDb != output.expectedMaxTimeInDb {
		t.Fatalf("expected max time to be: %v; got: %v", output.expectedMaxTimeInDb, maxTimeInDb)
		return false
	}
	if rowsInDb != output.expectedNumRowsInDb {
		t.Fatalf("expected rows in db to be: %d; got: %d", output.expectedNumRowsInDb, rowsInDb)
		return false
	}
	return checkLeaseInDb(t, db, output.expectedLeaseStateInDb)
}

func checkLeaseInDb(t testing.TB, db *pgxpool.Pool, wantedLeaseState leaseState) bool {
	row := db.QueryRow(
		context.Background(),
		"SELECT leader_name, lease_start, lease_until FROM "+schema.Catalog+".ha_leases WHERE cluster_name = $1",
		wantedLeaseState.cluster,
	)
	var stateInDb leaseState
	stateInDb.cluster = wantedLeaseState.cluster
	if err := row.Scan(&stateInDb.leader, &stateInDb.leaseStart, &stateInDb.leaseUntil); err != nil {
		return false
	}

	if !reflect.DeepEqual(wantedLeaseState, stateInDb) {
		t.Fatalf("lease state in db unexpected\nwanted: %v;\ngot: %v", wantedLeaseState, stateInDb)
		return false
	}
	return true
}

func generateHASamples(input haTestInput) (output []prompb.TimeSeries) {
	for now := input.minT; now.Before(input.maxT) || now.Equal(input.maxT); now = now.Add(time.Second) {
		ts := int64(promModel.TimeFromUnixNano(now.UnixNano()))
		sample := prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric"},
				{Name: model.ClusterNameLabel, Value: "cluster"},
				{Name: model.ReplicaNameLabel, Value: input.replica},
			},
			Samples: []prompb.Sample{{
				Value: rand.Float64(), Timestamp: ts,
			}},
		}
		output = append(output, sample)
	}
	return output
}

type manualTicker struct {
	c chan time.Time
}

func (m *manualTicker) Tick() {
	m.c <- time.Now()
}
func (m *manualTicker) Wait() {
	<-m.c
}

func (m *manualTicker) Stop() {
	panic("not implemented")
}
