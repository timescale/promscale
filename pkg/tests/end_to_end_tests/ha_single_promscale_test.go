// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/api/parser"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/util"

	"github.com/jackc/pgx/v4/pgxpool"
	promModel "github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/ha"
	haClient "github.com/timescale/promscale/pkg/ha/client"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
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
	withDB(t, testCase.db, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, testCase.db, "prom_writer")
		defer db.Close()
		ticker, writer, ing, err := prepareWriterWithHa(db, t)
		if err != nil {
			return
		}
		defer ing.Close()
		for _, step := range testCase.steps {
			t.Log(step.desc)
			req := generateHASamplesRequest(step.input)
			w := httptest.NewRecorder()
			writer.ServeHTTP(w, req)
			if !checkDbState(t, db, step.output) {
				return
			}

			if step.tickSyncRoutine {
				ticker.C <- time.Now()
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

var tCount int32

func prepareWriterWithHa(db *pgxpool.Pool, t testing.TB) (*util.ManualTicker, http.Handler, *ingestor.DBIngestor, error) {
	// function that returns time that is always in the future
	// and at least one hour apart from each other so
	// that calls to ha.Service.shouldTryToChangeLeader
	// depends only on the max time seen by an instance
	// and try_change_leader is called when we want
	tooFarInTheFutureNowFn := func() time.Time {
		return time.Now().Add(time.Duration(atomic.AddInt32(&tCount, 1)) * time.Hour)
	}

	// manual ticker, ticked when we want
	// to explicitly let the state sync routine run
	ticker := util.NewManualTicker(1)

	leaseClient := haClient.NewLeaseClient(pgxconn.NewPgxConn(db))
	haService := ha.NewServiceWith(leaseClient, ticker, tooFarInTheFutureNowFn)
	sigClose := make(chan struct{})
	sCache := cache.NewSeriesCache(cache.DefaultConfig, sigClose)
	dataParser := parser.NewParser()
	dataParser.AddPreprocessor(ha.NewFilter(haService))
	mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}

	ing, err := ingestor.NewPgxIngestor(pgxconn.NewPgxConn(db), mCache, sCache, nil, &ingestor.Cfg{
		InvertedLabelsCacheSize: cache.DefaultConfig.InvertedLabelsCacheSize,
	})
	if err != nil {
		t.Fatalf("could not create ingestor: %v", err)
	}
	api.InitMetrics()
	return ticker, api.Write(ing, dataParser, func(code string, duration, receivedSamples, receivedMetadata float64) {}), ing, err
}

func TestHALeaderChangeDueToInactivity(t *testing.T) {
	testCase := haTestCase{
		db: "ha_leader_change_due_to_inactivity",
		steps: []haTestCaseStep{
			{
				desc: "1 becomes leader",
				input: haTestInput{
					replica: "1",
					minT:    unixT(0),
					maxT:    unixT(0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 1,
					expectedMaxTimeInDb: unixT(0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: unixT(0),
						leaseUntil: unixT(60),
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
					expectedMaxTimeInDb: unixT(0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: unixT(0),
						leaseUntil: unixT(60),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 sends data with maxT after lease, triggers leader change -> insert",
				input: haTestInput{
					replica: "2",
					minT:    unixT(61),
					maxT:    unixT(61),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 2,
					expectedMaxTimeInDb: time.Unix(61, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(60, 0),
						leaseUntil: time.Unix(121, 0),
					},
				},
				tickSyncRoutine: false,
			}, {
				desc: "2 sends data again, is leader now, even without sync",
				input: haTestInput{
					replica: "2",
					minT:    unixT(62),
					maxT:    unixT(62),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 3,
					expectedMaxTimeInDb: unixT(62),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: unixT(60),
						// leader was updated with maxTimeSeen in previous step 61 + 60 (lease refresh)
						leaseUntil: unixT(121),
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
				desc: "2 sends data again, do a leader change, no need for sync routine",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(120, 0),
					maxT:    time.Unix(240, 0),
				},
				output: haTestOutput{
					// new lease will start where previous ended, we filter samples out of the lease
					//105 + (maxT - minT - (previousLeaseUntil - minT)) = 105 + 81 = 186
					expectedNumRowsInDb: 186,
					expectedMaxTimeInDb: time.Unix(240, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(160, 0),
						leaseUntil: time.Unix(300, 0),
					},
				},
				tickSyncRoutine: false,
			}, {
				desc: "sync routine didn't run, 1 sends data, inserted as backfill, no longer leader",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(106, 0),
					maxT:    time.Unix(140, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 221,
					expectedMaxTimeInDb: time.Unix(240, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(160, 0),
						leaseUntil: time.Unix(300, 0),
					},
				},
				tickSyncRoutine: false,
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
				desc: "2 sends data, out of lease, becomes leader, but will not tick",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(200, 0),
					maxT:    time.Unix(261, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 202,
					expectedMaxTimeInDb: time.Unix(261, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(260, 0),
						leaseUntil: time.Unix(321, 0),
					},
				},
				tickSyncRoutine: false,
			}, {
				desc: "1 sends, accepted, inserted as backfill",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(201, 0),
					maxT:    time.Unix(300, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 261,
					expectedMaxTimeInDb: time.Unix(261, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(260, 0),
						leaseUntil: time.Unix(321, 0),
					},
				},
				tickSyncRoutine: false,
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
		"SELECT leader_name, lease_start, lease_until FROM _prom_catalog.ha_leases WHERE cluster_name = $1",
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

func generateHASamplesRequest(input haTestInput) *http.Request {
	output := prompb.WriteRequest{}
	for now := input.minT; now.Before(input.maxT) || now.Equal(input.maxT); now = now.Add(time.Second) {
		ts := int64(promModel.TimeFromUnixNano(now.UnixNano()))
		sample := prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric"},
				{Name: ha.ClusterNameLabel, Value: "cluster"},
				{Name: ha.ReplicaNameLabel, Value: input.replica},
			},
			Samples: []prompb.Sample{{
				Value: rand.Float64(), Timestamp: ts,
			}},
		}
		output.Timeseries = append(output.Timeseries, sample)
	}
	data, _ := proto.Marshal(&output)
	body := strings.NewReader(string(snappy.Encode(nil, data)))
	req, _ := http.NewRequest("POST", "", body)

	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Add("Content-Type", "application/x-protobuf")
	req.Header.Add("X-Prometheus-Remote-Write-Version", "0.1.0")
	return req
}
