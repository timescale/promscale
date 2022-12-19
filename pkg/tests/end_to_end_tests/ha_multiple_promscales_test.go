// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	setLeaseExplicitlySQL = `UPDATE _prom_catalog.ha_leases SET leader_name=$1, lease_start=$2, lease_until=$3
			WHERE cluster_name=$4`
)

// setLeaseExplicitly tries to set the ha lease to
// what is sent in the state argument. If nil state
// sent, no action taken.
// returns:
//   - an error if it couldn't set the state
func setLeaseExplicitly(db *pgxpool.Pool, state *leaseState) error {
	if state == nil {
		return nil
	}
	_, err := db.Exec(context.Background(), setLeaseExplicitlySQL, state.leader, state.leaseStart, state.leaseUntil, state.cluster)
	return err
}

func TestHALeaseChangedFromOutside(t *testing.T) {
	testCase := haTestCase{
		db: "ha_lease_change_from_outside",
		steps: []haTestCaseStep{
			{
				desc: "1 becomes leader; don't run sync; lease is changed from outside",
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
				tickSyncRoutine: false,
				explicitLease: &leaseState{
					cluster:    "cluster",
					leader:     "1",
					leaseStart: unixT(0),
					leaseUntil: unixT(100),
				},
			}, {
				desc: "1 tries to send data in lease, all is good; sync",
				input: haTestInput{
					replica: "1",
					minT:    unixT(1),
					maxT:    unixT(1),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 2,
					expectedMaxTimeInDb: unixT(1),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: unixT(0),
						leaseUntil: unixT(100),
					},
				},
				tickSyncRoutine: true,
			},
		}}
	runHATest(t, testCase)
}

func TestHABackfillOldData(t *testing.T) {
	testCase := haTestCase{
		db: "ha_backfill_old_data",
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
			},
			{
				desc: "2 becomes leader",
				input: haTestInput{
					replica: "2",
					minT:    unixT(100),
					maxT:    unixT(100),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 2,
					expectedMaxTimeInDb: unixT(100),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: unixT(60),
						leaseUntil: unixT(160),
					},
				},
			},
			{
				desc: "1 sends backfill data",
				input: haTestInput{
					replica: "1",
					minT:    unixT(50),
					maxT:    unixT(50),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 3,
					expectedMaxTimeInDb: unixT(100),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: unixT(60),
						leaseUntil: unixT(160),
					},
				},
			},
			{
				desc: "1 becomes leader again and sends backfill data",
				input: haTestInput{
					replica: "1",
					minT:    unixT(51),
					maxT:    unixT(161),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 14, // 51-59, 160, 161 = 11, + 3 existing = 14
					expectedMaxTimeInDb: unixT(161),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: unixT(160),
						leaseUntil: unixT(221),
					},
				},
			},
			{
				desc: "2 sends data, backfill ingested, in lease data discarded",
				input: haTestInput{
					replica: "2",
					minT:    unixT(150),
					maxT:    unixT(220),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 24, // 10 backfill samples, 150 - 159
					expectedMaxTimeInDb: unixT(161),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: unixT(160),
						leaseUntil: unixT(221),
					},
				},
			},
			{
				desc: "2 becomes leader",
				input: haTestInput{
					replica: "2",
					minT:    unixT(300),
					maxT:    unixT(300),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 25,
					expectedMaxTimeInDb: unixT(300),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: unixT(221),
						leaseUntil: unixT(360),
					},
				},
			},
			{
				desc: "1 sends backfill data from two different past leases",
				input: haTestInput{
					replica: "1",
					minT:    unixT(49),
					maxT:    unixT(162),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 27, // 49 and 162 are in past leases for 1 and are not already ingested
					expectedMaxTimeInDb: unixT(300),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: unixT(221),
						leaseUntil: unixT(360),
					},
				},
			},
			{
				desc: "1 sends backfill data from two different past leases and becomes leader with new data",
				input: haTestInput{
					replica: "1",
					minT:    unixT(48),
					maxT:    unixT(360),
				},
				output: haTestOutput{
					// 48, 163-220 are from past leases and not ingested,
					// 360 is from new leader lease
					// 59 + 27 = 86
					expectedNumRowsInDb: 87,
					expectedMaxTimeInDb: unixT(360),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: unixT(360),
						leaseUntil: unixT(420),
					},
				},
			},
		}}
	runHATest(t, testCase)
}

func TestHALeaderChangedFromOutside(t *testing.T) {
	testCase := haTestCase{
		db: "ha_leader_change_from_outside",
		steps: []haTestCaseStep{
			{
				desc: "1 becomes leader; don't run sync; leader is changed from outside",
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
				tickSyncRoutine: false,
				explicitLease: &leaseState{
					cluster:    "cluster",
					leader:     "2",
					leaseStart: unixT(60),
					leaseUntil: unixT(120),
				},
			}, {
				desc: "1 tries to send data in cached lease, all is good; sync",
				input: haTestInput{
					replica: "1",
					minT:    unixT(1),
					maxT:    unixT(1),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 2,
					expectedMaxTimeInDb: unixT(1),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: unixT(60),
						leaseUntil: unixT(120),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "1 is no longer leader, but is backfilling",
				input: haTestInput{
					replica: "1",
					minT:    unixT(2),
					maxT:    unixT(2),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 3,
					expectedMaxTimeInDb: unixT(2),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: unixT(60),
						leaseUntil: unixT(120),
					},
				},
				tickSyncRoutine: true,
			},
		}}
	runHATest(t, testCase)
}

func TestHALeaderChangedFromOutsideCacheNotAwareAtFirst(t *testing.T) {
	testCase := haTestCase{
		db: "ha_leader_change_from_outside",
		steps: []haTestCaseStep{
			{
				desc: "1 becomes leader; don't run sync; leader is changed from outside",
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
				tickSyncRoutine: false,
				explicitLease: &leaseState{
					cluster:    "cluster",
					leader:     "2",
					leaseStart: unixT(60),
					leaseUntil: unixT(120),
				},
			}, {
				desc: "2 tries to send data in lease, new lease is picked up because data falls on end of lease; sync",
				input: haTestInput{
					replica: "2",
					minT:    unixT(60),
					maxT:    unixT(60),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 2,
					expectedMaxTimeInDb: unixT(60),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: unixT(60),
						leaseUntil: unixT(120),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 sends data again, is leader now; state in db not updated synchronously, lease doesn't change",
				input: haTestInput{
					replica: "2",
					minT:    unixT(119),
					maxT:    unixT(119),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 3,
					expectedMaxTimeInDb: unixT(119),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: unixT(60),
						leaseUntil: unixT(120),
					},
				},
				tickSyncRoutine: false,
			}, {
				desc: "2 sends data again, after known lease; update state in db synchronously",
				input: haTestInput{
					replica: "2",
					minT:    unixT(120),
					maxT:    unixT(120),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 4,
					expectedMaxTimeInDb: unixT(120),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: unixT(60),
						leaseUntil: unixT(180),
					},
				},
				tickSyncRoutine: false,
			},
		}}
	runHATest(t, testCase)
}

func TestHALeaderChangedFromOutsideSyncUpdateTriggeredBecauseOfSamplesOutOfCachedLease(t *testing.T) {
	testCase := haTestCase{
		db: "ha_leader_change_from_outside",
		steps: []haTestCaseStep{
			{
				desc: "1 becomes leader; don't run sync; leader is changed from outside",
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
				tickSyncRoutine: false,
				explicitLease: &leaseState{
					cluster:    "cluster",
					leader:     "2",
					leaseStart: unixT(60),
					leaseUntil: unixT(120),
				},
			}, {
				desc: "2 tries to send data after lease, lease is cached from before but will be synchronously updated",
				input: haTestInput{
					replica: "2",
					minT:    unixT(61),
					maxT:    unixT(61),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 2,
					expectedMaxTimeInDb: unixT(61),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: unixT(60),
						leaseUntil: unixT(120),
					},
				},
				tickSyncRoutine: true,
			},
		}}
	runHATest(t, testCase)
}

func unixT(sec int64) time.Time {
	return time.Unix(sec, 0)
}
