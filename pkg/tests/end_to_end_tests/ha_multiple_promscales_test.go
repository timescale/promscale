// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	setLeaseExplicitlySQL = `UPDATE ha_leases SET leader_name=$1, lease_start=$2, lease_until=$3
			WHERE cluster_name=$4`
)

// setLeaseExplicitly tries to set the ha lease to
// what is sent in the state argument. If nil state
// sent, no action taken.
// returns:
//    - an error if it couldn't set the state
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
				desc: "1 is no longer leader, can't send data",
				input: haTestInput{
					replica: "1",
					minT:    unixT(2),
					maxT:    unixT(2),
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
				desc: "2 tries to send data in lease, lease is cached from before-> rejected; sync",
				input: haTestInput{
					replica: "2",
					minT:    unixT(60),
					maxT:    unixT(60),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 1,
					expectedMaxTimeInDb: unixT(0),
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
					expectedNumRowsInDb: 2,
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
					expectedNumRowsInDb: 3,
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
