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
				tickSyncRoutine: false,
				explicitLease: &leaseState{
					cluster:    "cluster",
					leader:     "1",
					leaseStart: time.Unix(0, 0),
					leaseUntil: time.Unix(100, 0),
				},
			}, {
				desc: "1 tries to send data in lease, all is good; sync",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(1, 0),
					maxT:    time.Unix(1, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 2,
					expectedMaxTimeInDb: time.Unix(1, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "1",
						leaseStart: time.Unix(0, 0),
						leaseUntil: time.Unix(100, 0),
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
				tickSyncRoutine: false,
				explicitLease: &leaseState{
					cluster:    "cluster",
					leader:     "2",
					leaseStart: time.Unix(60, 0),
					leaseUntil: time.Unix(120, 0),
				},
			}, {
				desc: "1 tries to send data in cached lease, all is good; sync",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(1, 0),
					maxT:    time.Unix(1, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 2,
					expectedMaxTimeInDb: time.Unix(1, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(60, 0),
						leaseUntil: time.Unix(120, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "1 is no longer leader, can't send data",
				input: haTestInput{
					replica: "1",
					minT:    time.Unix(2, 0),
					maxT:    time.Unix(2, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 2,
					expectedMaxTimeInDb: time.Unix(1, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(60, 0),
						leaseUntil: time.Unix(120, 0),
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
				tickSyncRoutine: false,
				explicitLease: &leaseState{
					cluster:    "cluster",
					leader:     "2",
					leaseStart: time.Unix(60, 0),
					leaseUntil: time.Unix(120, 0),
				},
			}, {
				desc: "2 tries to send data in lease, lease is cached from before-> rejected; sync",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(60, 0),
					maxT:    time.Unix(60, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 1,
					expectedMaxTimeInDb: time.Unix(0, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(60, 0),
						leaseUntil: time.Unix(120, 0),
					},
				},
				tickSyncRoutine: true,
			}, {
				desc: "2 sends data again, is leader now; state in db not updated synchronously, lease doesn't change",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(120, 0),
					maxT:    time.Unix(120, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 2,
					expectedMaxTimeInDb: time.Unix(120, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(60, 0),
						leaseUntil: time.Unix(120, 0),
					},
				},
				tickSyncRoutine: false,
			}, {
				desc: "2 sends data again, after known lease; update state in db synchronously",
				input: haTestInput{
					replica: "2",
					minT:    time.Unix(121, 0),
					maxT:    time.Unix(121, 0),
				},
				output: haTestOutput{
					expectedNumRowsInDb: 3,
					expectedMaxTimeInDb: time.Unix(121, 0),
					expectedLeaseStateInDb: leaseState{
						cluster:    "cluster",
						leader:     "2",
						leaseStart: time.Unix(60, 0),
						leaseUntil: time.Unix(181, 0),
					},
				},
				tickSyncRoutine: false,
			},
		}}
	runHATest(t, testCase)
}
