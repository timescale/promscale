package end_to_end_tests

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"sync"
	"testing"
	"time"
)

type lockState struct {
	cluster, leader        string
	leaseStart, leaseUntil time.Time
}

func callCheckInsert(db *pgxpool.Pool, cluster, writer string, minT, maxT time.Time) (*lockState, error) {
	row := db.QueryRow(context.Background(), "SELECT * FROM check_insert($1,$2,$3,$4)", cluster, writer, minT, maxT)
	lock := lockState{}
	if err := row.Scan(&lock.cluster, &lock.leader, &lock.leaseStart, &lock.leaseUntil); err != nil {
		return nil, err
	}
	return &lock, nil
}

func checkLock(lock *lockState, wantedC, wantedL string, wantedLeaseStart, wantedLeaseUntil time.Time) bool {
	return lock.cluster == wantedC &&
		lock.leader == wantedL &&
		lock.leaseStart == wantedLeaseStart &&
		lock.leaseUntil == wantedLeaseUntil
}

func TestCheckInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const leaseTime = time.Minute
	const refreshTime = 10 * time.Second
	withDB(t, "ha_check_insert", func(db *pgxpool.Pool, t testing.TB) {
		// first check
		cluster := "c"
		writer := "w1"
		minT := time.Unix(1, 0)
		maxT := time.Unix(3, 0)
		lock, err := callCheckInsert(db, cluster, writer, minT, maxT)
		expectedLeaseUntil := time.Unix(3, 0).Add(leaseTime)
		if lock == nil {
			t.Fatal("error calling check_insert")
		}
		if !checkLock(lock, cluster, writer, minT, expectedLeaseUntil) {
			t.Fatal("first call to check insert didn't set lock properly")
		}

		// wrong leader ->
		writer = "w2"
		lock, err = callCheckInsert(db, cluster, writer, minT, maxT)
		leaderHasChanged := "ERROR: LEADER_HAS_CHANGED (SQLSTATE P0001)"
		if lock != nil || err.Error() != leaderHasChanged {
			t.Fatalf("expected leader changed error, got: %v", err)
		}

		// lease_start > min time
		writer = "w1"
		minT = time.Unix(0, 0)
		lock, err = callCheckInsert(db, cluster, writer, minT, maxT)
		if lock != nil || err.Error() != leaderHasChanged {
			t.Fatalf("expected leader changed error, got: %v", err)
		}

		// no update lease_until
		minT = time.Unix(1, 0)
		maxT = expectedLeaseUntil.Add(-refreshTime).Add(-time.Second)
		lock, err = callCheckInsert(db, cluster, writer, minT, maxT)
		if lock == nil || err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !checkLock(lock, cluster, writer, minT, expectedLeaseUntil) {
			t.Fatal("expected lock details to not change")
		}

		// update lease_until
		maxT = expectedLeaseUntil.Add(-refreshTime)
		lock, err = callCheckInsert(db, cluster, writer, minT, maxT)
		if lock == nil || err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !checkLock(lock, cluster, writer, minT, maxT.Add(leaseTime)) {
			t.Fatal("expected lock details to change")
		}
	})

}

func TestCheckInsertMultiCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const leaseTime = time.Minute
	const refreshTime = 10 * time.Second
	withDB(t, "ha_check_insert_multicluster", func(db *pgxpool.Pool, t testing.TB) {
		// first check
		cluster1 := "c1"
		cluster2 := "c2"
		writer := "w1"
		minT := time.Unix(1, 0)
		maxT := time.Unix(3, 0)
		lock, err := callCheckInsert(db, cluster1, writer, minT, maxT)
		expectedLeaseUntil := time.Unix(3, 0).Add(leaseTime)
		if lock == nil {
			t.Fatalf("error calling check_insert for first cluster: %v", err)
		}
		if !checkLock(lock, cluster1, writer, minT, expectedLeaseUntil) {
			t.Fatal("call to check insert didn't set lock properly")
		}

		// same writer instance different cluster
		writer = "w2"
		lock, err = callCheckInsert(db, cluster2, writer, minT, maxT)
		if lock == nil {
			t.Fatalf("error calling check_insert for second cluster: %v", err)
		}
		if !checkLock(lock, cluster2, writer, minT, expectedLeaseUntil) {
			t.Fatal("call to check insert didn't set lock properly")
		}
	})
}
func TestConcurrentCheckInsert(t *testing.T) {
	// 3 clusters with 3 writer instances
	// each writer calls check_insert from a separate routine
	// per cluster 1 routine will become leader, the others will
	// always get a LEADER_HAS_CHANGED
	clusters := []string{"c1", "c2", "c3"}
	writers := []string{"w1", "w2", "w3"}
	numCallsToCheck := 10
	timeBetweenCheck := 10 * time.Millisecond

	withDB(t, "ha_check_insert_concurrent", func(db *pgxpool.Pool, t testing.TB) {

		wg := sync.WaitGroup{}
		wg.Add(len(clusters) * len(writers))
		for _, c := range clusters {
			for _, w := range writers {
				writer := w
				cluster := c
				go func() {
					minT := time.Unix(0, 0)
					maxT := time.Unix(2, 0)
					firstLockRes, firstErr := callCheckInsert(db, cluster, writer, minT, maxT)
					for i := 0; i < numCallsToCheck; i++ {
						time.Sleep(timeBetweenCheck)
						minT = minT.Add(time.Second)
						maxT = maxT.Add(time.Second)
						lockRes, err := callCheckInsert(db, cluster, writer, minT, maxT)
						// this routine got an error on first try -> other routine is leader
						if firstErr != nil && err == nil {
							wg.Done()
							t.Fatal("non-leader routine acquired lock")
							return
						} else if lockRes == nil && firstLockRes != nil {
							wg.Done()
							t.Fatal("leader failed to acquire lock")
							return
						}
					}
					wg.Done()
				}()
			}
		}
		wg.Wait()
	})

}
