package end_to_end_tests

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"sync"
	"testing"
	"time"
)

func callTryChangeLeader(db *pgxpool.Pool, cluster, writer string, maxT time.Time) (*leaseState, error) {
	row := db.QueryRow(context.Background(), "SELECT * FROM "+schema.Catalog+".try_change_leader($1,$2,$3)", cluster, writer, maxT)
	lock := leaseState{}
	if err := row.Scan(&lock.cluster, &lock.leader, &lock.leaseStart, &lock.leaseUntil); err != nil {
		return nil, err
	}
	return &lock, nil
}

func TestTryChangeLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	withDB(t, "ha_try_change_leader", func(db *pgxpool.Pool, t testing.TB) {
		cluster := "c"
		originalWriter := "w1"
		minT := time.Unix(1, 0)
		maxT := time.Unix(3, 0)

		// try change when no leader exists -> null values returned
		_, err := callTryChangeLeader(db, cluster, originalWriter, maxT)
		if err == nil {
			t.Fatal("unexpected lack of error")
			return
		}

		// set leader to w1
		lock, err := callCheckInsert(db, cluster, originalWriter, minT, maxT)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// try to change with invalid max time
		falseNewWriter := "w2"
		lock, err = callTryChangeLeader(db, cluster, falseNewWriter, lock.leaseUntil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if lock.leader != originalWriter {
			t.Fatalf("leader changed unexpectedly, expected: %v; got: %v", originalWriter, lock.leader)
			return
		}

		// try change with same leader, newer maxT -> lease interval change
		preChangeLeaseUntil := lock.leaseUntil
		lock, err = callTryChangeLeader(db, cluster, originalWriter, lock.leaseUntil.Add(time.Second))
		if err != nil || lock.leaseStart != preChangeLeaseUntil || !lock.leaseUntil.After(preChangeLeaseUntil) {
			t.Fatal("lease not updated properly")
			return
		}

		// new leader, newer maxT
		newLeader := "w2"
		preChangeLeaseUntil = lock.leaseUntil
		lock, err = callTryChangeLeader(db, cluster, newLeader, lock.leaseUntil.Add(time.Second))
		if err != nil || lock.leader != newLeader || lock.leaseStart != preChangeLeaseUntil || !lock.leaseUntil.After(preChangeLeaseUntil) {
			t.Fatal("lock not updated properly")
			return
		}
	})
}

func TestConcurrentTryChangeLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	writers := []string{"w1", "w2", "w3"}
	numCallsToCheck := 10
	timeBetweenCheck := 10 * time.Millisecond

	withDB(t, "ha_try_change_leader_concurrent", func(db *pgxpool.Pool, t testing.TB) {

		wg := sync.WaitGroup{}
		wg.Add(len(writers))
		minT := time.Unix(0, 0)
		maxT := time.Unix(2, 0)
		firstLockRes, _ := callCheckInsert(db, "c", writers[0], minT, maxT)
		for _, w := range writers {
			writer := w
			go func() {
				preChangeLeaseStart := firstLockRes.leaseStart
				preChangeLeaseUntil := firstLockRes.leaseUntil

				for i := 0; i < numCallsToCheck; i++ {
					lock, err := callTryChangeLeader(db, "c", writer, preChangeLeaseUntil)
					if lock == nil || err != nil {
						wg.Done()
						t.Fatalf("did not receive lock info, lock: %v; err: %v", lock, err)
						return
					}

					if lock.leaseStart.Before(preChangeLeaseStart) || lock.leaseUntil.Before(preChangeLeaseUntil) {
						wg.Done()
						t.Fatalf("lock lease interval went backwards")
						return
					}
					time.Sleep(timeBetweenCheck)
					preChangeLeaseStart = lock.leaseStart
					preChangeLeaseUntil = lock.leaseUntil
					preChangeLeaseUntil = preChangeLeaseUntil.Add(time.Second)
				}
				wg.Done()
			}()
		}
		wg.Wait()
	})
}
