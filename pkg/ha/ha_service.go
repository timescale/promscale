// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"context"
	"fmt"
	"github.com/timescale/promscale/pkg/ha/client"
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/ha/state"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/util"
	"golang.org/x/sync/semaphore"
)

const (
	backOffDurationOnLeaderChange = 10 * time.Second
	haSyncerTimeInterval          = 15 * time.Second
	haLastWriteIntervalInSecs     = 30
)

// Service contains the lease state for all prometheus clusters
// and logic for determining if a specific sample should
// be allowed to be inserted. Also it keeps the lease state
// up to date by periodically refreshing it from the database.
type Service struct {
	state               *sync.Map
	leaseClient         client.LeaseClient
	leaderChangeLocks   *sync.Map
	syncTicker          util.Ticker
	currentTimeProvider func() time.Time
}

// NewHaService constructs a new HA service with the supplied
// database connection and the default sync interval.
// The sync interval determines how often the leases for
// all clusters are refreshed from the database.
func NewHAService(dbClient pgxconn.PgxConn) *Service {
	return NewHAServiceWith(dbClient, util.NewTicker(haSyncerTimeInterval), time.Now)
}

// NewHaServiceWith constructs a new HA service with the supplied
// database connection, ticker and a current time provide function.
// The ticker determines when the lease states for all clusters
// are refreshed from the database, the currentTimeFn determines the
// current time, used for deterministic tests.
func NewHAServiceWith(dbClient pgxconn.PgxConn, ticker util.Ticker, currentTimeFn func() time.Time) *Service {
	lockClient := client.NewHaLockClient(dbClient)

	service := &Service{
		state:               &sync.Map{},
		leaseClient:         lockClient,
		leaderChangeLocks:   &sync.Map{},
		syncTicker:          ticker,
		currentTimeProvider: currentTimeFn,
	}
	go service.haStateSyncer()
	return service
}

// haStateSyncer periodically synchronizes the in-memory
// lease states with latest values from the database and initiates
// a leader change if the conditions are met.
func (s *Service) haStateSyncer() {
	for {
		s.syncTicker.Wait()
		s.state.Range(func(c, l interface{}) bool {
			cluster := fmt.Sprint(c)
			lease := l.(*state.Lease)
			stateBeforeUpdate := lease.Clone()
			err := lease.UpdateFromDB(s.leaseClient, stateBeforeUpdate.Leader, stateBeforeUpdate.LeaseStart, stateBeforeUpdate.MaxTimeSeenLeader)
			if err != nil {
				errMsg := fmt.Sprintf("failed to validate cluster %s state", cluster)
				log.Error("msg", errMsg, "err", err)
				return true
			}

			stateAfterUpdate := lease.Clone()
			if ok := s.shouldTryToChangeLeader(stateAfterUpdate); !ok {
				go s.tryChangeLeader(cluster, lease)
			}
			return true
		})
	}
}

// CheckLease verifies the samples are from prom leader & in an expected time range.
//	An instance is selected a leader for a specific time range, which is expanded as
//	newer samples come in from that leader, but samples before the granted lease
//	are supposed to be dropped.
func (s *Service) CheckLease(minT, maxT time.Time, clusterName, replicaName string) (
	allowInsert bool, acceptedMinT time.Time, err error,
) {
	lease, err := s.getLocalClusterLease(clusterName, replicaName, minT, maxT)
	if err != nil {
		return false, time.Time{}, err
	}
	leaseView := lease.Clone()
	if replicaName != leaseView.Leader {
		return false, time.Time{}, nil
	}

	acceptedMinT = minT
	if minT.Before(leaseView.LeaseStart) {
		acceptedMinT = leaseView.LeaseStart
	}

	if !maxT.Before(leaseView.LeaseUntil) {
		err = lease.UpdateFromDB(s.leaseClient, replicaName, minT, maxT)
		if err != nil {
			return false, time.Time{}, err
		}

		// on sync-up if notice leader has changed skip
		// ingestion replica prom instance
		if lease.SafeGetLeader() != replicaName {
			return false, time.Time{}, nil
		}
	}

	// requesting replica is leader, allow
	return true, acceptedMinT, nil
}

func (s *Service) getLocalClusterLease(clusterName, replicaName string, minT, maxT time.Time) (*state.Lease, error) {
	currentTime := s.currentTimeProvider()
	l, ok := s.state.Load(clusterName)
	if ok {
		lease := l.(*state.Lease)
		lease.UpdateMaxSeenTime(replicaName, maxT, currentTime)
		return lease, nil
	}
	newLease, err := state.NewLease(s.leaseClient, clusterName, replicaName, minT, maxT, currentTime)
	if err != nil {
		return nil, err
	}
	l, _ = s.state.LoadOrStore(clusterName, newLease)
	newLease = l.(*state.Lease)
	return newLease, nil
}

func (s *Service) tryChangeLeader(cluster string, currentLease *state.Lease) {
	clusterLock := s.getLeaderChangeLock(cluster)
	ok := clusterLock.TryAcquire(1)
	if !ok {
		// change leader already in progress
		return
	}
	defer clusterLock.Release(1)

	for {
		stateView := currentLease.Clone()
		if ok := s.shouldTryToChangeLeader(stateView); ok {
			return
		}
		leaseState, err := s.leaseClient.TryChangeLeader(
			context.Background(), cluster, stateView.MaxTimeInstance, stateView.MaxTimeSeen,
		)
		if err != nil {
			log.Error("msg", "Couldn't change leader", "err", err)
			return
		}

		err = currentLease.SetUpdateFromDB(leaseState)
		if err != nil {
			log.Error("msg", "Couldn't set update from db to lease", "err", err)
			return
		}
		if leaseState.Leader != stateView.Leader {
			// leader changed
			return
		}
		// leader didn't change, wait a bit and try again
		time.Sleep(backOffDurationOnLeaderChange)
	}
}

func (s *Service) loadState(cluster string) (*state.Lease, error) {
	l, ok := s.state.Load(cluster)
	if !ok {
		return nil, fmt.Errorf("couldn't load %s cluster state from ha service", cluster)
	}
	ss := l.(*state.Lease)
	return ss, nil
}

func (s *Service) getLeaderChangeLock(cluster string) *semaphore.Weighted {
	lock, _ := s.leaderChangeLocks.LoadOrStore(cluster, semaphore.NewWeighted(1))
	return lock.(*semaphore.Weighted)
}

func (s *Service) shouldTryToChangeLeader(state *state.LeaseView) bool {
	diff := s.currentTimeProvider().Sub(state.RecentLeaderWriteTime)
	// check leaseUntil is after maxT received from samples or
	// recent leader write is not more than 30 secs older.
	if state.LeaseUntil.After(state.MaxTimeSeen) || diff <= haLastWriteIntervalInSecs {
		return true
	}
	return false
}
