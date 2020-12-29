// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"fmt"
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/ha/client"
	"github.com/timescale/promscale/pkg/ha/state"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/util"
	"golang.org/x/sync/semaphore"
)

const (
	haSyncerTimeInterval      = 15 * time.Second
	haLastWriteIntervalInSecs = 30
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
	doneChannel         chan bool
	doneWG              sync.WaitGroup
}

// NewHaService constructs a new HA service with the supplied
// database connection and the default sync interval.
// The sync interval determines how often the leases for
// all clusters are refreshed from the database.
func NewHAService(leaseClient client.LeaseClient) *Service {
	return NewHAServiceWith(leaseClient, util.NewTicker(haSyncerTimeInterval), time.Now)
}

// NewHaServiceWith constructs a new HA service with the leaseClient,
// ticker and a current time provide function.
// The ticker determines when the lease states for all clusters
// are refreshed from the database, the currentTimeFn determines the
// current time, used for deterministic tests.
func NewHAServiceWith(leaseClient client.LeaseClient, ticker util.Ticker, currentTimeFn func() time.Time) *Service {

	service := &Service{
		state:               &sync.Map{},
		leaseClient:         leaseClient,
		leaderChangeLocks:   &sync.Map{},
		syncTicker:          ticker,
		currentTimeProvider: currentTimeFn,
		doneChannel:         make(chan bool),
	}
	service.doneWG.Add(1)
	go service.haStateSyncer()
	return service
}

// haStateSyncer periodically synchronizes the in-memory
// lease states with latest values from the database and initiates
// a leader change if the conditions are met.
func (s *Service) haStateSyncer() {
	for {
		select {
		case <-s.doneChannel:
			s.doneWG.Done()
			break
		case <-s.syncTicker.Channel():
			s.state.Range(func(c, l interface{}) bool {
				cluster := fmt.Sprint(c)
				lease := l.(*state.Lease)
				stateBeforeUpdate := lease.Clone()
				err := lease.UpdateLease(s.leaseClient, stateBeforeUpdate.Leader, stateBeforeUpdate.LeaseStart, stateBeforeUpdate.MaxTimeSeenLeader)
				if err != nil {
					errMsg := fmt.Sprintf("failed to validate cluster %s state", cluster)
					log.Error("msg", errMsg, "err", err)
					return true
				}

				stateAfterUpdate := lease.Clone()
				if s.shouldTryToChangeLeader(stateAfterUpdate) {
					go s.tryChangeLeader(cluster, lease)
				}
				return true
			})
		}
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
		err = lease.UpdateLease(s.leaseClient, replicaName, minT, maxT)
		if err != nil {
			return false, time.Time{}, err
		}

		// on sync-up if notice leader has changed skip
		// ingestion replica prom instance
		if lease.GetLeader() != replicaName {
			return false, time.Time{}, nil
		}
	}

	// requesting replica is leader, allow
	return true, acceptedMinT, nil
}

func (s *Service) Close() {
	close(s.doneChannel)
	s.doneWG.Wait()
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

	if err := currentLease.TryChangeLeader(s.leaseClient); err != nil {
		log.Error("msg", "Couldn't set update from db to lease", "err", err)
		return
	}
}

func (s *Service) getLeaderChangeLock(cluster string) *semaphore.Weighted {
	// use a semaphore since we want a tryAcquire()
	lock, _ := s.leaderChangeLocks.LoadOrStore(cluster, semaphore.NewWeighted(1))
	return lock.(*semaphore.Weighted)
}

func (s *Service) shouldTryToChangeLeader(lease *state.LeaseView) bool {
	diff := s.currentTimeProvider().Sub(lease.RecentLeaderWriteTime)
	// check leaseUntil is after maxT received from samples or
	// recent leader write is not more than 30 secs older.
	if diff <= haLastWriteIntervalInSecs || lease.LeaseUntil.After(lease.MaxTimeSeen) {
		return false
	}
	return true
}
