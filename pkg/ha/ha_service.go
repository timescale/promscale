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
)

const (
	haSyncerTimeInterval      = 15 * time.Second
	haLastWriteIntervalInSecs = 30
	tryLeaderChangeErrFmt     = "failed to attempt leader change for cluster %s"
	failedToUpdateLeaseErrFmt = "failed to update lease for cluster %s"
)

// actionToTake is an enumeration used to signal how the CheckLease
// method should continue
type actionToTake int

const (
	// deny the insert
	deny actionToTake = iota + 1
	// request a synchronous lease update from the db
	doSync
	// try to change leader
	tryChangeLeader
	// allow the insert to happen
	allow
)

// Service contains the lease state for all prometheus clusters
// and logic for determining if a specific sample should
// be allowed to be inserted. Also it keeps the lease state
// up to date by periodically refreshing it from the database.
type Service struct {
	state               *sync.Map
	leaseClient         client.LeaseClient
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
				stateAfterUpdate, err := lease.UpdateLease(
					s.leaseClient,
					stateBeforeUpdate.Leader,
					stateBeforeUpdate.LeaseStart,
					stateBeforeUpdate.MaxTimeSeenLeader,
				)
				if err != nil {
					errMsg := fmt.Sprintf(failedToUpdateLeaseErrFmt, cluster)
					log.Error("msg", errMsg, "err", err)
					return true
				}

				if s.shouldTryToChangeLeader(stateAfterUpdate) {
					if _, err := lease.TryChangeLeader(s.leaseClient); err != nil {
						errMsg := fmt.Sprintf(tryLeaderChangeErrFmt, cluster)
						log.Error("msg", errMsg, "err", err)
					}
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
		errMsg := fmt.Sprintf("error trying to get lease for cluster %s", clusterName)
		log.Error("msg", errMsg, "err", err)
		return false, time.Time{}, err
	}
	leaseView := lease.Clone()
	whatToDo := s.determineCourseOfAction(leaseView, replicaName, minT, maxT)
	switch whatToDo {
	case deny:
		return false, time.Time{}, err
	case doSync:
		leaseView, err = lease.UpdateLease(s.leaseClient, replicaName, minT, maxT)
		if err != nil {
			errMsg := fmt.Sprintf(failedToUpdateLeaseErrFmt, clusterName)
			log.Error("msg", errMsg, "err", err)
			return false, time.Time{}, err
		}

		if leaseView.Leader != replicaName {
			return false, time.Time{}, nil
		}
	case tryChangeLeader:
		leaseView, err := lease.TryChangeLeader(s.leaseClient)
		if err != nil {
			errMsg := fmt.Sprintf(tryLeaderChangeErrFmt, clusterName)
			log.Error("msg", errMsg, "err", err)
			return false, time.Time{}, err
		}
		if leaseView.Leader != replicaName {
			return false, time.Time{}, nil
		}
	}

	acceptedMinT = minT
	if minT.Before(leaseView.LeaseStart) {
		acceptedMinT = leaseView.LeaseStart
	}

	// requesting replica is leader, allow
	return true, acceptedMinT, nil
}

func (s *Service) Close() {
	close(s.doneChannel)
	s.doneWG.Wait()
}

func (s *Service) determineCourseOfAction(leaseView *state.LeaseView, replicaName string, minT, maxT time.Time) actionToTake {
	if replicaName == leaseView.Leader {
		if !maxT.Before(leaseView.LeaseUntil) {
			return doSync
		}
		return allow
	}

	if minT.After(leaseView.LeaseUntil) {
		if s.shouldTryToChangeLeader(leaseView) {
			return tryChangeLeader
		}
		return doSync
	} else {
		return deny
	}
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

func (s *Service) shouldTryToChangeLeader(lease *state.LeaseView) bool {
	diff := s.currentTimeProvider().Sub(lease.RecentLeaderWriteTime)
	// check leaseUntil is after maxT received from samples or
	// recent leader write is not more than 30 secs older.
	if diff <= haLastWriteIntervalInSecs || lease.LeaseUntil.After(lease.MaxTimeSeen) {
		return false
	}
	return true
}
