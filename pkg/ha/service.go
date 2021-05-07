// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"context"
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
	tryLeaderChangeErrFmt     = "failed to attempt leader change for cluster %s"
	failedToUpdateLeaseErrFmt = "failed to update lease for cluster %s"
)

var ErrNoLeasesInRange = fmt.Errorf("no valid leases in range found")

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

// NewService constructs a new HA lease service with the supplied
// database connection and the default sync interval.
// The sync interval determines how often the leases for
// all clusters are refreshed from the database.
func NewService(leaseClient client.LeaseClient) *Service {
	return NewServiceWith(leaseClient, util.NewTicker(haSyncerTimeInterval), time.Now)
}

// NewServiceWith constructs a new HA lease service with the leaseClient,
// ticker and a current time provide function.
// The ticker determines when the lease states for all clusters
// are refreshed from the database, the currentTimeFn determines the
// current time, used for deterministic tests.
func NewServiceWith(leaseClient client.LeaseClient, ticker util.Ticker, currentTimeFn func() time.Time) *Service {

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
				if err := lease.RefreshLease(); err != nil {
					errMsg := fmt.Sprintf(failedToUpdateLeaseErrFmt, cluster)
					log.Error("msg", errMsg, "err", err)
					return true
				}

				if err := lease.TryChangeLeader(s.currentTimeProvider()); err != nil {
					errMsg := fmt.Sprintf(tryLeaderChangeErrFmt, cluster)
					log.Error("msg", errMsg, "err", err)
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

	return lease.ValidateSamplesInfo(replicaName, minT, maxT, s.currentTimeProvider())
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

func (s *Service) GetBackfillLeaseRange(start, end time.Time, cluster string, replica string) (time.Time, time.Time, error) {
	state, err := s.leaseClient.GetPastLeaseInfo(context.Background(), cluster, replica, start, end)
	if err == client.ErrNoPastLease {
		err = ErrNoLeasesInRange
	}
	return state.LeaseStart, state.LeaseUntil, err
}
