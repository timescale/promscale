package ha

import (
	"context"
	"fmt"
	"golang.org/x/sync/semaphore"
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const backOffDurationOnLeaderChange = 10 * time.Second

type Service struct {
	state             *State
	lockClient        haLockClient
	leaseTimeout      time.Duration
	leaseRefresh      time.Duration
	_leaderChangeLock *semaphore.Weighted
}

func NewHAService(dbClient pgxconn.PgxConn) (*Service, error) {
	lockClient := newHaLockClient(dbClient)
	timeout, refresh, err := lockClient.readLeaseSettings(context.Background())
	if err != nil {
		return nil, fmt.Errorf("could not create HA state: %#v", err)
	}
	return &Service{
		state: &State{
			_mu: sync.RWMutex{},
		},
		lockClient:   lockClient,
		leaseTimeout: timeout,
		leaseRefresh: refresh,
	}, nil
}

// checkInsert verifies the samples are from prom leader & in expected time range.
// 	returns true if the insert is allowed
func (h *Service) checkInsert(minT, maxT time.Time, clusterName, replicaName string) (bool, error) {
	// TODO use proper context
	lockState, err := h.lockClient.checkInsert(context.Background(), clusterName, replicaName, minT, maxT)

	// unknown error while checking lock state
	if err != nil && err != leaderHasChanged {
		return false, fmt.Errorf("could not check ha lock state: %#v", err)
	}

	// leader changed
	if err == leaderHasChanged {
		// read latest lock state
		lockState, err = h.lockClient.readLockState(context.Background(), clusterName)
		// couldn't get latest lock state
		if err != nil {
			return false, fmt.Errorf("could not check ha lock state: %#v", err)
		}
		// update state asynchronously
		go h.state.update(lockState, replicaName, maxT)
		go h.tryChangeLeader()

		if lockState.leader == replicaName && !minT.Before(lockState.leaseStart) {
			return true, nil
		} else {

			return false, nil
		}
	}

	// requesting replica is leader, allow
	if err == nil && replicaName == lockState.leader && !minT.Before(lockState.leaseStart) {
		go h.state.update(lockState, replicaName, maxT)
		return true, nil
	}

	// replica is not leader or timestamps out of lease range, ignore them
	return false, nil
}

func (h *Service) tryChangeLeader() {
	ok := h._leaderChangeLock.TryAcquire(1)
	if !ok {
		// change leader already in progress
		return
	}
	defer h._leaderChangeLock.Release(1)

	for {
		stateView := h.state.clone()
		lockState, err := h.lockClient.readLockState(context.Background(), stateView.cluster)
		if err != nil {
			log.Error("msg", "Couldn't check lock state", "err", err)
			return
		}
		if stateView.leader == lockState.leader || !lockState.leaseUntil.After(stateView.maxTimeSeen) {
			// no need to change leader
			return
		}
		lockState, err = h.lockClient.tryChangeLeader(
			context.Background(), stateView.cluster, stateView.maxTimeInstance, stateView.maxTimeSeen,
		)
		if err != nil {
			log.Error("msg", "Couldn't change leader", "err", err)
			return
		}
		if lockState.leader == stateView.maxTimeInstance {
			// leader changed
			return
		}
		// leader didn't change, wait a bit and try again
		time.Sleep(backOffDurationOnLeaderChange)
		// QUESTION: Do we need to do a h.state.update(lockState, stateView.maxTimeInstance, stateView.maxTimeSeen)
	}
}
