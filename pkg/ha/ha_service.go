package ha

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	backOffDurationOnLeaderChange = 10 * time.Second
	haSyncerTimeInterval          = 15 * time.Second
)

type Service struct {
	state             map[string]*State
	lockClient        haLockClient
	leaseTimeout      time.Duration
	leaseRefresh      time.Duration
	_leaderChangeLock *semaphore.Weighted
	_syncTicker       *time.Ticker
}

func NewHAService(dbClient pgxconn.PgxConn) (*Service, error) {
	lockClient := newHaLockClient(dbClient)
	timeout, refresh, err := lockClient.readLeaseSettings(context.Background())
	if err != nil {
		return nil, fmt.Errorf("could not create HA state: %#v", err)
	}

	service := &Service{
		state:             make(map[string]*State),
		lockClient:        lockClient,
		leaseTimeout:      timeout,
		leaseRefresh:      refresh,
		_leaderChangeLock: semaphore.NewWeighted(1),
		_syncTicker:       time.NewTicker(haSyncerTimeInterval),
	}
	go service.haStateSyncer()
	return service, nil
}

func (h *Service) haStateSyncer() {
	for range h._syncTicker.C {
		for clusterName, state := range h.state {
			// To validate cluster state we use maxTimeInstance instead if state.leader
			// because if prometheus leader crashes promscale needs to sync leader
			// using maxTimeSeenInstance and validate if leaderHasChanged condition
			err := h.validateClusterState(state.leaseStart, state.maxTimeSeen, clusterName, state.maxTimeInstance)
			if err != nil {
				log.Error("failed to validate cluster %s state %v", clusterName, err)
			}
		}
	}
}

// checkInsert verifies the samples are from prom leader & in expected time range.
// 	returns true if the insert is allowed
func (h *Service) checkInsert(minT, maxT time.Time, clusterName, replicaName string) (bool, error) {
	// TODO use proper context
	if h.state[clusterName] == nil {
		h.state[clusterName] = &State{
			_mu: sync.RWMutex{},
		}
		err := h.validateClusterState(minT, maxT, clusterName, replicaName)
		if err != nil {
			return false, err
		}
	}

	currentState := h.state[clusterName]
	allow := false
	if replicaName == currentState.leader && maxT.Before(currentState.leaseUntil) && minT.After(currentState.leaseStart) {
		// requesting replica is leader, allow
		allow = true
	}

	currentState.updateMaxSeenTime(replicaName, maxT)
	return allow, nil
}

func (h *Service) validateClusterState(minT, maxT time.Time, clusterName, replicaName string) error {
	lockState, err := h.lockClient.checkInsert(context.Background(), clusterName, replicaName, minT, maxT)
	if err != nil && err.Error() != leaderHasChanged.Error() {
		return fmt.Errorf("could not check ha lock state: %#v", err)
	}

	// leader changed
	if err != nil && err.Error() == leaderHasChanged.Error() {
		// read latest lock state
		lockState, err = h.lockClient.readLockState(context.Background(), clusterName)
		// couldn't get latest lock state
		if err != nil {
			log.Error("could not check ha lock state: %#v", err)
		}

		// update state asynchronously
		h.state[clusterName].updateStateFromDB(lockState, maxT, replicaName)
		go h.tryChangeLeader(clusterName)
	} else {
		h.state[clusterName].updateStateFromDB(lockState, maxT, replicaName)
	}
	return nil
}

func (h *Service) tryChangeLeader(cluster string) {
	ok := h._leaderChangeLock.TryAcquire(1)
	if !ok {
		// change leader already in progress
		return
	}
	defer h._leaderChangeLock.Release(1)

	for {
		stateView := h.state[cluster].clone()

		if stateView.leaseUntil.After(stateView.maxTimeSeen) {
			return
		}

		lockState, err := h.lockClient.tryChangeLeader(
			context.Background(), cluster, stateView.maxTimeInstance, stateView.maxTimeSeen,
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
