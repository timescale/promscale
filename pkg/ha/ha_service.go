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
	state             *sync.Map
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
		state:             &sync.Map{},
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
		h.state.Range(func(c, s interface{}) bool {
			cluster := fmt.Sprint(c)
			state := castToState(s)
			stateView := state.clone()
			err := h.syncLockStateFromDB(stateView.leaseStart, stateView.maxTimeSeenLeader, cluster, stateView.leader)
			if err != nil {
				log.Error("failed to validate cluster %s state %v", cluster, err)
			}
			if stateView.maxTimeSeen.After(stateView.leaseUntil) {
				go h.tryChangeLeader(cluster)
			}
			return true
		})
	}
}

// checkInsert verifies the samples are from prom leader & in an expected time range.
// 	returns an boolean signifying whether to allow (true),
//		or deny (false). The second returned argument is the
//      minimum timestamp of the accepted samples.
//		An error is returned if the lock state could not be
//		checked against the db
func (h *Service) checkInsert(minT, maxT time.Time, clusterName, replicaName string) (bool, time.Time, error) {
	s, ok := h.state.Load(clusterName)
	if !ok {
		s, _ = h.state.LoadOrStore(clusterName, &State{})
		err := h.syncLockStateFromDB(minT, maxT, clusterName, replicaName)
		if err != nil {
			return false, time.Time{}, err
		}
	}

	state := castToState(s)
	stateView := state.clone()

	defer state.updateMaxSeenTime(replicaName, maxT)
	if replicaName != stateView.leader {
		return false, time.Time{}, nil
	}

	acceptedMinT := minT
	if minT.Before(stateView.leaseStart) {
		acceptedMinT = stateView.leaseStart
	}

	if maxT.After(stateView.leaseUntil) {
		err := h.syncLockStateFromDB(minT, maxT, clusterName, replicaName)
		if err != nil {
			return false, time.Time{}, err
		}
	}

	// requesting replica is leader, allow
	return true, acceptedMinT, nil
}

func (h *Service) syncLockStateFromDB(minT, maxT time.Time, clusterName, replicaName string) error {
	// TODO use proper context
	lockState, err := h.lockClient.checkInsert(context.Background(), clusterName, replicaName, minT, maxT)
	if err != nil && err.Error() != leaderHasChanged.Error() {
		return fmt.Errorf("could not check ha lock state: %#v", err)
	}

	state, err := h.loadState(clusterName)
	if err != nil {
		return err
	}
	// leader changed
	if err != nil && err.Error() == leaderHasChanged.Error() {
		// read latest lock state
		lockState, err = h.lockClient.readLockState(context.Background(), clusterName)
		// couldn't get latest lock state
		if err != nil {
			log.Error("could not check ha lock state: %#v", err)
		}
	}
	state.updateStateFromDB(lockState, maxT, replicaName)
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
		state, err := h.loadState(cluster)
		if err != nil {
			log.Error("error", err)
			return
		}
		stateView := state.clone()

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

func (h *Service) loadState(cluster string) (*State, error) {
	s, ok := h.state.Load(cluster)
	if !ok {
		return nil, fmt.Errorf("couldn't load %s cluster state from ha service", cluster)
	}
	state := castToState(s)
	return state, nil
}

func castToState(s interface{}) *State {
	state := s.(*State)
	return state
}
