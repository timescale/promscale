package ha

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgconn"
	"github.com/timescale/promscale/pkg/ha/state"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
	"golang.org/x/sync/semaphore"
)

const (
	backOffDurationOnLeaderChange = 10 * time.Second
	haSyncerTimeInterval          = 15 * time.Second
)

type Service struct {
	state              *sync.Map
	lockClient         haLockClient
	leaseTimeout       time.Duration
	leaseRefresh       time.Duration
	_leaderChangeLocks *sync.Map
	_syncTicker        *time.Ticker
}

func NewHAService(dbClient pgxconn.PgxConn) (*Service, error) {
	lockClient := newHaLockClient(dbClient)
	timeout, refresh, err := lockClient.readLeaseSettings(context.Background())
	if err != nil {
		return nil, fmt.Errorf("could not create HA state: %#v", err)
	}

	service := &Service{
		state:              &sync.Map{},
		lockClient:         lockClient,
		leaseTimeout:       timeout,
		leaseRefresh:       refresh,
		_leaderChangeLocks: &sync.Map{},
		_syncTicker:        time.NewTicker(haSyncerTimeInterval),
	}
	go service.haStateSyncer()
	return service, nil
}

func (h *Service) haStateSyncer() {
	for range h._syncTicker.C {
		h.state.Range(func(c, s interface{}) bool {
			cluster := fmt.Sprint(c)
			ss := castToState(s)
			stateView := ss.Clone()
			err := h.syncLockStateFromDB(stateView.LeaseStart, stateView.MaxTimeSeenLeader, cluster, stateView.Leader)
			if err != nil {
				log.Error("failed to validate cluster %s state %v", cluster, err)
			}
			if stateView.MaxTimeSeen.After(stateView.LeaseUntil) {
				go h.tryChangeLeader(cluster, stateView.Leader)
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
func (h *Service) CheckInsert(minT, maxT time.Time, clusterName, replicaName string) (bool, time.Time, error) {
	s, ok := h.state.Load(clusterName)
	if !ok {
		ss := &state.State{}
		ss.UpdateMaxTimeOnZero(maxT, replicaName)
		s, _ = h.state.LoadOrStore(clusterName, ss)
		err := h.syncLockStateFromDB(minT, maxT, clusterName, replicaName)
		if err != nil {
			return false, time.Time{}, err
		}
	}

	ss := castToState(s)
	stateView := ss.Clone()

	defer ss.UpdateMaxSeenTime(replicaName, maxT)
	if replicaName != stateView.Leader {
		return false, time.Time{}, nil
	}

	acceptedMinT := minT
	if minT.Before(stateView.LeaseStart) {
		acceptedMinT = stateView.LeaseStart
	}

	if maxT.After(stateView.LeaseUntil) {
		err := h.syncLockStateFromDB(minT, maxT, clusterName, replicaName)
		if err != nil {
			return false, time.Time{}, err
		}
		ss, err := h.loadState(clusterName)
		if err != nil {
			return false, time.Time{}, err
		}
		// on sync-up if notice leader has changed skip
		// ingestion replica prom instance
		if s := ss.Clone(); s.Leader != replicaName {
			return false, time.Time{}, nil
		}

	}

	// requesting replica is leader, allow
	return true, acceptedMinT, nil
}

func (h *Service) syncLockStateFromDB(minT, maxT time.Time, clusterName, replicaName string) error {
	// TODO use proper context
	stateFromDB, err := h.lockClient.updateLease(context.Background(), clusterName, replicaName, minT, maxT)
	leaderHasChanged := false
	if err != nil {
		if e, ok := err.(*pgconn.PgError); ok && e.Code == "PS010" {
			leaderHasChanged = true
		} else {
			return fmt.Errorf("could not check ha lock state: %w", err)
		}
	}

	// leader changed
	if leaderHasChanged {
		// read latest lock state
		stateFromDB, err = h.lockClient.readLockState(context.Background(), clusterName)
		// couldn't get latest lock state
		if err != nil {
			log.Error("could not check ha lock state: %#v", err)
		}
	}

	ss, err := h.loadState(clusterName)
	if err != nil {
		return err
	}

	ss.UpdateStateFromDB(stateFromDB, maxT, replicaName)
	return nil
}

func (h *Service) tryChangeLeader(cluster, currentLeader string) {

	clusterLock := h.getLeaderChangeLock(cluster)
	ok := clusterLock.TryAcquire(1)
	if !ok {
		// change leader already in progress
		return
	}
	defer clusterLock.Release(1)

	for {
		ss, err := h.loadState(cluster)
		if err != nil {
			log.Error("error", err)
			return
		}
		stateView := ss.Clone()

		if stateView.LeaseUntil.After(stateView.MaxTimeSeen) {
			return
		}

		lockState, err := h.lockClient.tryChangeLeader(
			context.Background(), cluster, stateView.MaxTimeInstance, stateView.MaxTimeSeen,
		)
		if err != nil {
			log.Error("msg", "Couldn't change leader", "err", err)
			return
		}

		ss.UpdateStateFromDB(lockState, stateView.MaxTimeSeen, stateView.MaxTimeInstance)
		if lockState.Leader != currentLeader {
			// leader changed
			return
		}
		// leader didn't change, wait a bit and try again
		time.Sleep(backOffDurationOnLeaderChange)
		// QUESTION: Do we need to do a h.state.update(lockState, stateView.maxTimeInstance, stateView.maxTimeSeen)
	}
}

func (h *Service) loadState(cluster string) (*state.State, error) {
	s, ok := h.state.Load(cluster)
	if !ok {
		return nil, fmt.Errorf("couldn't load %s cluster state from ha service", cluster)
	}
	ss := castToState(s)
	return ss, nil
}

func (h *Service) getLeaderChangeLock(cluster string) *semaphore.Weighted {
	lock, _ := h._leaderChangeLocks.LoadOrStore(cluster, semaphore.NewWeighted(1))
	return lock.(*semaphore.Weighted)
}

func castToState(s interface{}) *state.State {
	ss := s.(*state.State)
	return ss
}
