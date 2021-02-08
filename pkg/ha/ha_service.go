package ha

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgconn"
	"github.com/timescale/promscale/pkg/ha/state"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/util"
	"golang.org/x/sync/semaphore"
)

const (
	backOffDurationOnLeaderChange = 10 * time.Second
	haSyncerTimeInterval          = 15 * time.Second
	haLastWriteIntervalInSecs     = 30
)

type Service struct {
	state               *sync.Map
	lockClient          haLockClient
	leaseTimeout        time.Duration
	leaseRefresh        time.Duration
	leaderChangeLocks   *sync.Map
	syncTicker          util.Ticker
	currentTimeProvider func() time.Time
}

func NewHAService(dbClient pgxconn.PgxConn) (*Service, error) {
	return NewHAServiceWith(dbClient, util.NewTicker(haSyncerTimeInterval), time.Now)
}

func NewHAServiceWith(dbClient pgxconn.PgxConn, ticker util.Ticker, currentTimeFn func() time.Time) (*Service, error) {
	lockClient := newHaLockClient(dbClient)
	timeout, refresh, err := lockClient.readLeaseSettings(context.Background())
	if err != nil {
		return nil, fmt.Errorf("could not create HA state: %#v", err)
	}

	service := &Service{
		state:               &sync.Map{},
		lockClient:          lockClient,
		leaseTimeout:        timeout,
		leaseRefresh:        refresh,
		leaderChangeLocks:   &sync.Map{},
		syncTicker:          ticker,
		currentTimeProvider: currentTimeFn,
	}
	go service.haStateSyncer()
	return service, nil
}

func (h *Service) haStateSyncer() {
	for {
		h.syncTicker.Wait()
		h.state.Range(func(c, s interface{}) bool {
			cluster := fmt.Sprint(c)
			ss := castToState(s)
			stateView := ss.Clone()
			latestState, err := h.syncLockStateFromDB(stateView.LeaseStart, stateView.MaxTimeSeenLeader, cluster, stateView.Leader)
			if err != nil {
				errMsg := fmt.Sprintf("failed to validate cluster %s state", cluster)
				log.Error("msg", errMsg, "err", err)
				return true
			}

			go exposeHAStateToMetrics(cluster, stateView.Leader, latestState.Leader)
			if ok := h.checkLeaseUntilAndLastWrite(stateView); !ok {
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
	var (
	  stateRef *state.State
	  stateView *state.StateView
          err error
        )

	s, ok := h.state.Load(clusterName)
	if !ok {
		stateRef = &state.State{}
		stateRef.UpdateMaxTimeOnZero(maxT, replicaName)
		h.state.LoadOrStore(clusterName, stateRef)
		stateView, err = h.syncLockStateFromDB(minT, maxT, clusterName, replicaName)
		if err != nil {
			return false, time.Time{}, err
		}
		go exposeHAStateToMetrics(clusterName, "", stateView.Leader)
	} else {
		stateRef = castToState(s)
		stateView = stateRef.Clone()
	}

	defer stateRef.UpdateMaxSeenTime(replicaName, maxT)
	if replicaName != stateView.Leader {
		return false, time.Time{}, nil
	}

	acceptedMinT := minT
	if minT.Before(stateView.LeaseStart) {
		acceptedMinT = stateView.LeaseStart
	}

	if maxT.After(stateView.LeaseUntil) {
		stateView, err = h.syncLockStateFromDB(minT, maxT, clusterName, replicaName)
		if err != nil {
			return false, time.Time{}, err
		}

		// on sync-up if notice leader has changed skip
		// ingestion replica prom instance
		if stateView.Leader != replicaName {
			return false, time.Time{}, nil
		}
	}

	// requesting replica is leader, allow
	stateRef.UpdateLastLeaderWriteTime()
	return true, acceptedMinT, nil
}

func (h *Service) syncLockStateFromDB(minT, maxT time.Time, clusterName, replicaName string) (*state.StateView, error) {
	// TODO use proper context
	stateFromDB, err := h.lockClient.updateLease(context.Background(), clusterName, replicaName, minT, maxT)
	leaderHasChanged := false
	if err != nil {
		if e, ok := err.(*pgconn.PgError); ok && e.Code == "PS010" {
			leaderHasChanged = true
		} else {
			return nil, fmt.Errorf("could not check ha lock state: %w", err)
		}
	}

	// leader changed
	if leaderHasChanged {
		// read latest lock state
		stateFromDB, err = h.lockClient.readLockState(context.Background(), clusterName)
		// couldn't get latest lock state
		if err != nil {
			return nil, fmt.Errorf("could not check ha lock state: %#v", err)
		}
	}

	ss, err := h.loadState(clusterName)
	if err != nil {
		return nil, err
	}

	ss.UpdateStateFromDB(stateFromDB)
	return ss.Clone(), nil
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
		if ok := h.checkLeaseUntilAndLastWrite(stateView); ok {
			return
		}
		lockState, err := h.lockClient.tryChangeLeader(
			context.Background(), cluster, stateView.MaxTimeInstance, stateView.MaxTimeSeen,
		)
		if err != nil {
			log.Error("msg", "Couldn't change leader", "err", err)
			return
		}

		ss.UpdateStateFromDB(lockState)
		if lockState.Leader != currentLeader {
			// leader changed
			exposeHAStateToMetrics(cluster, currentLeader, stateView.MaxTimeInstance)
			return
		}
		// leader didn't change, wait a bit and try again
		time.Sleep(backOffDurationOnLeaderChange)
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
	lock, _ := h.leaderChangeLocks.LoadOrStore(cluster, semaphore.NewWeighted(1))
	return lock.(*semaphore.Weighted)
}

func castToState(s interface{}) *state.State {
	ss := s.(*state.State)
	return ss
}

func (h *Service) checkLeaseUntilAndLastWrite(state *state.StateView) bool {
	diff := h.currentTimeProvider().Sub(state.RecentLeaderWriteTime)
	// check leaseUntil is after maxT received from samples or
	// recent leader write is not more than 30 secs older.
	if state.LeaseUntil.After(state.MaxTimeSeen) || diff <= haLastWriteIntervalInSecs {
		return true
	}
	return false
}

func exposeHAStateToMetrics(cluster, oldLeader, newLeader string) {
	if oldLeader != "" {
		metrics.HAClusterLeaderDetails.WithLabelValues(cluster, oldLeader).Set(0)
	}
	metrics.HAClusterLeaderDetails.WithLabelValues(cluster, newLeader).Set(1)
	counter, err := metrics.NumOfHAClusterLeaderChanges.GetMetricWithLabelValues(cluster)
	if err != nil {
		return
	}
	counter.Inc()
}
