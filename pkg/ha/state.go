package ha

import (
	"fmt"
	"sync"
	"time"
)

type State struct {
	leader            string
	leaseStart        time.Time
	leaseUntil        time.Time
	maxTimeSeen       time.Time // max data time seen by any instance
	maxTimeInstance   string    // the instance name that’s seen the maxtime
	maxTimeSeenLeader time.Time
	_mu               sync.RWMutex
}

type StateView struct {
	leader            string
	leaseStart        time.Time
	leaseUntil        time.Time
	maxTimeSeen       time.Time
	maxTimeInstance   string
	maxTimeSeenLeader time.Time
}

func (h *State) updateStateFromDB(latestState *haLockState, maxT time.Time, replicaName string) {
	fmt.Println("latest state: ", latestState)
	h._mu.Lock()
	defer h._mu.Unlock()
	if h.maxTimeSeen.IsZero() {
		h.maxTimeSeen = maxT
		h.maxTimeInstance = replicaName
	}
	h.leader = latestState.leader
	h.leaseStart = latestState.leaseStart
	h.leaseUntil = latestState.leaseUntil
}

func (h *State) updateMaxSeenTime(currentReplica string, currentMaxT time.Time) {
	h._mu.Lock()
	defer h._mu.Unlock()
	if currentMaxT.After(h.maxTimeSeen) {
		h.maxTimeInstance = currentReplica
		h.maxTimeSeen = currentMaxT
	}

	if currentMaxT.After(h.maxTimeSeenLeader) && currentReplica == h.leader {
		h.maxTimeSeenLeader = currentMaxT
	}
}

func (h *State) clone() *StateView {
	h._mu.RLock()
	defer h._mu.RUnlock()
	return &StateView{
		leader:          h.leader,
		leaseStart:      h.leaseStart,
		leaseUntil:      h.leaseUntil,
		maxTimeSeen:     h.maxTimeSeen,
		maxTimeInstance: h.maxTimeInstance,
	}
}
