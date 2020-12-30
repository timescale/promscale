package ha

import (
	"sync"
	"time"
)

type State struct {
	cluster         string
	leader          string
	leaseStart      time.Time
	leaseUntil      time.Time
	maxTimeSeen     time.Time // max data time seen by any instance
	maxTimeInstance string    // the instance name that’s seen the maxtime
	_mu             sync.RWMutex
}

type StateView struct {
	cluster         string
	leader          string
	leaseStart      time.Time
	leaseUntil      time.Time
	maxTimeSeen     time.Time
	maxTimeInstance string
}

func (h *State) update(latestState *haLockState, currentReplica string, currentMaxT time.Time) {
	h._mu.Lock()
	defer h._mu.Unlock()
	h.leader = latestState.leader
	h.leaseStart = latestState.leaseStart
	h.leaseUntil = latestState.leaseUntil
	if currentMaxT.After(h.maxTimeSeen) {
		h.maxTimeInstance = currentReplica
		h.maxTimeSeen = currentMaxT
	}
}

func (h *State) clone() *StateView {
	h._mu.RLock()
	defer h._mu.RUnlock()
	return &StateView{
		cluster:         h.cluster,
		leader:          h.leader,
		leaseStart:      h.leaseStart,
		leaseUntil:      h.leaseUntil,
		maxTimeSeen:     h.maxTimeSeen,
		maxTimeInstance: h.maxTimeInstance,
	}
}
