package state

import (
	"fmt"
	"sync"
	"time"
)

type State struct {
	leader                string
	leaseStart            time.Time
	leaseUntil            time.Time
	maxTimeSeen           time.Time // max data time seen by any instance
	maxTimeInstance       string    // the instance name that’s seen the maxtime
	maxTimeSeenLeader     time.Time
	_mu                   sync.RWMutex
	recentLeaderWriteTime time.Time
}

type StateView struct {
	Leader                string
	LeaseStart            time.Time
	LeaseUntil            time.Time
	MaxTimeSeen           time.Time
	MaxTimeInstance       string
	MaxTimeSeenLeader     time.Time
	RecentLeaderWriteTime time.Time
}

func (s StateView) String() string {
	return fmt.Sprintf("{\n\tLead:%s;\n\tUntil:%s;\n\tMaxSeen:%s;\n\tMaxInst:%s;\n\tMaxSeenLeader:%s\n}",
		s.Leader, s.LeaseUntil, s.MaxTimeSeen, s.MaxTimeInstance, s.MaxTimeSeenLeader)
}

// haLockState represents the current lock holder
// as reported from the db
type HALockState struct {
	Cluster    string
	Leader     string
	LeaseStart time.Time
	LeaseUntil time.Time
}

func (h *State) UpdateStateFromDB(latestState *HALockState) {
	h._mu.Lock()
	defer h._mu.Unlock()
	h.leader = latestState.Leader
	h.leaseStart = latestState.LeaseStart
	h.leaseUntil = latestState.LeaseUntil
}

func (h *State) UpdateMaxTimeOnZero(maxT time.Time, replicaName string) {
	if h.maxTimeSeen.IsZero() {
		h.maxTimeSeen = maxT
		h.maxTimeInstance = replicaName
	}
}

func (h *State) UpdateMaxSeenTime(currentReplica string, currentMaxT time.Time) {
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

func (h *State) UpdateLastLeaderWriteTime() {
	h._mu.Lock()
	defer h._mu.Unlock()
	h.recentLeaderWriteTime = time.Now()
}

func (h *State) Clone() *StateView {
	h._mu.RLock()
	defer h._mu.RUnlock()
	return &StateView{
		Leader:                h.leader,
		LeaseStart:            h.leaseStart,
		LeaseUntil:            h.leaseUntil,
		MaxTimeSeen:           h.maxTimeSeen,
		MaxTimeInstance:       h.maxTimeInstance,
		MaxTimeSeenLeader:     h.maxTimeSeenLeader,
		RecentLeaderWriteTime: h.recentLeaderWriteTime,
	}
}
