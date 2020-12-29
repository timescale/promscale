package ha

import (
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/pgxconn"
)

type HA struct {
	Cluster         string
	Leader          string
	LeaseStart      time.Time
	LeaseUntil      time.Time
	MaxTimeSeen     time.Time //max data time seen by any instance
	MaxTimeInstance string    // the instance name that’s seen the maxtime
	DBClient        pgxconn.PgxConn
	LeaseTimeout    string
	LeaseRefresh    string
	Enabled         bool
	Ha              HAWriter
	mu              sync.Mutex
}

type HAWriter interface {
	// checkInserter inserts current HA state if state doesn't exist.
	// if state already exists updates the leaseUntil
	CheckInserter(minT, maxT time.Time, clusterName, replicaName string) bool
	// change leader if leaseUntil < maxTimeSeen
	changeLeader(minT, maxT time.Time, clusterName string) (leader string, leaseStart, leaseUntil time.Time)
}

func NewHAState() *HA {
	return &HA{}
}

// check whether the samples are from prom leader & in expected time range.
func (h *HA) CheckInsert(minT, maxT time.Time, clusterName, replicaName string) bool {
	latestLeader, leaseStart, leaseUntil := h.readHALocks(minT, maxT, clusterName)
	if replicaName == latestLeader {
		h.updateHAState(clusterName, latestLeader, replicaName, maxT, leaseStart, leaseUntil)
		return true
	}

	// check if leaseUntil is behind the maxTimeSeen
	// if yes try changing the leader
	if h.LeaseUntil.Before(maxT) {
		latestLeader, leaseStart, leaseUntil = h.changeLeader(minT, maxT, clusterName)
		h.updateHAState(clusterName, latestLeader, replicaName, maxT, leaseStart, leaseUntil)
		return true
	}

	return false
}

func (h *HA) updateHAState(clusterName, leaderName, replicaName string, maxT, leaseStart, leaseUntil time.Time) {
	h.mu.Lock()
	h.MaxTimeSeen = maxT
	h.Cluster = clusterName
	h.Leader = leaderName
	h.MaxTimeInstance = replicaName
	h.LeaseStart = leaseStart
	h.LeaseUntil = leaseUntil
	h.mu.Unlock()
}

func (h *HA) readHALocks(minT, maxT time.Time, clusterName string) (leader string, leaseStart, leaseUntil time.Time) {
	return "", time.Time{}, time.Time{}
}

func (h *HA) changeLeader(minT, maxT time.Time, clusterName string) (leader string, leaseStart, leaseUntil time.Time) {
	return "", time.Time{}, time.Time{}
}
