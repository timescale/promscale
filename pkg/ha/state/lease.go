package state

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/ha/client"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
)

// Lease represents the state of a lease for a cluster
// in a given time. It shows which instance is the leader,
// and the data time range for which the leader holds the lease.
// It also holds the maximum data time seen by any
// instance that sent data through this Promscale,
// the maximum data time seen by the leader,
// and the real time when the leader last sent data.
// 	Access to any field should be controlled through the _mu RW Mutex.
type Lease struct {
	cluster               string
	leader                string
	leaseStart            time.Time
	leaseUntil            time.Time
	maxTimeSeen           time.Time // max data time seen from any prometheus replica
	maxTimeInstance       string    // the replica name that’s seen the maxtime
	maxTimeSeenLeader     time.Time // max data time seen by current leader
	recentLeaderWriteTime time.Time // real time when leader last wrote data
	_mu                   sync.RWMutex
}

// LeaseView represents a snapshot of a lease
// to be used locally in a single goroutine
// so the caller doesn't need to acquire a mutex.
// Obtained by calling Lease.Clone()
type LeaseView struct {
	Cluster               string
	Leader                string
	LeaseStart            time.Time
	LeaseUntil            time.Time
	MaxTimeSeen           time.Time
	MaxTimeInstance       string
	MaxTimeSeenLeader     time.Time
	RecentLeaderWriteTime time.Time
}

// Creates a new Lease and immediately synchronizes with the database, it either
//	- sets the potentialLeader as the leader for the cluster with a lease
//	  for the requested minT and maxT
//	- or the existing leader and lease details are returned and set in the
//	  new Lease.
// An error is returned if an error occurred querying the database.
func NewLease(c client.LeaseClient, cluster, potentialLeader string, minT, maxT, currentTime time.Time) (*Lease, error) {
	dbState, err := c.UpdateLease(context.Background(), cluster, potentialLeader, minT, maxT)
	if err != nil {
		return nil, fmt.Errorf("could not create new lease: %#v", err)
	}

	return &Lease{
		cluster:               dbState.Cluster,
		leader:                dbState.Leader,
		leaseStart:            dbState.LeaseStart,
		leaseUntil:            dbState.LeaseUntil,
		maxTimeSeen:           maxT,
		maxTimeInstance:       potentialLeader,
		maxTimeSeenLeader:     maxT,
		recentLeaderWriteTime: currentTime,
	}, nil
}

// UpdateLease uses the supplied client to attempt to update the lease for the potentialLeader.
// It either updates the lease, or a new leader with the assigned lease interval is set, as
// signified by the database as the source of truth.
// An error is returned if the db can't be reached.
func (h *Lease) UpdateLease(c client.LeaseClient, potentialLeader string, minT, maxT time.Time) error {
	h._mu.RLock()
	cluster := h.cluster
	if h.leader != potentialLeader {
		h._mu.RUnlock()
		return fmt.Errorf("should never be updating the lease for a non-leader")
	}
	h._mu.RUnlock()
	stateFromDB, err := c.UpdateLease(context.Background(), cluster, potentialLeader, minT, maxT)
	if err != nil {
		return fmt.Errorf("could not update lease from db: %#v", err)
	}
	h.setUpdateFromDB(stateFromDB)
	return nil
}

// TryChangeLeader uses the supplied client to attempt to change the leader
// of the cluster based on the maximum observed data time and the instance
// that had it. If updates the lease with the latest state from the database.
// An error is returned if the db can't be reached.
func (h *Lease) TryChangeLeader(c client.LeaseClient) error {
	h._mu.RLock()
	cluster := h.cluster
	maxTimeInstance := h.maxTimeInstance
	maxTimeSeen := h.maxTimeSeen
	h._mu.RUnlock()
	leaseState, err := c.TryChangeLeader(
		context.Background(), cluster, maxTimeInstance, maxTimeSeen,
	)

	if err != nil {
		return fmt.Errorf("could not call try change leader from db: %#v", err)
	}

	h.setUpdateFromDB(leaseState)
	return nil
}

// UpdateMaxSeenTime updates the maximum data time seen by the current leader,
// the maximum data time seen by any Prometheus instance/replica, and writes the
// current wall time of when the current leader last sent data samples.
func (h *Lease) UpdateMaxSeenTime(currentReplica string, currentMaxT, currentWallTime time.Time) {
	h._mu.Lock()
	defer h._mu.Unlock()
	if currentMaxT.After(h.maxTimeSeen) {
		h.maxTimeInstance = currentReplica
		h.maxTimeSeen = currentMaxT
	}

	if currentReplica != h.leader {
		return
	}

	if currentMaxT.After(h.maxTimeSeenLeader) {
		h.maxTimeSeenLeader = currentMaxT
	}
	h.recentLeaderWriteTime = currentWallTime
}

// GetLeader returns the current leader. To be used
// when only the current leader is required, so we can avoid
// a complete state copy with Clone().
func (h *Lease) GetLeader() string {
	h._mu.RLock()
	defer h._mu.RUnlock()
	return h.leader
}

// Safely creates a LeaseView form the current lease state.
// The LeaseView is to be used locally in a single goroutine
// to avoid the need for a RLock each time one of the fields
// needs to be read.
func (h *Lease) Clone() *LeaseView {
	h._mu.RLock()
	defer h._mu.RUnlock()
	return &LeaseView{
		Cluster:               h.cluster,
		Leader:                h.leader,
		LeaseStart:            h.leaseStart,
		LeaseUntil:            h.leaseUntil,
		MaxTimeSeen:           h.maxTimeSeen,
		MaxTimeInstance:       h.maxTimeInstance,
		MaxTimeSeenLeader:     h.maxTimeSeenLeader,
		RecentLeaderWriteTime: h.recentLeaderWriteTime,
	}
}

func (h *Lease) setUpdateFromDB(stateFromDB *client.LeaseDBState) {
	h._mu.Lock()
	defer h._mu.Unlock()
	oldLeader := h.leader
	h.leader = stateFromDB.Leader
	h.leaseStart = stateFromDB.LeaseStart
	h.leaseUntil = stateFromDB.LeaseUntil
	if oldLeader != h.leader {
		h.maxTimeSeenLeader = time.Time{}
		h.recentLeaderWriteTime = time.Now()
	}
	exposeHAStateToMetrics(stateFromDB.Cluster, oldLeader, stateFromDB.Leader)
}

func exposeHAStateToMetrics(cluster, oldLeader, newLeader string) {
	if oldLeader == newLeader {
		return
	}
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
