package state

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/ha/client"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
)

const (
	haLastWriteInterval = 30 * time.Second
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
	client                client.LeaseClient
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
		client:                c,
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

func (l *Lease) ValidateSamplesInfo(replica string, minT, maxT, currT time.Time) (bool, time.Time, error) {
	l._mu.RLock()
	leader := l.leader
	leaseUntil := l.leaseUntil
	l._mu.RUnlock()

	if leader == replica {
		if !maxT.Before(leaseUntil) {
			if err := l.updateLease(replica, minT, maxT); err != nil {
				return false, time.Time{}, err
			}
		}
		return l.confirmLeaseValidation(replica, minT)
	}

	if !maxT.After(leaseUntil) {
		return false, time.Time{}, nil
	}

	if err := l.TryChangeLeader(currT); err != nil {
		return false, time.Time{}, err
	}
	return l.confirmLeaseValidation(replica, minT)
}

func (l *Lease) confirmLeaseValidation(replica string, minT time.Time) (bool, time.Time, error) {
	l._mu.RLock()
	leader := l.leader
	leaseStart := l.leaseStart
	l._mu.RUnlock()

	if leader != replica {
		return false, time.Time{}, nil
	}

	if minT.Before(leaseStart) {
		minT = leaseStart
	}
	return true, minT, nil
}

func (l *Lease) RefreshLease() error {
	l._mu.RLock()
	leader := l.leader
	leaseStart := l.leaseStart
	maxTimeSeenLeader := l.maxTimeSeenLeader
	l._mu.RUnlock()
	return l.updateLease(leader, leaseStart, maxTimeSeenLeader)
}

// updateLease uses the supplied client to attempt to update the lease for the potentialLeader.
// It either updates the lease, or a new leader with the assigned lease interval is set, as
// signified by the database as the source of truth.
// An error is returned if the db can't be reached.
func (h *Lease) updateLease(potentialLeader string, minT, maxT time.Time) error {
	h._mu.RLock()
	cluster := h.cluster
	if h.leader != potentialLeader {
		h._mu.RUnlock()
		return fmt.Errorf("should never be updating the lease for a non-leader")
	}
	h._mu.RUnlock()
	stateFromDB, err := h.client.UpdateLease(context.Background(), cluster, potentialLeader, minT, maxT)
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
func (l *Lease) TryChangeLeader(currT time.Time) error {
	l._mu.RLock()
	cluster := l.cluster
	leaseUntil := l.leaseUntil
	maxTimeInstance := l.maxTimeInstance
	maxTimeSeen := l.maxTimeSeen
	recentLeaderSeen := l.recentLeaderWriteTime
	l._mu.RUnlock()

	// Only proceed if we have seen samples after the lease expired.
	if leaseUntil.After(maxTimeSeen) {
		return nil
	}

	// And haven't seen samples from leader in haLastWriteInterval (wall-clock-time).
	if currT.Sub(recentLeaderSeen) <= haLastWriteInterval {
		return nil
	}

	err := l.changeLeader(
		cluster, maxTimeInstance, maxTimeSeen,
	)

	if err != nil {
		return fmt.Errorf("could not call try change leader from db: %#v", err)
	}

	return nil
}

func (l *Lease) changeLeader(cluster string, leader string, maxTimeSeen time.Time) error {
	stateFromDB, err := l.client.TryChangeLeader(
		context.Background(), cluster, leader, maxTimeSeen,
	)

	if err != nil {
		return fmt.Errorf("could not call try change leader from db: %#v", err)
	}

	l.setUpdateFromDB(stateFromDB)
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
