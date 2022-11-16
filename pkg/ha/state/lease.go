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
type Lease struct {
	// _mu protects all data fields, client is not protected.
	_mu                   sync.RWMutex
	state                 client.LeaseDBState
	MaxTimeSeen           time.Time // max data time seen from any prometheus replica
	MaxTimeInstance       string    // the replica name that’s seen the maxtime
	MaxTimeSeenLeader     time.Time // max data time seen by current leader
	RecentLeaderWriteTime time.Time // real time when leader last wrote data

	client client.LeaseClient
}

// Creates a new Lease and immediately synchronizes with the database, it either
//   - sets the potentialLeader as the leader for the cluster with a lease
//     for the requested minT and maxT
//   - or the existing leader and lease details are returned and set in the
//     new Lease.
//
// An error is returned if an error occurred querying the database.
func NewLease(c client.LeaseClient, cluster, potentialLeader string, minT, maxT, currentTime time.Time) (*Lease, error) {
	stateFromDB, err := c.UpdateLease(context.Background(), cluster, potentialLeader, minT, maxT)
	if err != nil {
		return nil, fmt.Errorf("could not create new lease: %#v", err)
	}

	exposeHAStateToMetrics(cluster, "", stateFromDB.Leader)

	return &Lease{
		client:                c,
		state:                 stateFromDB,
		MaxTimeSeen:           maxT,
		MaxTimeInstance:       potentialLeader,
		MaxTimeSeenLeader:     maxT,
		RecentLeaderWriteTime: currentTime,
	}, nil
}

// ValidateSamplesInfo verifies if the replica and time range can insert the samples into
// storage. It returns if it can insert, lease start time and error if any happens.
func (l *Lease) ValidateSamplesInfo(replica string, minT, maxT, currT time.Time) (bool, time.Time, error) {
	l._mu.RLock()
	leader := l.state.Leader
	leaseUntil := l.state.LeaseUntil
	leaseStart := l.state.LeaseStart
	l._mu.RUnlock()

	if leader == replica {
		if !maxT.Before(leaseUntil) {
			if err := l.updateLease(replica, minT, maxT); err != nil {
				return false, time.Time{}, err
			}
		}
		return l.confirmLeaseValidation(replica, minT)
	}

	if maxT.Before(leaseUntil) {
		return false, leaseStart, nil
	}

	if err := l.TryChangeLeader(currT); err != nil {
		return false, time.Time{}, err
	}
	return l.confirmLeaseValidation(replica, minT)
}

func (l *Lease) confirmLeaseValidation(replica string, minT time.Time) (bool, time.Time, error) {
	l._mu.RLock()
	defer l._mu.RUnlock()

	return l.state.Leader == replica, l.state.LeaseStart, nil
}

// RefreshLease tries to extend the current lease with the current leader.
func (l *Lease) RefreshLease() error {
	l._mu.RLock()
	leader := l.state.Leader
	leaseStart := l.state.LeaseStart
	maxTimeSeenLeader := l.MaxTimeSeenLeader
	l._mu.RUnlock()
	return l.updateLease(leader, leaseStart, maxTimeSeenLeader)
}

// updateLease uses the supplied client to attempt to update the lease for the potentialLeader.
// It either updates the lease, or a new leader with the assigned lease interval is set, as
// signified by the database as the source of truth.
// An error is returned if the db can't be reached.
func (h *Lease) updateLease(potentialLeader string, minT, maxT time.Time) error {
	h._mu.RLock()
	cluster := h.state.Cluster
	if h.state.Leader != potentialLeader {
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
	cluster := l.state.Cluster
	leaseUntil := l.state.LeaseUntil
	maxTimeInstance := l.MaxTimeInstance
	maxTimeSeen := l.MaxTimeSeen
	recentLeaderSeen := l.RecentLeaderWriteTime
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
	if currentMaxT.After(h.MaxTimeSeen) {
		h.MaxTimeInstance = currentReplica
		h.MaxTimeSeen = currentMaxT
	}

	if currentReplica != h.state.Leader {
		return
	}

	if currentMaxT.After(h.MaxTimeSeenLeader) {
		h.MaxTimeSeenLeader = currentMaxT
	}
	h.RecentLeaderWriteTime = currentWallTime
}

func (h *Lease) setUpdateFromDB(stateFromDB client.LeaseDBState) {
	h._mu.Lock()
	oldLeader := h.state.Leader
	h.state = stateFromDB
	if oldLeader != h.state.Leader {
		h.MaxTimeSeenLeader = time.Time{}
		h.RecentLeaderWriteTime = time.Now()
	}
	h._mu.Unlock()
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
