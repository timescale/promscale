// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgconn"
	"github.com/timescale/promscale/pkg/ha/state"
)

type mockLockClient struct {
	leadersPerCluster map[string]*state.HALockState
}

func (m *mockLockClient) updateLease(_ context.Context, cluster, leader string, minTime, maxTime time.Time) (*state.HALockState, error) {
	lock, exists := m.leadersPerCluster[cluster]
	if !exists {
		lock = &state.HALockState{
			Cluster:    cluster,
			Leader:     leader,
			LeaseStart: minTime,
			LeaseUntil: maxTime,
		}
		m.leadersPerCluster[cluster] = lock
	}

	if cluster == "cluster4" || cluster == "cluster5" {
		p := &pgconn.PgError{}
		p.Code = "PS010"
		return lock, p
	}

	return lock, nil
}

func (m *mockLockClient) tryChangeLeader(_ context.Context, cluster, newLeader string, maxTime time.Time) (*state.HALockState, error) {
	lock, exists := m.leadersPerCluster[cluster]
	if !exists {
		return nil, fmt.Errorf("no leader for %s, checkInsert never called before tryChange leader", cluster)
	}
	lock = &state.HALockState{
		Cluster:    cluster,
		Leader:     newLeader,
		LeaseStart: lock.LeaseUntil,
		LeaseUntil: maxTime,
	}
	m.leadersPerCluster[cluster] = lock
	return lock, nil
}

func newMockLockClient() *mockLockClient {
	return &mockLockClient{leadersPerCluster: make(map[string]*state.HALockState)}
}

func (m *mockLockClient) readLeaseSettings(ctx context.Context) (timeout, refresh time.Duration, err error) {
	return time.Minute * 1, time.Second * 10, nil
}

var count int

func (m *mockLockClient) readLockState(ctx context.Context, cluster string) (*state.HALockState, error) {
	count++

	// the below cases are added to simulate the scenario's the leader has updated by another
	// promscale now the current promscale should adopt to the change.
	if cluster == "cluster4" {
		m.leadersPerCluster[cluster].Leader = "replica2"
	}

	if cluster == "cluster5" && count == 1 {
		m.leadersPerCluster[cluster].Leader = "replica2"
		m.leadersPerCluster[cluster].LeaseUntil = time.Now().Add(-10 * time.Minute)
	}

	return m.leadersPerCluster[cluster], nil
}
