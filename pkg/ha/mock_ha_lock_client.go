package ha

import (
	"context"
	"fmt"
	"time"
)

type mockLockClient struct {
	leadersPerCluster map[string]*haLockState
}

func (m *mockLockClient) checkInsert(_ context.Context, cluster, leader string, minTime, maxTime time.Time) (*haLockState, error) {
	lock, exists := m.leadersPerCluster[cluster]
	if !exists {
		lock = &haLockState{
			cluster:    cluster,
			leader:     leader,
			leaseStart: minTime,
			leaseUntil: maxTime,
		}
		m.leadersPerCluster[cluster] = lock
	}

	if cluster == "cluster4" || cluster == "cluster5" {
		return lock, leaderHasChanged
	}

	return lock, nil
}

func (m *mockLockClient) tryChangeLeader(_ context.Context, cluster, newLeader string, maxTime time.Time) (*haLockState, error) {
	lock, exists := m.leadersPerCluster[cluster]
	if !exists {
		return nil, fmt.Errorf("no leader for %s, checkInsert never called before tryChange leader", cluster)
	}
	lock = &haLockState{
		cluster:    cluster,
		leader:     newLeader,
		leaseStart: lock.leaseUntil,
		leaseUntil: maxTime,
	}
	m.leadersPerCluster[cluster] = lock
	return lock, nil
}

func newMockLockClient() *mockLockClient {
	return &mockLockClient{leadersPerCluster: make(map[string]*haLockState)}
}

func (m *mockLockClient) readLeaseSettings(ctx context.Context) (timeout, refresh time.Duration, err error) {
	return time.Minute * 1, time.Second * 10, nil
}

var count int
func (m *mockLockClient) readLockState(ctx context.Context, cluster string) (*haLockState, error) {
	count++

	// the below cases are added to simulate the scenario's the leader has updated by another
	// promscale now the current promscale should adopt to the change.
	if cluster == "cluster4" {
		m.leadersPerCluster[cluster].leader = "replica2"
	}

	if cluster == "cluster5" && count == 1 {
		m.leadersPerCluster[cluster].leader = "replica2"
		m.leadersPerCluster[cluster].leaseUntil = time.Now().Add(-10*time.Minute)
	}

	return m.leadersPerCluster[cluster], nil
}
