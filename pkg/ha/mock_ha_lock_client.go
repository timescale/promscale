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
