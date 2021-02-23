// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"context"
	"fmt"
	"time"

	"github.com/timescale/promscale/pkg/ha/client"
)

type mockLockClient struct {
	leadersPerCluster map[string]*client.LeaseDBState
}

func (m *mockLockClient) UpdateLease(_ context.Context, cluster, leader string, minTime, maxTime time.Time) (*client.LeaseDBState, error) {
	lock, exists := m.leadersPerCluster[cluster]
	if !exists {
		lock = &client.LeaseDBState{
			Cluster:    cluster,
			Leader:     leader,
			LeaseStart: minTime,
			LeaseUntil: maxTime.Add(time.Second),
		}
		m.leadersPerCluster[cluster] = lock
	}

	return lock, nil
}

func (m *mockLockClient) TryChangeLeader(_ context.Context, cluster, newLeader string, maxTime time.Time) (*client.LeaseDBState, error) {
	lock, exists := m.leadersPerCluster[cluster]
	if !exists {
		return nil, fmt.Errorf("no leader for %s, UpdateLease never called before TryChangeLeader", cluster)
	}
	lock = &client.LeaseDBState{
		Cluster:    cluster,
		Leader:     newLeader,
		LeaseStart: lock.LeaseUntil,
		LeaseUntil: maxTime.Add(time.Second),
	}
	m.leadersPerCluster[cluster] = lock
	return lock, nil
}

func newMockLockClient() *mockLockClient {
	return &mockLockClient{leadersPerCluster: make(map[string]*client.LeaseDBState)}
}
