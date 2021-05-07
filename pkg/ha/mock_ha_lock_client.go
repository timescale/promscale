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
	leadersPerCluster map[string][]client.LeaseDBState
}

func (m *mockLockClient) GetPastLeaseInfo(ctx context.Context, cluster string, replica string, start time.Time, end time.Time) (client.LeaseDBState, error) {
	locks, exists := m.leadersPerCluster[cluster]
	if !exists {
		return client.LeaseDBState{}, client.ErrNoPastLease
	}

	for _, lock := range locks {
		if lock.Cluster == cluster &&
			lock.Leader == replica &&
			lock.LeaseUntil.After(start) &&
			!lock.LeaseUntil.After(end) {
			return lock, nil
		}
	}
	return client.LeaseDBState{}, client.ErrNoPastLease
}

func (m *mockLockClient) UpdateLease(_ context.Context, cluster, leader string, minTime, maxTime time.Time) (client.LeaseDBState, error) {
	lock, exists := m.leadersPerCluster[cluster]
	if !exists {
		lock := client.LeaseDBState{
			Cluster:    cluster,
			Leader:     leader,
			LeaseStart: minTime,
			LeaseUntil: maxTime.Add(time.Second),
		}
		m.leadersPerCluster[cluster] = []client.LeaseDBState{lock}
	}

	return lock[len(lock)-1], nil
}

func (m *mockLockClient) TryChangeLeader(_ context.Context, cluster, newLeader string, maxTime time.Time) (client.LeaseDBState, error) {
	locks, exists := m.leadersPerCluster[cluster]
	if !exists {
		return client.LeaseDBState{}, fmt.Errorf("no leader for %s, UpdateLease never called before TryChangeLeader", cluster)
	}
	lock := client.LeaseDBState{
		Cluster:    cluster,
		Leader:     newLeader,
		LeaseStart: locks[0].LeaseUntil,
		LeaseUntil: maxTime.Add(time.Second),
	}
	m.leadersPerCluster[cluster] = append(locks, lock)
	return lock, nil
}

func newMockLockClient() *mockLockClient {
	return &mockLockClient{leadersPerCluster: make(map[string][]client.LeaseDBState)}
}
