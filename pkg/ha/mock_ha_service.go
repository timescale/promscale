package ha

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

func MockNewHAService(clusterInfo []*haLockState) *Service {
	lockClient := newMockLockClient()
	timeout, refresh, _ := lockClient.readLeaseSettings(context.Background())

	for _, c := range clusterInfo {
		lockClient.leadersPerCluster[c.cluster] = c
	}

	return &Service{
		state: &State{
			_mu: sync.RWMutex{},
		},
		lockClient:        lockClient,
		leaseTimeout:      timeout,
		leaseRefresh:      refresh,
		_leaderChangeLock: semaphore.NewWeighted(1),
	}
}
