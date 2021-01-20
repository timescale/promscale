package ha

import (
	"context"
	"golang.org/x/sync/semaphore"
)

func MockNewHAService(clusterInfo []*haLockState) *Service {
	lockClient := newMockLockClient()
	timeout, refresh, _ := lockClient.readLeaseSettings(context.Background())

	for _, c := range clusterInfo {
		lockClient.leadersPerCluster[c.cluster] = c
	}

	service := &Service{
		state:             make(map[string]*State),
		lockClient:        lockClient,
		leaseTimeout:      timeout,
		leaseRefresh:      refresh,
		_leaderChangeLock: semaphore.NewWeighted(1),
	}
	return service
}
