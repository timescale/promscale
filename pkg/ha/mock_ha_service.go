package ha

import (
	"context"
	"golang.org/x/sync/semaphore"
	"sync"
	"time"
)

func MockNewHAService(clusterInfo []*haLockState) *Service {
	lockClient := newMockLockClient()
	timeout, refresh, _ := lockClient.readLeaseSettings(context.Background())

	for _, c := range clusterInfo {
		lockClient.leadersPerCluster[c.cluster] = c
	}

	service := &Service{
		state:             &sync.Map{},
		lockClient:        lockClient,
		leaseTimeout:      timeout,
		leaseRefresh:      refresh,
		_leaderChangeLock: semaphore.NewWeighted(1),
	}
	return service
}

func SetLeaderInMockService(service *Service, cluster, leader string, minT, maxT time.Time) {
	service.lockClient.(*mockLockClient).leadersPerCluster[cluster] = &haLockState{
		cluster:    cluster,
		leader:     leader,
		leaseStart: minT,
		leaseUntil: maxT,
	}
}
