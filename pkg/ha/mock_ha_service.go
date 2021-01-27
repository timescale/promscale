package ha

import (
	"context"
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/ha/state"
	"golang.org/x/sync/semaphore"
)

func MockNewHAService(clusterInfo []*state.HALockState) *Service {
	lockClient := newMockLockClient()
	timeout, refresh, _ := lockClient.readLeaseSettings(context.Background())

	for _, c := range clusterInfo {
		lockClient.leadersPerCluster[c.Cluster] = c
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
	service.lockClient.(*mockLockClient).leadersPerCluster[cluster] = &state.HALockState{
		Cluster:    cluster,
		Leader:     leader,
		LeaseStart: minT,
		LeaseUntil: maxT,
	}
}
