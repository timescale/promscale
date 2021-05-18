// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/ha/client"
)

func MockNewHAService() *Service {
	lockClient := newMockLockClient()

	service := &Service{
		state:               &sync.Map{},
		leaseClient:         lockClient,
		currentTimeProvider: time.Now,
	}
	return service
}

func SetLeaderInMockService(service *Service, states []client.LeaseDBState) {
	for _, state := range states {
		curr := service.leaseClient.(*mockLockClient).leadersPerCluster[state.Cluster]
		service.leaseClient.(*mockLockClient).leadersPerCluster[state.Cluster] = append(curr, state)
	}
}
