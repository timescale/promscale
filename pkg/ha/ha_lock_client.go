// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"context"
	"fmt"
	"time"

	"github.com/timescale/promscale/pkg/ha/state"

	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	leaseRefreshKey    = "ha_lease_refresh"
	leaseTimeoutKey    = "ha_lease_timeout"
	leasesTable        = schema.Catalog + ".ha_leases"
	updateLeaseFn      = schema.Catalog + ".update_lease"
	tryChangeLeaderFn  = schema.Catalog + ".try_change_leader"
	checkInsertSql     = "SELECT * FROM " + updateLeaseFn + "($1, $2, $3, $4)"
	tryChangeLeaderSql = "SELECT * FROM " + tryChangeLeaderFn + "($1, $2, $3)"
	latestLockStateSql = "SELECT leader_name, lease_start, lease_until FROM " + leasesTable + " WHERE cluster_name = $1"
	readLeaseSettings  = "SELECT value FROM " + schema.Catalog + ".default where key IN($1)"
)

// haLockClient defines an interface for checking and changing leader status
type haLockClient interface {
	// checkInsert confirms permissions for a given leader wanting to insert
	// data in a given time range.
	// returns:
	//   *haLockState current state of the lock
	//   error - either a generic error that signifies
	//		the check couldn't be performed
	//		or a leaderHasChanged error signifying the leader has changed and HAState
	//		needs to be updated
	updateLease(ctx context.Context, cluster, replica string, minTime, maxTime time.Time) (*state.HALockState, error)
	// tryChangeLeader tries to set a new leader for a cluster
	// returns:
	// *haLockState current state of the lock (if try was successful state.leader == newLeader)
	// error signifying the call couldn't be made
	tryChangeLeader(ctx context.Context, cluster, newLeader string, maxTime time.Time) (*state.HALockState, error)
	// readLockState retrieves the latest state of the lock. To be called only
	// when checkInsert returns a leaderHasChanged error
	// returns:
	// 		*haLockState latest state of the lock
	//		* error if the check couldn't be performed
	readLockState(ctx context.Context, cluster string) (*state.HALockState, error)
	// readLeaseSettings gets the lease timeout and lease refresh parameters
	readLeaseSettings(ctx context.Context) (timeout, refresh time.Duration, err error)
}

type haLockClientDB struct {
	dbConn pgxconn.PgxConn
}

func newHaLockClient(dbConn pgxconn.PgxConn) haLockClient {
	return &haLockClientDB{dbConn: dbConn}
}

func (h *haLockClientDB) updateLease(ctx context.Context, cluster, leader string, minTime, maxTime time.Time) (*state.HALockState, error) {
	dbLock := state.HALockState{}
	row := h.dbConn.QueryRow(ctx, checkInsertSql, cluster, leader, minTime, maxTime)
	if err := row.Scan(&(dbLock.Cluster), &(dbLock.Leader), &(dbLock.LeaseStart), &(dbLock.LeaseUntil)); err != nil {
		return nil, err
	}
	return &dbLock, nil
}

func (h *haLockClientDB) tryChangeLeader(ctx context.Context, cluster, newLeader string, maxTime time.Time) (*state.HALockState, error) {
	dbLock := state.HALockState{}
	row := h.dbConn.QueryRow(ctx, tryChangeLeaderSql, cluster, newLeader, maxTime)
	if err := row.Scan(&(dbLock.Cluster), &(dbLock.Leader), &(dbLock.LeaseStart), &(dbLock.LeaseUntil)); err != nil {
		return nil, err
	}
	return &dbLock, nil
}

func (h *haLockClientDB) readLockState(ctx context.Context, cluster string) (*state.HALockState, error) {
	dbLock := state.HALockState{Cluster: cluster}
	row := h.dbConn.QueryRow(ctx, latestLockStateSql, cluster)
	if err := row.Scan(&dbLock.Leader, &dbLock.LeaseStart, &dbLock.LeaseUntil); err != nil {
		return nil, err
	}
	return &dbLock, nil
}

func (h *haLockClientDB) readLeaseSettings(ctx context.Context) (timeout, refresh time.Duration, err error) {
	var value string
	// get leaseTimeOut
	row := h.dbConn.QueryRow(ctx, readLeaseSettings, leaseTimeoutKey)
	if err != nil {
		return -1, -1, err
	}
	if err := row.Scan(&value); err != nil {
		return -1, -1, err
	}
	if timeout, err = parseTimestampToDuration(value); err != nil {
		return -1, -1, err
	}

	// get leaseRefresh
	row = h.dbConn.QueryRow(ctx, readLeaseSettings, leaseRefreshKey)
	if err != nil {
		return -1, -1, err
	}
	if err := row.Scan(&value); err != nil {
		return -1, -1, err
	}

	if refresh, err = parseTimestampToDuration(value); err != nil {
		return -1, -1, err
	}

	return timeout, refresh, nil
}

func parseTimestampToDuration(value string) (time.Duration, error) {
	valueAsDuration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("failed to parse timestamp to time.Duration while reading HA lease settings %v", err)
	}
	return valueAsDuration, nil
}
