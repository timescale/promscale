// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	leaseRefreshKey      = "ha_lease_refresh"
	leaseTimeoutKey      = "ha_lease_timeout"
	checkInsertSql       = "SELECT * FROM " + schema.Catalog + ".check_insert($1, $2, $3, $4)"
	tryChangeLeaderSql   = "SELECT * FROM " + schema.Catalog + ".try_change_leader($1, $2, $3)"
	latestLockStateSql   = "SELECT leader, lease_start, lease_until FROM " + schema.Catalog + ".ha_locks WHERE cluster_name = $1"
	readLeaseSettingsSql = "SELECT key, value FROM " + schema.Catalog + ".default where key IN('" + leaseRefreshKey + "','" + leaseTimeoutKey + "')"

	leaderHasChangedErrStr = "ERROR: LEADER_HAS_CHANGED (SQLSTATE P0001)"
)

var leaderHasChanged = errors.New(leaderHasChangedErrStr)

// haLockState represents the current lock holder
// as reported from the db
type haLockState struct {
	cluster    string
	leader     string
	leaseStart time.Time
	leaseUntil time.Time
}

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
	checkInsert(ctx context.Context, cluster, replica string, minTime, maxTime time.Time) (*haLockState, error)
	// tryChangeLeader tries to set a new leader for a cluster
	// returns:
	// *haLockState current state of the lock (if try was successful state.leader == newLeader)
	// error signifying the call couldn't be made
	tryChangeLeader(ctx context.Context, cluster, newLeader string, maxTime time.Time) (*haLockState, error)
	// readLockState retrieves the latest state of the lock. To be called only
	// when checkInsert returns a leaderHasChanged error
	// returns:
	// 		*haLockState latest state of the lock
	//		* error if the check couldn't be performed
	readLockState(ctx context.Context, cluster string) (*haLockState, error)
	// readLeaseSettings gets the lease timeout and lease refresh parameters
	readLeaseSettings(ctx context.Context) (timeout, refresh time.Duration, err error)
}

type haLockClientDB struct {
	dbConn pgxconn.PgxConn
}

func newHaLockClient(dbConn pgxconn.PgxConn) haLockClient {
	return &haLockClientDB{dbConn: dbConn}
}

func (h *haLockClientDB) checkInsert(ctx context.Context, cluster, leader string, minTime, maxTime time.Time) (*haLockState, error) {
	dbLock := haLockState{}
	row := h.dbConn.QueryRow(ctx, checkInsertSql, cluster, leader, minTime, maxTime)
	if err := row.Scan(&(dbLock.cluster), &(dbLock.leader), &(dbLock.leaseStart), &(dbLock.leaseUntil)); err != nil {
		return nil, err
	}
	return &dbLock, nil
}

func (h *haLockClientDB) tryChangeLeader(ctx context.Context, cluster, newLeader string, maxTime time.Time) (*haLockState, error) {
	dbLock := haLockState{}
	row := h.dbConn.QueryRow(ctx, tryChangeLeaderSql, cluster, newLeader, maxTime)
	if err := row.Scan(&(dbLock.cluster), &(dbLock.leader), &(dbLock.leaseStart), &(dbLock.leaseUntil)); err != nil {
		return nil, err
	}
	return &dbLock, nil
}

func (h *haLockClientDB) readLockState(ctx context.Context, cluster string) (*haLockState, error) {
	dbLock := haLockState{cluster: cluster}
	row := h.dbConn.QueryRow(ctx, latestLockStateSql, cluster)
	if err := row.Scan(&dbLock.leader, &dbLock.leaseStart, &dbLock.leaseUntil); err != nil {
		return nil, err
	}
	return &dbLock, nil
}

func (h *haLockClientDB) readLeaseSettings(ctx context.Context) (timeout, refresh time.Duration, err error) {
	rows, err := h.dbConn.Query(ctx, readLeaseSettingsSql)
	if err != nil {
		return -1, -1, err
	}
	defer rows.Close()
	for rows.Next() {
		var key, value string
		var valueAsDuration time.Duration
		if err := rows.Scan(&key, &value); err != nil {
			return -1, -1, err
		}

		if valueAsDuration, err = time.ParseDuration(value); err != nil {
			return -1, -1, err
		}

		if key == leaseTimeoutKey {
			timeout = valueAsDuration
		} else if key == leaseRefreshKey {
			refresh = valueAsDuration
		} else {
			// should be unreachable
			return -1, -1, fmt.Errorf("lease settings query is not good")
		}
	}
	return timeout, refresh, nil
}
