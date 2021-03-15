// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package client

import (
	"context"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
	"time"
)

const (
	leasesTable          = schema.Catalog + ".ha_leases"
	updateLeaseFn        = schema.Catalog + ".update_lease"
	tryChangeLeaderFn    = schema.Catalog + ".try_change_leader"
	updateLeaseSql       = "SELECT * FROM " + updateLeaseFn + "($1, $2, $3, $4)"
	tryChangeLeaderSql   = "SELECT * FROM " + tryChangeLeaderFn + "($1, $2, $3)"
	latestLeaseStateSql  = "SELECT leader_name, lease_start, lease_until FROM " + leasesTable + " WHERE cluster_name = $1"
	leaderChangedErrCode = "PS010"
)

// LeaseDBState represents the current lock holder
// as reported from the DB.
type LeaseDBState struct {
	Cluster    string
	Leader     string
	LeaseStart time.Time
	LeaseUntil time.Time
}

// LeaseClient defines an interface for checking and changing leader status
type LeaseClient interface {
	// updateLease confirms permissions for a given leader wanting to insert
	// data in a given time range.
	// returns:
	//   *haLockState current state of the lock
	//   error - either a generic error that signifies
	//		the check couldn't be performed
	//		or a leaderHasChanged error signifying the leader has changed and HAState
	//		needs to be updated
	UpdateLease(ctx context.Context, cluster, replica string, minTime, maxTime time.Time) (*LeaseDBState, error)
	// tryChangeLeader tries to set a new leader for a cluster
	// returns:
	// *haLockState current state of the lock (if try was successful state.leader == newLeader)
	// error signifying the call couldn't be made
	TryChangeLeader(ctx context.Context, cluster, newLeader string, maxTime time.Time) (*LeaseDBState, error)
}

type haLeaseClientDB struct {
	dbConn pgxconn.PgxConn
}

func NewHaLeaseClient(dbConn pgxconn.PgxConn) LeaseClient {
	return &haLeaseClientDB{dbConn: dbConn}
}

func (h *haLeaseClientDB) UpdateLease(ctx context.Context, cluster, leader string, minTime, maxTime time.Time) (
	dbState *LeaseDBState, err error,
) {
	dbState = new(LeaseDBState)
	row := h.dbConn.QueryRow(ctx, updateLeaseSql, cluster, leader, minTime, maxTime)
	leaderHasChanged := false
	if err := row.Scan(&(dbState.Cluster), &(dbState.Leader), &(dbState.LeaseStart), &(dbState.LeaseUntil)); err != nil {
		if e, ok := err.(*pgconn.PgError); !ok || e.Code != leaderChangedErrCode {
			return nil, fmt.Errorf("could not update lease: %w", err)
		}

		leaderHasChanged = true
	}

	// leader changed
	if leaderHasChanged {
		// read latest lease state
		dbState, err = h.readLeaseState(context.Background(), cluster)
		// couldn't get latest lease state
		if err != nil {
			return nil, fmt.Errorf("could not update lease: %#v", err)
		}
	}
	return
}

func (h *haLeaseClientDB) TryChangeLeader(ctx context.Context, cluster, newLeader string, maxTime time.Time) (*LeaseDBState, error) {
	dbLock := LeaseDBState{}
	row := h.dbConn.QueryRow(ctx, tryChangeLeaderSql, cluster, newLeader, maxTime)
	if err := row.Scan(&(dbLock.Cluster), &(dbLock.Leader), &(dbLock.LeaseStart), &(dbLock.LeaseUntil)); err != nil {
		return nil, err
	}
	return &dbLock, nil
}

func (h *haLeaseClientDB) readLeaseState(ctx context.Context, cluster string) (*LeaseDBState, error) {
	dbLock := LeaseDBState{Cluster: cluster}
	row := h.dbConn.QueryRow(ctx, latestLeaseStateSql, cluster)
	if err := row.Scan(&dbLock.Leader, &dbLock.LeaseStart, &dbLock.LeaseUntil); err != nil {
		return nil, err
	}
	return &dbLock, nil
}
