// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package client

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	leasesTable         = "_prom_catalog.ha_leases"
	leaseLogsTable      = "_prom_catalog.ha_leases_logs"
	updateLeaseFn       = "_prom_catalog.update_lease"
	tryChangeLeaderFn   = "_prom_catalog.try_change_leader"
	updateLeaseSQL      = "SELECT * FROM " + updateLeaseFn + "($1, $2, $3, $4)"
	tryChangeLeaderSQL  = "SELECT * FROM " + tryChangeLeaderFn + "($1, $2, $3)"
	latestLeaseStateSQL = "SELECT leader_name, lease_start, lease_until FROM " + leasesTable + " WHERE cluster_name = $1"
	getPastLeaseInfoSQL = "SELECT lease_start, lease_until FROM " + leaseLogsTable +
		" WHERE cluster_name = $1" +
		" AND leader_name = $2" +
		" AND lease_until > $3" +
		" AND lease_until <= $4" +
		" ORDER BY lease_start" +
		" LIMIT 1"

	leaderChangedErrCode = "PS010"
)

var ErrNoPastLease = fmt.Errorf("no past leases found")

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
	UpdateLease(ctx context.Context, cluster, replica string, minTime, maxTime time.Time) (LeaseDBState, error)
	// tryChangeLeader tries to set a new leader for a cluster
	// returns:
	// *haLockState current state of the lock (if try was successful state.leader == newLeader)
	// error signifying the call couldn't be made
	TryChangeLeader(ctx context.Context, cluster, newLeader string, maxTime time.Time) (LeaseDBState, error)
	GetPastLeaseInfo(ctx context.Context, cluster, replica string, start, end time.Time) (LeaseDBState, error)
}

type leaseClientDB struct {
	dbConn pgxconn.PgxConn
}

func NewLeaseClient(dbConn pgxconn.PgxConn) LeaseClient {
	return &leaseClientDB{dbConn: dbConn}
}

func (l *leaseClientDB) UpdateLease(ctx context.Context, cluster, leader string, minTime, maxTime time.Time) (LeaseDBState, error) {
	dbState := LeaseDBState{}
	row := l.dbConn.QueryRow(ctx, updateLeaseSQL, cluster, leader, minTime, maxTime)
	leaderHasChanged := false
	err := row.Scan(&(dbState.Cluster), &(dbState.Leader), &(dbState.LeaseStart), &(dbState.LeaseUntil))
	if err != nil {
		if e, ok := err.(*pgconn.PgError); !ok || e.Code != leaderChangedErrCode {
			return dbState, fmt.Errorf("could not update lease: %w", err)
		}

		leaderHasChanged = true
	}

	// leader changed
	if leaderHasChanged {
		// read latest lease state
		dbState, err = l.readLeaseState(context.Background(), cluster)
		// couldn't get latest lease state
		if err != nil {
			return dbState, fmt.Errorf("could not update lease: %#v", err)
		}
	}
	return dbState, nil
}

func (l *leaseClientDB) TryChangeLeader(ctx context.Context, cluster, newLeader string, maxTime time.Time) (LeaseDBState, error) {
	dbState := LeaseDBState{}
	row := l.dbConn.QueryRow(ctx, tryChangeLeaderSQL, cluster, newLeader, maxTime)
	if err := row.Scan(&(dbState.Cluster), &(dbState.Leader), &(dbState.LeaseStart), &(dbState.LeaseUntil)); err != nil {
		return dbState, err
	}
	return dbState, nil
}

func (l *leaseClientDB) GetPastLeaseInfo(ctx context.Context, cluster, replica string, start, end time.Time) (LeaseDBState, error) {
	dbState := LeaseDBState{
		Cluster: cluster,
		Leader:  replica,
	}
	row := l.dbConn.QueryRow(ctx, getPastLeaseInfoSQL, cluster, replica, start, end)
	if err := row.Scan(&(dbState.LeaseStart), &(dbState.LeaseUntil)); err != nil {
		if err == pgx.ErrNoRows {
			return dbState, ErrNoPastLease
		}
		return dbState, err
	}
	return dbState, nil
}

func (l *leaseClientDB) readLeaseState(ctx context.Context, cluster string) (LeaseDBState, error) {
	dbState := LeaseDBState{Cluster: cluster}
	row := l.dbConn.QueryRow(ctx, latestLeaseStateSQL, cluster)
	if err := row.Scan(&dbState.Leader, &dbState.LeaseStart, &dbState.LeaseUntil); err != nil {
		return dbState, err
	}
	return dbState, nil
}
