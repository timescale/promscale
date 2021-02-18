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
	leaseRefreshKey      = "ha_lease_refresh"
	leaseTimeoutKey      = "ha_lease_timeout"
	leasesTable          = schema.Catalog + ".ha_leases"
	updateLeaseFn        = schema.Catalog + ".update_lease"
	tryChangeLeaderFn    = schema.Catalog + ".try_change_leader"
	updateLeaseSql       = "SELECT * FROM " + updateLeaseFn + "($1, $2, $3, $4)"
	tryChangeLeaderSql   = "SELECT * FROM " + tryChangeLeaderFn + "($1, $2, $3)"
	latestLockStateSql   = "SELECT leader_name, lease_start, lease_until FROM " + leasesTable + " WHERE cluster_name = $1"
	readLeaseSettings    = "SELECT value FROM " + schema.Catalog + ".default where key IN($1)"
	leaderChangedErrCode = "PS010"
	unsetIntervalValue   = -time.Second
)

// LeaseDBState represents the current lock holder
// as reported from the db
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
	// readLeaseSettings gets the lease timeout and lease refresh parameters
	ReadLeaseSettings(ctx context.Context) (timeout, refresh time.Duration, err error)
}

type haLockClientDB struct {
	dbConn               pgxconn.PgxConn
	leaseRefreshInterval time.Duration
	leaseTimeoutInterval time.Duration
}

func NewHaLockClient(dbConn pgxconn.PgxConn) LeaseClient {
	return &haLockClientDB{dbConn: dbConn, leaseRefreshInterval: unsetIntervalValue, leaseTimeoutInterval: unsetIntervalValue}
}

func (h *haLockClientDB) UpdateLease(ctx context.Context, cluster, leader string, minTime, maxTime time.Time) (
	dbState *LeaseDBState, err error,
) {
	dbState = new(LeaseDBState)
	row := h.dbConn.QueryRow(ctx, updateLeaseSql, cluster, leader, minTime, maxTime)
	leaderHasChanged := false
	if err := row.Scan(&(dbState.Cluster), &(dbState.Leader), &(dbState.LeaseStart), &(dbState.LeaseUntil)); err != nil {
		if e, ok := err.(*pgconn.PgError); ok && e.Code == leaderChangedErrCode {
			leaderHasChanged = true
		} else {
			return nil, fmt.Errorf("could not update lease: %w", err)
		}
	}

	// leader changed
	if leaderHasChanged {
		// read latest lock state
		dbState, err = h.readLeaseState(context.Background(), cluster)
		// couldn't get latest lock state
		if err != nil {
			return nil, fmt.Errorf("could not update lease: %#v", err)
		}
	}
	return
}

func (h *haLockClientDB) TryChangeLeader(ctx context.Context, cluster, newLeader string, maxTime time.Time) (*LeaseDBState, error) {
	dbLock := LeaseDBState{}
	row := h.dbConn.QueryRow(ctx, tryChangeLeaderSql, cluster, newLeader, maxTime)
	if err := row.Scan(&(dbLock.Cluster), &(dbLock.Leader), &(dbLock.LeaseStart), &(dbLock.LeaseUntil)); err != nil {
		return nil, err
	}
	return &dbLock, nil
}

func (h *haLockClientDB) readLeaseState(ctx context.Context, cluster string) (*LeaseDBState, error) {
	dbLock := LeaseDBState{Cluster: cluster}
	row := h.dbConn.QueryRow(ctx, latestLockStateSql, cluster)
	if err := row.Scan(&dbLock.Leader, &dbLock.LeaseStart, &dbLock.LeaseUntil); err != nil {
		return nil, err
	}
	return &dbLock, nil
}

func (h *haLockClientDB) ReadLeaseSettings(ctx context.Context) (timeout, refresh time.Duration, err error) {
	if h.leaseTimeoutInterval != unsetIntervalValue && h.leaseRefreshInterval != unsetIntervalValue {
		return h.leaseTimeoutInterval, h.leaseRefreshInterval, nil
	}

	var value string
	// get leaseTimeOut
	row := h.dbConn.QueryRow(ctx, readLeaseSettings, leaseTimeoutKey)
	if err = row.Scan(&value); err != nil {
		return -1, -1, err
	}
	if timeout, err = parseTimestampToDuration(value); err != nil {
		return -1, -1, err
	}

	// get leaseRefresh
	row = h.dbConn.QueryRow(ctx, readLeaseSettings, leaseRefreshKey)
	if err = row.Scan(&value); err != nil {
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
