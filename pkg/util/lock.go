// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package util

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	pgx "github.com/jackc/pgx/v4"

	"github.com/timescale/timescale-prometheus/pkg/log"
)

const (
	waitForConnectionTimeout = time.Second
)

// PgAdvisoryLock is implementation of leader election based on PostgreSQL advisory locks. All adapters withing a HA group are trying
// to obtain an advisory lock for particular group. The one who holds the lock can write to the database. Due to the fact
// that Prometheus HA setup provides no consistency guarantees this implementation is best effort in regards
// to metrics that is written (some duplicates or data loss are possible during fail-over)
// `leader-election-pg-advisory-lock-prometheus-timeout` config must be set when using PgAdvisoryLock. It will
// trigger leader resign (if instance is a leader) and will prevent an instance to become a leader if there are no requests coming
// from Prometheus within a given timeout. Make sure to provide a reasonable value for the timeout (should be co-related with
// Prometheus scrape interval, eg. 2x or 3x more then scrape interval to prevent leader flipping).
// Recommended architecture when using PgAdvisoryLock is to have one adapter instance for one Prometheus instance.
type PgAdvisoryLock struct {
	conn        *pgx.Conn
	connStr     string
	groupLockID int

	mutex    sync.RWMutex
	obtained bool
}

// NewPgAdvisoryLock creates a new instance with specified lock ID, connection pool and lock timeout.
func NewPgAdvisoryLock(groupLockID int, connStr string) (*PgAdvisoryLock, error) {
	lock := &PgAdvisoryLock{
		connStr:     connStr,
		obtained:    false,
		groupLockID: groupLockID,
	}
	_, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	return lock, nil
}

func getConn(connStr string, cur, maxRetries int) (*pgx.Conn, error) {
	if maxRetries == cur {
		return nil, fmt.Errorf("max attempts reached. giving up on getting a db connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitForConnectionTimeout)
	defer cancel()
	lockConn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("error getting DB connection: %v", err)
	}
	err = checkConnection(lockConn)
	if err != nil {
		log.Error("msg", "Connection pool returned invalid connection", "err", err)
		return getConn(connStr, cur+1, maxRetries)
	}
	return lockConn, nil
}

// ID returns the group lock ID for this instance.
func (l *PgAdvisoryLock) ID() string {
	return strconv.Itoa(l.groupLockID)
}

// BecomeLeader tries to become a leader by acquiring the lock.
func (l *PgAdvisoryLock) BecomeLeader() (bool, error) {
	return l.TryLock()
}

// IsLeader returns the current leader status for this instance.
func (l *PgAdvisoryLock) IsLeader() (bool, error) {
	return l.TryLock()
}

// Resign releases the leader status of this instance.
func (l *PgAdvisoryLock) Resign() error {
	return l.Release()
}

// TryLock tries to obtain the lock if its not already the leader. In the case
// that it is the leader, it verifies the connection to make sure the lock hasn't
// been already lost.
func (l *PgAdvisoryLock) TryLock() (bool, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	gotLock, err := l.getAdvisoryLock()

	if !gotLock || err != nil {
		l.obtained = false
		return false, err
	}

	if !l.obtained {
		l.obtained = true
		log.Debug("msg", fmt.Sprintf("Lock obtained for group id %d", l.groupLockID))
	}

	return true, nil
}

func (l *PgAdvisoryLock) getAdvisoryLock() (bool, error) {
	var err error
	if l.conn == nil {
		l.conn, err = getConn(l.connStr, 0, 10)
	}
	if err != nil {
		return false, err
	}
	defer func() {
		if err != nil {
			l.connCleanUp()
		}
	}()
	rows, err := l.conn.Query(context.Background(), "SELECT pg_try_advisory_lock($1)", l.groupLockID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			return false, fmt.Errorf("error while trying to read response rows from `pg_try_advisory_lock` function: %v", err)
		}
		return false, fmt.Errorf("missing response row from `pg_try_advisory_lock` function")
	}
	var result bool
	if err := rows.Scan(&result); err != nil {
		return false, err
	}
	return result, nil
}

func (l *PgAdvisoryLock) connCleanUp() {
	if l.conn != nil {
		if err := l.conn.Close(context.Background()); err != nil {
			log.Error("err", err)
		}
	}
	l.conn = nil
}

//Close cleans up the connection
func (l *PgAdvisoryLock) Close() {
	l.connCleanUp()
}

// Locked returns if the instance was able to obtain the leader lock.
func (l *PgAdvisoryLock) Locked() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.obtained
}

// Release releases the already obtained leader lock.
func (l *PgAdvisoryLock) Release() error {
	l.mutex.Lock()
	if !l.obtained {
		return fmt.Errorf("can't release while not holding the lock")
	}
	defer l.mutex.Unlock()
	rows, err := l.conn.Query(context.Background(), "SELECT pg_advisory_unlock_all()")
	if err != nil {
		return err
	}
	rows.Close()
	l.obtained = false
	return nil
}

func checkConnection(conn *pgx.Conn) error {
	_, err := conn.Exec(context.Background(), "SELECT 1")
	if err != nil {
		return fmt.Errorf("invalid connection: %v", err)
	}
	return nil
}
