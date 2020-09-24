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

	"github.com/timescale/promscale/pkg/log"
)

const (
	waitForConnectionTimeout = time.Second
)

var (
	SharedLeaseFailure = fmt.Errorf("failed to acquire shared lease")
)

func GetSharedLease(ctx context.Context, conn *pgx.Conn, id int64) error {
	gotten, err := runLockFunction(ctx, conn, "SELECT pg_try_advisory_lock_shared($1)", id)
	if err != nil {
		return err
	}
	if !gotten {
		return SharedLeaseFailure
	}
	return nil
}

// PgLeaderLock is implementation of leader election based on PostgreSQL advisory locks. All adapters within a HA group are trying
// to obtain an advisory lock for particular group. The one who holds the lock can write to the database. Due to the fact
// that Prometheus HA setup provides no consistency guarantees this implementation is best effort in regards
// to metrics that is written (some duplicates or data loss are possible during fail-over)
// `leader-election-pg-advisory-lock-prometheus-timeout` config must be set when using PgLeaderLock. It will
// trigger leader resign (if instance is a leader) and will prevent an instance to become a leader if there are no requests coming
// from Prometheus within a given timeout. Make sure to provide a reasonable value for the timeout (should be co-related with
// Prometheus scrape interval, eg. 2x or 3x more then scrape interval to prevent leader flipping).
// Recommended architecture when using PgLeaderLock is to have one adapter instance for one Prometheus instance.
type PgLeaderLock struct {
	PgAdvisoryLock
	obtained bool
}

type AfterConnectFunc = func(ctx context.Context, conn *pgx.Conn) error

func NewPgLeaderLock(groupLockID int64, connStr string, afterConnect AfterConnectFunc) (*PgLeaderLock, error) {
	if afterConnect == nil {
		afterConnect = checkConnection
	}
	lock := &PgLeaderLock{
		PgAdvisoryLock{
			connStr:      connStr,
			groupLockID:  groupLockID,
			afterConnect: afterConnect,
		},
		false,
	}
	_, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// ID returns the group lock ID for this instance.
func (l *PgLeaderLock) ID() string {
	return strconv.FormatInt(int64(l.groupLockID), 10)
}

// BecomeLeader tries to become a leader by acquiring the lock.
func (l *PgLeaderLock) BecomeLeader() (bool, error) {
	return l.TryLock()
}

// IsLeader returns the current leader status for this instance.
func (l *PgLeaderLock) IsLeader() (bool, error) {
	return l.TryLock()
}

// TryLock tries to obtain the lock if its not already the leader. In the case
// that it is the leader, it verifies the connection to make sure the lock hasn't
// been already lost.
func (l *PgLeaderLock) TryLock() (bool, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.obtained && l.conn != nil {
		// we already hold the lock verify the connection
		err := l.conn.QueryRow(context.Background(), "SELECT").Scan()
		if err != nil {
			return false, err
		}
		return true, nil
	}

	gotLock, err := l.getAdvisoryLock()

	if !gotLock || err != nil {
		l.obtained = false
		l.connCleanUp()
		return false, err
	}

	if !l.obtained {
		l.obtained = true
		log.Debug("msg", fmt.Sprintf("Lock obtained for group id %d", l.groupLockID))
	}

	return true, nil
}

// Resign releases the leader status of this instance.
func (l *PgLeaderLock) Resign() error {
	return l.Release()
}

// Locked returns if the instance was able to obtain the locks.
func (l *PgLeaderLock) Locked() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.obtained
}

// Release releases the already obtained locks.
func (l *PgLeaderLock) Release() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	defer l.connCleanUp()
	if !l.obtained {
		return fmt.Errorf("can't release while not holding the lock")
	}

	defer func() { l.obtained = false }()

	unlocked, err := l.unlock()
	if err != nil {
		return err
	}
	if !unlocked {
		log.Debug("msg", fmt.Sprintf("false release for group id %d", l.groupLockID))
	}

	return nil
}

type PgAdvisoryLock struct {
	conn         *pgx.Conn
	connStr      string
	groupLockID  int64
	afterConnect AfterConnectFunc

	mutex sync.RWMutex
}

type AdvisoryLock interface {
	GetAdvisoryLock() (bool, error)
	GetSharedAdvisoryLock() (bool, error)
	Unlock() (bool, error)
	UnlockShared() (bool, error)
	Close()
}

// PgAdvisoryLock is a AdvisoryLock
var _ AdvisoryLock = (*PgAdvisoryLock)(nil)

// NewPgAdvisoryLock creates a new instance with specified lock ID, connection pool and lock timeout.
func NewPgAdvisoryLock(groupLockID int64, connStr string) (*PgAdvisoryLock, error) {
	lock := &PgAdvisoryLock{
		connStr:      connStr,
		groupLockID:  groupLockID,
		afterConnect: checkConnection,
	}
	return lock, nil
}

func (l *PgAdvisoryLock) getConn(connStr string, cur, maxRetries int) (*pgx.Conn, error) {
	if maxRetries == cur {
		return nil, fmt.Errorf("max attempts reached. giving up on getting a db connection")
	}

	ctx, cancel := context.WithTimeout(context.Background(), waitForConnectionTimeout)
	defer cancel()

	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing config connection: %w", err)
	}

	lockConn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error getting DB connection: %w", err)
	}

	err = l.afterConnect(ctx, lockConn)
	if err != nil {
		log.Error("msg", "Lock connection initialization failed", "err", err)
		return l.getConn(connStr, cur+1, maxRetries)
	}

	return lockConn, nil
}

func (l *PgAdvisoryLock) GetAdvisoryLock() (bool, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.getAdvisoryLock()
}

func (l *PgAdvisoryLock) getAdvisoryLock() (bool, error) {
	return l.runLockFunction("SELECT pg_try_advisory_lock($1)")
}

func (l *PgAdvisoryLock) GetSharedAdvisoryLock() (bool, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.getSharedAdvisoryLock()
}

func (l *PgAdvisoryLock) getSharedAdvisoryLock() (bool, error) {
	return l.runLockFunction("SELECT pg_try_advisory_lock_shared($1)")
}

func (l *PgAdvisoryLock) Conn() (*pgx.Conn, error) {
	err := l.ensureConnInit()
	if err != nil {
		return nil, err
	}
	return l.conn, nil
}

func (l *PgAdvisoryLock) runLockFunction(query string) (bool, error) {
	err := l.ensureConnInit()
	if err != nil {
		return false, err
	}
	return runLockFunction(context.Background(), l.conn, query, l.groupLockID)
}

func (l *PgAdvisoryLock) ensureConnInit() error {
	if l.conn != nil {
		return nil
	}
	conn, err := l.getConn(l.connStr, 0, 10)
	l.conn = conn
	return err
}

func (l *PgAdvisoryLock) Unlock() (bool, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.unlock()
}

func (l *PgAdvisoryLock) unlock() (bool, error) {
	return l.runLockFunction("SELECT pg_advisory_unlock($1)")
}

func (l *PgAdvisoryLock) UnlockShared() (bool, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.unlockShared()
}

func (l *PgAdvisoryLock) unlockShared() (bool, error) {
	return l.runLockFunction("SELECT pg_advisory_unlock_shared($1)")
}

func runLockFunction(ctx context.Context, conn *pgx.Conn, query string, lockId int64) (result bool, err error) {
	err = conn.QueryRow(ctx, query, lockId).Scan(&result)
	if err != nil {
		return false, fmt.Errorf("error while trying to read response rows from locking function: %w", err)
	}
	return result, nil
}

//Close cleans up the connection
func (l *PgAdvisoryLock) Close() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.connCleanUp()
}

func (l *PgAdvisoryLock) connCleanUp() {
	if l.conn != nil {
		if err := l.conn.Close(context.Background()); err != nil {
			log.Error("err", err)
		}
	}
	l.conn = nil
}

func checkConnection(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, "SELECT 1")
	if err != nil {
		return fmt.Errorf("invalid connection: %w", err)
	}
	return nil
}
