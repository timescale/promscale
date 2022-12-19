// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package util

import (
	"context"
	"fmt"
	"sync"
	"time"

	pgx "github.com/jackc/pgx/v5"

	"github.com/timescale/promscale/pkg/log"
)

const defaultConnectionTimeout = time.Minute

var SharedLeaseFailure = fmt.Errorf("failed to acquire shared lease")

func GetSharedLease(ctx context.Context, conn *pgx.Conn, id int64) error {
	gotten, err := runLockFunction(ctx, conn, "SELECT pg_try_advisory_lock_shared($1)", id)
	if err != nil {
		return fmt.Errorf("Unable to get shared schema lock. Please make sure that no operations requiring exclusive locking are running: %w", err)
	}
	if !gotten {
		return SharedLeaseFailure
	}
	return nil
}

type AfterConnectFunc = func(ctx context.Context, conn *pgx.Conn) error

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

	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("error parsing config connection: %w", err)
	}

	ctx := context.Background()
	if cfg.ConnectTimeout.Seconds() == 0 {
		// Set the defaultConnectionTimeout if the connection string does not contain the
		// the connection timeout information.
		cctx, cancel := context.WithTimeout(context.Background(), defaultConnectionTimeout)
		defer cancel()
		ctx = cctx
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

// Close cleans up the connection
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
