package util

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/timescale/prometheus-postgresql-adapter/pkg/log"
	"strconv"
	"sync"
	"time"
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
	conn     *sql.Conn
	mutex    sync.RWMutex
	obtained bool
	connPool *sql.DB

	groupLockId int
}

func NewPgAdvisoryLock(groupLockId int, connPool *sql.DB) (*PgAdvisoryLock, error) {
	lock := &PgAdvisoryLock{connPool: connPool, obtained: false, groupLockId: groupLockId}
	_, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	return lock, nil
}

func getConn(pool *sql.DB, cur, maxRetries int) (*sql.Conn, error) {
	if maxRetries == cur {
		return nil, fmt.Errorf("max attempts reached. giving up on getting a db connection")
	}
	ctx, _ := context.WithTimeout(context.Background(), waitForConnectionTimeout)
	lockConn, err := pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting DB connection: %v", err)
	}
	err = checkConnection(lockConn)
	if err != nil {
		log.Error("msg", "Connection pool returned invalid connection", "err", err)
		return getConn(pool, cur+1, maxRetries)
	}
	return lockConn, nil
}

func (l *PgAdvisoryLock) Id() string {
	return strconv.Itoa(l.groupLockId)
}

func (l *PgAdvisoryLock) BecomeLeader() (bool, error) {
	return l.TryLock()
}

func (l *PgAdvisoryLock) IsLeader() (bool, error) {
	return l.Locked(), nil
}

func (l *PgAdvisoryLock) Resign() error {
	return l.Release()
}

func (l *PgAdvisoryLock) TryLock() (bool, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.obtained {
		err := checkConnection(l.conn)
		if err != nil {
			l.obtained = false
		} else {
			return l.obtained, nil
		}
	}
	var err error
	l.conn, err = getConn(l.connPool, 0, 10)
	defer func() {
		if !l.obtained {
			l.connCleanUp()
		}
	}()

	if err != nil {
		return false, err
	}
	rows, err := l.conn.QueryContext(context.Background(), "SELECT pg_try_advisory_lock($1)", l.groupLockId)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return false, fmt.Errorf("error while trying to read response rows from `pg_try_advisory_lock` function")
	}
	if err := rows.Scan(&l.obtained); err != nil {
		return false, err
	}
	if l.obtained {
		log.Debug("msg", fmt.Sprintf("Lock obtained for group id %d", l.groupLockId))
	}
	return l.obtained, nil
}

func (l *PgAdvisoryLock) connCleanUp() {
	if l.conn != nil {
		if err := l.conn.Close(); err != nil {
			log.Error("err", err)
		}
	}
	l.conn = nil
}

func (l *PgAdvisoryLock) Locked() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.obtained
}

func (l *PgAdvisoryLock) Release() error {
	l.mutex.Lock()
	if !l.obtained {
		return fmt.Errorf("can't release while not holding the lock")
	}
	defer l.mutex.Unlock()
	rows, err := l.conn.QueryContext(context.Background(), "SELECT pg_advisory_unlock($1)", l.groupLockId)
	if err != nil {
		return err
	}
	rows.Next()
	var success bool
	if err := rows.Scan(&success); err != nil {
		return err
	}
	if !success {
		return fmt.Errorf("failed to release a lock with group lock id: %v", l.groupLockId)
	}
	rows.Close()
	l.connCleanUp()
	l.obtained = false
	return nil
}

func checkConnection(conn *sql.Conn) error {
	_, err := conn.ExecContext(context.Background(), "SELECT 1")
	if err != nil {
		return fmt.Errorf("invalid connection: %v", err)
	}
	return nil
}
