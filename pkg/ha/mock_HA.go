package ha

import (
	"github.com/timescale/promscale/pkg/pgxconn"
	"time"
)

type mockHaWriter struct {
	DBClient        pgxconn.PgxConn
}

func NewMockHAState(dbClient *pgxconn.PgxConn) *State {
	return &State{
		writer: newMockHAWriter(dbClient),
	}
}

func newMockHAWriter(dbClient *pgxconn.PgxConn) *mockHaWriter {
	return &mockHaWriter{
		DBClient: *dbClient,
	}
}

func (h *mockHaWriter) readHALocks(minT, maxT time.Time, clusterName string) (cluster, leader string, leaseStart, leaseUntil time.Time) {
	return "leader1", "replica1", time.Time{}, time.Now().Add(2* time.Minute)
}

func (h *mockHaWriter) changeLeader(minT, maxT time.Time, clusterName string) (leader string, leaseStart, leaseUntil time.Time) {
	return "leader1", time.Time{}, time.Time{}
}
