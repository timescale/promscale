// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"context"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/timescale/promscale/pkg/pgxconn"
)

func TestRegisterMetric(t *testing.T) {
	metric := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "test", Name: "extraction"})

	engine := &engineImpl{}
	_, has := engine.metrics.Load("some_stats")
	require.False(t, has)

	require.NoError(t, engine.RegisterMetric("some_stats", metric))

	_, has = engine.metrics.Load("some_stats")
	require.True(t, has)

	wrongMetric := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "test", Name: "wrong", Buckets: prometheus.DefBuckets})
	wrongMetric.Observe(164)

	require.Error(t, engine.RegisterMetric("some_wrong_stats", wrongMetric))

	_, has = engine.metrics.Load("some_wrong_stats")
	require.False(t, has)
}

func TestEngineStop(t *testing.T) {
	engine := &engineImpl{
		conn: mockPgxConn{},
	}
	engine.Start()
	engine.Stop()
}

type mockRow struct{}

func (mockRow) Scan(dest ...interface{}) error { return nil }

type mockPgxConn struct{}

func (mockPgxConn) Close() {}
func (mockPgxConn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (mockPgxConn) Query(ctx context.Context, sql string, args ...interface{}) (pgxconn.PgxRows, error) {
	return nil, nil
}
func (mockPgxConn) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return mockRow{}
}
func (mockPgxConn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (mockPgxConn) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource { return nil }
func (mockPgxConn) NewBatch() pgxconn.PgxBatch                           { return nil }
func (mockPgxConn) SendBatch(ctx context.Context, b pgxconn.PgxBatch) (pgx.BatchResults, error) {
	return nil, nil
}
func (mockPgxConn) Acquire(ctx context.Context) (*pgxpool.Conn, error) { return nil, nil }
func (mockPgxConn) BeginTx(ctx context.Context) (pgx.Tx, error)        { return nil, nil }
