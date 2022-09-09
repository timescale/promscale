package querier

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func TestDecideRollup(t *testing.T) {
	r := &rollupDecider{
		conn: mockPgxConn{},
		cache: map[string]time.Duration{
			"hour":      time.Hour,
			"5_minute":  5 * time.Minute,
			"15_minute": 15 * time.Minute,
			"week":      7 * 24 * time.Hour,
		},
		resolutionInASCOrder: []time.Duration{5 * time.Minute, 15 * time.Minute, time.Hour, 7 * 24 * time.Hour},
	}
	tcs := []struct {
		name               string
		min                time.Duration
		max                time.Duration
		expectedSchemaName string
	}{
		{
			name:               "1 sec",
			min:                0,
			max:                time.Second,
			expectedSchemaName: noRollupSchema, // raw resolution.
		}, {
			name:               "5 min",
			min:                0,
			max:                5 * time.Minute,
			expectedSchemaName: noRollupSchema,
		}, {
			name:               "30 mins",
			min:                0,
			max:                30 * time.Minute,
			expectedSchemaName: noRollupSchema,
		}, {
			name:               "1 hour",
			min:                0,
			max:                time.Hour,
			expectedSchemaName: noRollupSchema,
		}, {
			name:               "1 day",
			min:                0,
			max:                24 * time.Hour,
			expectedSchemaName: "5_minute",
		}, {
			name:               "7 days",
			min:                0,
			max:                7 * 24 * time.Hour,
			expectedSchemaName: "15_minute",
		}, {
			name:               "30 days",
			min:                0,
			max:                30 * 24 * time.Hour,
			expectedSchemaName: "hour",
		}, {
			name:               "1 year",
			min:                0,
			max:                12 * 30 * 24 * time.Hour,
			expectedSchemaName: "week",
		}, {
			name:               "100 years",
			min:                0,
			max:                100 * 12 * 30 * 24 * time.Hour,
			expectedSchemaName: "week",
		},
	}
	for _, tc := range tcs {
		recommendedSchema := r.decide(int64(tc.min.Seconds()), int64(tc.max.Seconds()))
		require.Equal(t, tc.expectedSchemaName, recommendedSchema, tc.name)
	}
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
