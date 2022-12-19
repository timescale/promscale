package end_to_end_tests

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgclient"
)

// TestWriterSynchronousCommit ensures that the database pool for the writer can be configured with synchronous_commit
// turned on or off and that the effects are limited to that pool
func TestWriterSynchronousCommit(t *testing.T) {

	// creates a new pool of database connections as would be created for the writer path
	createWriterPool := func(connStr string, lockerCalled *atomic.Bool, synchronousCommit bool) *pgxpool.Pool {
		pgConfig, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			t.Fatal(err)
		}
		pgConfig.MaxConns = 2
		pgConfig.MinConns = 1

		lockerCalled.Store(false)
		schemaLocker := func(ctx context.Context, conn *pgx.Conn) error {
			lockerCalled.Store(true)
			return nil
		}
		pgConfig.AfterConnect = pgclient.WriterPoolAfterConnect(schemaLocker, synchronousCommit)
		writerPool, err := pgxpool.NewWithConfig(context.Background(), pgConfig)
		if err != nil {
			t.Fatal(err)
		}
		return writerPool
	}

	// gets the value of the synchronous_commit setting from the database
	getSynchronousCommit := func(writerPool *pgxpool.Pool) string {
		var setting string
		err := writerPool.QueryRow(context.Background(), "show synchronous_commit").Scan(&setting)
		if err != nil {
			t.Fatal(err)
		}
		return setting
	}

	withDB(t, "writer_sync_commit", func(db *pgxpool.Pool, t testing.TB) {
		lockerCalled := &atomic.Bool{} // used to ensure that the schema locker function is still called

		// create a writer pool with synchronous_commit turned off
		writerPool1 := createWriterPool(db.Config().ConnString(), lockerCalled, false)
		setting := getSynchronousCommit(writerPool1)
		require.Equal(t, "off", setting, "expected synchronous_commit to be off but it was %s", setting)
		require.True(t, lockerCalled.Load(), "schemaLocker function should have been called and wasn't")

		// ensure that setting synchronous_commit to off on the writer pool did not impact the setting
		// in other database sessions
		setting = getSynchronousCommit(db)
		require.Equal(t, "on", setting, "expected synchronous_commit to be on but it was %s", setting)

		// make sure the setting stays off after the first transaction finished
		setting = getSynchronousCommit(writerPool1)
		require.Equal(t, "off", setting, "expected synchronous_commit to be off but it was %s", setting)
		writerPool1.Close()

		// now create a writer pool with synchronous_commit turned on
		lockerCalled.Store(false)
		writerPool2 := createWriterPool(db.Config().ConnString(), lockerCalled, true)

		// make sure the setting is on and the schema locker function was called
		setting = getSynchronousCommit(writerPool2)
		require.Equal(t, "on", setting, "expected synchronous_commit to be on but it was %s", setting)
		require.True(t, lockerCalled.Load(), "schemaLocker function should have been called and wasn't")
		writerPool2.Close()
	})
}
