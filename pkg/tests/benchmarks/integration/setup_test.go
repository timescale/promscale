// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package integration

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/log"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tests/common"
)

var (
	benchmarkPgContainer testcontainers.Container
	containerCloser      io.Closer
	containerActive      bool
	benchDir             string

	extensionState = testhelpers.Timescale2AndPromscale
	benchDatabase  = "tmp_db_promscale_go_benchmark"
)

func MainTest(m *testing.M) {
	if err := log.Init(log.Config{Level: "info"}); err != nil {
		reportAndExit(err)
	}
	dir, err := testhelpers.TempDir("go_benchmark")
	if err != nil {
		reportAndExit(err)
	}
	benchDir = dir
}

// startContainer clean starts the container, and fills new data, so that benchmarking is done fresh
// and no previous benchmark affects the persistent data.
// It must never be called concurrently. Call to a start container should be done only on serial
// execution of benchmarks.
//
// Todo: This can be optimized to change the template-0 database to have real-dataset and then creating database
// before a benchmark, and deleting it after that benchmark is done. This will do the same thing, but avoid costly
// container restarts or the ingestion of data.
func startContainer() {
	container, closer, err := testhelpers.StartPGContainer(context.Background(), extensionState, benchDir, false)
	if err != nil {
		reportAndExit(err)
	}
	benchmarkPgContainer = container
	containerCloser = closer
	containerActive = true
}

// prepareContainer fills the pg container with initial data, so that the database
// is non-empty before being benchmarked.
func prepareContainer(t testing.TB) {
	ts := common.GeneratePromLikeLargeTimeseries()
	db := testhelpers.PgxPoolWithSuperuser(t, benchDatabase)

	ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
	require.NoError(t, err)

	numInsertables, numMetadata, err := ingestor.Ingest(common.NewWriteRequestWithTs(ts))
	require.NoError(t, err)
	require.NotEqual(t, 0, int(numInsertables))
	require.Equal(t, 0, int(numMetadata))
}

func terminateContainer() {
	if !containerActive {
		log.Fatal("msg", "cannot terminate an inactive container")
	}
	if err := benchmarkPgContainer.Terminate(context.Background()); err != nil {
		reportAndExit(err)
	}
	if err := containerCloser.Close(); err != nil {
		reportAndExit(err)
	}
}

func withDB(t testing.TB, DBName string, benchFunc func(db *pgxpool.Pool, t testing.TB)) {
	testhelpers.WithDB(t, DBName, true, false, extensionState, func(_ *pgxpool.Pool, t testing.TB, connectURL string) {
		common.PerformMigrate(t, connectURL, testhelpers.PgConnectURL(DBName, testhelpers.Superuser), true, true)
		pool, err := pgxpool.Connect(context.Background(), connectURL)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		benchFunc(pool, t)
	})
}

func reportAndExit(err error) {
	log.Error("msg", err)
	os.Exit(1)
}
