// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package integration

import (
	"context"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/log"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
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

func TestMain(m *testing.M) {
	if err := log.Init(log.Config{Level: "info"}); err != nil {
		reportAndExit(err)
	}
	dir, err := testhelpers.TempDir("go_benchmark")
	if err != nil {
		reportAndExit(err)
	}
	benchDir = dir
	os.Exit(m.Run())
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

// terminateContainer terminates an active benchmarkPgContainer container. If the container is not active,
// it exits silently.
func terminateContainer() {
	if !containerActive {
		return
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

const benchDataPath = "../../testdata/bench_data"

func ingestBenchData(t testing.TB, db *pgxpool.Pool) {
	log.Info("msg", "ingesting bench dataset...")
	files, err := ioutil.ReadDir(benchDataPath)
	require.NoError(t, err)

	ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
	require.NoError(t, err)

	for _, f := range files {
		ts := readTimeseries(t, f)
		log.Info("msg", "ingesting dump", "name", f.Name(), "num-series", len(ts))
		numInsertables, _, err := ingestor.Ingest(common.NewWriteRequestWithTs(ts))
		require.NoError(t, err)
		require.NotEqual(t, 0, int(numInsertables))
	}

	log.Info("msg", "completed ingesting bench dataset")
}

func readTimeseries(t testing.TB, f fs.FileInfo) []prompb.TimeSeries {
	name := benchDataPath + "/" + f.Name()
	bSlice, err := ioutil.ReadFile(name)
	require.NoError(t, err)

	qry := &prompb.QueryResult{}
	err = qry.Unmarshal(bSlice)
	require.NoError(t, err)

	ts := make([]prompb.TimeSeries, len(qry.Timeseries))

	for i := range qry.Timeseries {
		ts[i] = *qry.Timeseries[i]
	}

	return ts
}
