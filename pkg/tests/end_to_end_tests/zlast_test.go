// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

// This file contains tests that should be executed towards the end of the end-to-end tests. This is because the behaviour
// of these tests are database related and due to the design of the tests system, running these tests will disturb the global
// variables in main_test.go and hence disturb all other tests. Example: running the TestDeleteMetricSQLAPI will lead to changes
// in the database and hence affect the golden files tests. Hence, its better to execute such tests towards the last.

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tests/common"
	"github.com/timescale/promscale/pkg/tests/upgrade_tests"
)

func TestDeleteMetricSQLAPI(t *testing.T) {
	var (
		testDir   = pgContainerTestDataDir
		container = pgContainer
	)
	withDB(t, *testDatabase, func(db *pgxpool.Pool, tb testing.TB) {
		ts := common.GenerateLargeTimeseries()
		if *extendedTest {
			ts = common.GenerateRealTimeseries()
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		if _, _, err := ingestor.Ingest(newWriteRequestWithTs(copyMetrics(ts))); err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation()
		if err != nil {
			t.Fatal(err)
		}
		startSnapShot := upgrade_tests.GetDbInfoIgnoringTable(t, container, *testDatabase, testDir, db, "", "label", extensionState)
		tts := common.GenerateSmallTimeseries()
		if _, _, err := ingestor.Ingest(newWriteRequestWithTs(copyMetrics(tts))); err != nil {
			t.Fatal(err)
		}
		snapShotAfterNewMetrics := upgrade_tests.GetDbInfoIgnoringTable(t, container, *testDatabase, testDir, db, "", "label", extensionState)
		if reflect.DeepEqual(startSnapShot, snapShotAfterNewMetrics) {
			t.Fatal("start=snapshot and snapshot-after-new-metric-ingestion should not be equal")
		}
		_, err = db.Exec(context.Background(), "SELECT prom_api.drop_metric('firstMetric')")
		if err != nil {
			t.Fatalf("err executing delete query: %v", err)
		}
		_, err = db.Exec(context.Background(), "SELECT prom_api.drop_metric('secondMetric')")
		if err != nil {
			t.Fatalf("err executing delete query: %v", err)
		}
		endSnapShot := upgrade_tests.GetDbInfoIgnoringTable(t, container, *testDatabase, testDir, db, "", "label", extensionState)
		if !reflect.DeepEqual(startSnapShot, endSnapShot) {
			upgrade_tests.PrintDbSnapshotDifferences(t, startSnapShot, endSnapShot)
			t.Fatal("start-snapshot and end-snapshot should be equal")
		}
	})
}
