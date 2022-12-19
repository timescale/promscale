// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package upgrade_tests

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	constants "github.com/timescale/promscale/pkg/tests"

	"github.com/blang/semver/v4"
	"github.com/docker/go-connections/nat"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	tput "github.com/timescale/promscale/pkg/util/throughput"
	"github.com/timescale/promscale/pkg/version"
)

var (
	testDatabase = flag.String("database", "tmp_db_timescale_upgrade_test", "database to run integration tests on")
	printLogs    = flag.Bool("print-logs", false, "print TimescaleDB logs")
	// use "local/dev_promscale_extension:head-ts2-pg13" for local testing
	dockerImage        = flag.String("timescale-docker-image", constants.PromscaleExtensionContainer, "TimescaleDB docker image for latest version to run tests against")
	baseExtensionState testhelpers.TestOptions
)

func init() {
	tput.InitWatcher(0)
}

func TestMain(m *testing.M) {
	var code int
	flag.Parse()
	baseExtensionState.UseTimescaleDB()
	baseExtensionState.SetTimescaleDockerImage(*dockerImage)
	if err := os.Setenv("IS_TEST", "true"); err != nil {
		// E2E tests calls prometheus.MustRegister() more than once in clockcache,
		// hence, we set this environment variable to have a different behaviour
		// while assigning registerer in clockcache metrics.
		panic(err)
	}
	_ = log.Init(log.Config{
		Level: "debug",
	})
	code = m.Run()
	os.Exit(code)
}

/* Prev image is the db image with the old promscale extension. We do NOT test timescaleDB extension upgrades here. */
func getDBImages(extensionState testhelpers.TestOptions, prevPromscaleVersion *semver.Version) (prev string, clean string, err error) {
	// TODO: Extracting the pg version from the docker image name is a nasty hack. How can we avoid this?
	dockerImageName := extensionState.GetDockerImageName()
	pgVersion, err := extensionState.TryGetDockerImagePgVersion()
	if err != nil {
		panic("unable to get docker image version")
	}

	if pgVersion != "12" {
		//using the oldest supported version of PG seems sufficient.
		//we don't want to use any features in a newer PG version that isn't available in an older one
		//but migration code that works in an older PG version should generally work in a newer one.
		panic("Only use pg12 for upgrade tests")
	}

	// From Promscale 0.17.0 onwards the minimum extension version is 0.8.0
	if prevPromscaleVersion != nil && prevPromscaleVersion.GE(semver.MustParse("0.17.0")) {
		return "timescale/timescaledb-ha:pg" + pgVersion + ".13-ts2.9.1-latest", dockerImageName, nil
	}

	// From Promscale 0.15.0 to 0.16.0 the minimum extension version is 0.7.0
	if prevPromscaleVersion != nil && prevPromscaleVersion.GE(semver.MustParse("0.15.0")) {
		return "timescale/timescaledb-ha:pg" + pgVersion + ".12-ts2.8.1-latest", dockerImageName, nil
	}

	return "timescale/timescaledb-ha:pg" + pgVersion + "-ts2.1-latest", dockerImageName, nil
}

func writeToFiles(t *testing.T, upgradedDbInfo, pristineDbInfo dbSnapshot) error {
	dir, err := os.MkdirTemp("", "upgrade_test")
	if err != nil {
		return err
	}
	t.Logf("writing upgraded and pristine output to dir: %s", dir)
	upgradedPath := filepath.Join(dir, "upgraded.txt")
	pristinePath := filepath.Join(dir, "pristine.txt")
	w := func(path string, snapshot dbSnapshot) error {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		return snapshot.writeToFile(f)
	}
	if err := w(upgradedPath, upgradedDbInfo); err != nil {
		return err
	}
	return w(pristinePath, pristineDbInfo)
}

// We test that upgrading from both the earliest and the directly-previous versions works
// While it may seem that the earliest version is sufficient, idempotent scripts are only
// run on each completed updated and so testing the upgrade as it relates to the last idempotent
// state is important. To see why we need both tests, think of the following example:
//
// Say you have an earliest version 1 and a new version 3. In version 2 you introduce procedure foo(). that you
// drop in version 3.
// DROP FUNCTION IF EXISTS foo() (wrong since foo is procedure not function), would pass the earlier->latest test since
// version 1 has no function foo, and would only be caught in prev->latest test.
// DROP PROCEDURE foo() (wrong since missing IF NOT EXISTS), would pass the prev->latest test but would be caught in the
// earliest->latest test.
func TestUpgradeFromPrev(t *testing.T) {
	upgradedDbInfo := getUpgradedDbInfo(t, false, version.PrevReleaseVersion, baseExtensionState)
	pristineDbInfo := getPristineDbInfo(t, false, baseExtensionState)
	err := writeToFiles(t, upgradedDbInfo, pristineDbInfo)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pristineDbInfo, upgradedDbInfo) {
		PrintDbSnapshotDifferences(t, pristineDbInfo, upgradedDbInfo)
	}
}

func TestUpgradeFromEarliest(t *testing.T) {
	upgradedDbInfo := getUpgradedDbInfo(t, false, version.EarliestUpgradeTestVersion, baseExtensionState)
	pristineDbInfo := getPristineDbInfo(t, false, baseExtensionState)
	err := writeToFiles(t, upgradedDbInfo, pristineDbInfo)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pristineDbInfo, upgradedDbInfo) {
		PrintDbSnapshotDifferences(t, pristineDbInfo, upgradedDbInfo)
	}
}

// niksa: we need to fix promscale extension to work with multinode before bringing this test back
// func TestUpgradeFromEarliestMultinode(t *testing.T) {
// 	extState := baseExtensionState
// 	extState.UseMultinode()
// 	upgradedDbInfo := getUpgradedDbInfo(t, false, true, extState)
// 	pristineDbInfo := getPristineDbInfo(t, false, extState)
// 	err := writeToFiles(t, upgradedDbInfo, pristineDbInfo)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if !reflect.DeepEqual(pristineDbInfo, upgradedDbInfo) {
// 		PrintDbSnapshotDifferences(t, pristineDbInfo, upgradedDbInfo)
// 	}
// }

// TestUpgradeFromPrevNoData tests migrations with no ingested data.
// See issue: https://github.com/timescale/promscale/issues/330
func TestUpgradeFromEarliestNoData(t *testing.T) {
	upgradedDbInfo := getUpgradedDbInfo(t, true, version.EarliestUpgradeTestVersion, baseExtensionState)
	pristineDbInfo := getPristineDbInfo(t, true, baseExtensionState)
	err := writeToFiles(t, upgradedDbInfo, pristineDbInfo)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pristineDbInfo, upgradedDbInfo) {
		PrintDbSnapshotDifferences(t, pristineDbInfo, upgradedDbInfo)
	}
}

func turnOffCompressionOnMetric(t *testing.T) {
	db, err := pgxpool.New(context.Background(), testhelpers.PgConnectURL(*testDatabase, testhelpers.NoSuperuser))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(context.Background(), "SELECT set_compression_on_metric_table('test_uncompressed', false);")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func getUpgradedDbInfo(t *testing.T, noData bool, prevVersionStr string, extensionState testhelpers.TestOptions) (upgradedDbInfo dbSnapshot) {
	prevVersion := semver.MustParse(prevVersionStr)
	// TODO we could probably improve performance of this test by 2x if we
	//      gathered the db info in parallel. Unfortunately our db runner doesn't
	//      support this yet
	withDBStartingAtOldVersionAndUpgrading(t, *testDatabase, prevVersion, extensionState,
		/* preUpgrade */
		func(dbContainer testcontainers.Container, dbTmpDir string, connectorHost string, connectorPort nat.Port) {
			if noData {
				return
			}
			client := http.Client{}
			defer client.CloseIdleConnections()

			writeUrl := fmt.Sprintf("http://%s/write", net.JoinHostPort(connectorHost, connectorPort.Port()))

			doWrite(t, &client, writeUrl, preUpgradeData1, preUpgradeData2, preUpgradeData3)
			turnOffCompressionOnMetric(t)
		},
		/* postUpgrade */
		func(dbContainer testcontainers.Container, dbTmpDir string) {
			if !noData {
				func() {
					connectURL := testhelpers.PgConnectURL(*testDatabase, testhelpers.NoSuperuser)
					db, err := testhelpers.PgxPoolWithRegisteredTypes(connectURL)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()

					ing, err := ingestor.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
					if err != nil {
						t.Fatalf("error connecting to DB: %v", err)
					}

					doIngest(t, ing, postUpgradeData1, postUpgradeData2)

					ing.Close()
				}()
			}

			connectURL := testhelpers.PgConnectURL(*testDatabase, testhelpers.Superuser)
			db, err := testhelpers.PgxPoolWithRegisteredTypes(connectURL)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			upgradedDbInfo = SnapshotDB(t, dbContainer, *testDatabase, dbTmpDir, db, extensionState)
		})
	return
}

func getPristineDbInfo(t *testing.T, noData bool, extensionState testhelpers.TestOptions) (pristineDbInfo dbSnapshot) {
	withNewDBAtCurrentVersion(t, *testDatabase, extensionState,
		/* preRestart */
		func(container testcontainers.Container, _ string, db *pgxpool.Pool, tmpDir string) {
			if noData {
				return
			}
			ing, err := ingestor.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
			if err != nil {
				t.Fatalf("error connecting to DB: %v", err)
			}
			defer ing.Close()

			doIngest(t, ing, preUpgradeData1, preUpgradeData2, preUpgradeData3)
		},
		/* postRestart */
		func(container testcontainers.Container, _ string, db *pgxpool.Pool, tmpDir string) {
			if !noData {
				func() {
					connectURL := testhelpers.PgConnectURL(*testDatabase, testhelpers.NoSuperuser)
					db, err := testhelpers.PgxPoolWithRegisteredTypes(connectURL)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()

					ing, err := ingestor.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
					if err != nil {
						t.Fatalf("error connecting to DB: %v", err)
					}
					defer ing.Close()

					doIngest(t, ing, postUpgradeData1, postUpgradeData2)
				}()
			}
			pristineDbInfo = SnapshotDB(t, container, *testDatabase, tmpDir, db, extensionState)
		})
	return
}

// pick a start time in the future so data won't get compressed
const startTime = 6600000000000 // approx 210 years after the epoch
var (
	preUpgradeData1 = []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "test"},
				{Name: "test", Value: "test"},
			},
			Samples: []prompb.Sample{
				{Timestamp: startTime + 1, Value: 0.1},
				{Timestamp: startTime + 2, Value: 0.2},
			},
		},
	}
	preUpgradeData2 = []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "test2"},
				{Name: "foo", Value: "bar"},
			},
			Samples: []prompb.Sample{
				{Timestamp: startTime + 4, Value: 2.2},
			},
		},
	}

	preUpgradeData3 = []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "test_uncompressed"},
				{Name: "foo", Value: "bar"},
			},
			Samples: []prompb.Sample{
				{Timestamp: startTime + 4, Value: 2.2},
			},
		},
	}

	postUpgradeData1 = []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "test"},
				{Name: "testB", Value: "testB"},
			},
			Samples: []prompb.Sample{
				{Timestamp: startTime + 4, Value: 0.4},
				{Timestamp: startTime + 5, Value: 0.5},
			},
		},
	}
	postUpgradeData2 = []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "test3"},
				{Name: "baz", Value: "quf"},
			},
			Samples: []prompb.Sample{
				{Timestamp: startTime + 66, Value: 6.0},
			},
		},
	}
)

func addNode2(t testing.TB, DBName string) {
	db, err := pgx.Connect(context.Background(), testhelpers.PgConnectURL(DBName, testhelpers.Superuser))
	if err != nil {
		t.Fatal(err)
	}
	err = testhelpers.AddDataNode2(db, DBName)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(context.Background(), "CALL add_prom_node('dn1');")
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Start a db with the prev extra extension and a prev connector as well.
// This ensures that we test upgrades of both the extension and the connector schema.
// Then run preUpgrade and shut everything down.
// Start a new db with the latest extra extension and migrate to the latest version of the connector schema.
// Then run postUpgrade.
func withDBStartingAtOldVersionAndUpgrading(
	t testing.TB,
	DBName string,
	prevVersion semver.Version,
	extensionState testhelpers.TestOptions,
	preUpgrade func(dbContainer testcontainers.Container, dbTmpDir string, connectorHost string, connectorPort nat.Port),
	postUpgrade func(dbContainer testcontainers.Container, dbTmpDir string)) {
	var err error
	ctx := context.Background()

	tmpDir, err := testhelpers.TempDir("update_test_out")
	if err != nil {
		log.Fatal(err)
	}

	dataDir, err := testhelpers.TempDir("update_test_data")
	if err != nil {
		log.Fatal(err)
	}

	ver, err := extensionState.TryGetDockerImagePgVersion()
	if err != nil {
		t.Fatal(err)
	} else {
		if ver == "14" {
			t.Skip("cannot test upgrade on pg14")
		}
	}

	prevDBImage, cleanImage, err := getDBImages(extensionState, &prevVersion)
	if err != nil {
		t.Fatal("unable to get db image", err)
	}
	// Start a db with the prev extension and a prev connector as well
	// Then run preUpgrade and shut everything down.
	func() {
		dbContainer, closer, err := testhelpers.StartDatabaseImage(ctx, t, prevDBImage, tmpDir, dataDir, *printLogs, extensionState)
		if err != nil {
			t.Fatal("Error setting up container", err)
		}

		defer func() { _ = closer.Close() }()

		db, err := testhelpers.DbSetup(*testDatabase, testhelpers.NoSuperuser, true, extensionState)
		if err != nil {
			t.Fatal(err)
			return
		}
		db.Close()

		connectorImage := "timescale/promscale:" + prevVersion.String()
		connector, err := testhelpers.StartConnectorWithImage(context.Background(), dbContainer, connectorImage, prevVersion, *printLogs, []string{}, *testDatabase)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer testhelpers.StopContainer(ctx, connector, *printLogs, t)

		connectorHost, err := connector.Host(ctx)
		if err != nil {
			t.Fatal(err)
			return
		}

		connectorPort, err := connector.MappedPort(ctx, testhelpers.ConnectorPort)
		if err != nil {
			t.Fatal(err)
			return
		}
		t.Logf("Running preUpgrade with old version of connector and db: connector=%v db=%v", connectorImage, prevDBImage)
		preUpgrade(dbContainer, tmpDir, connectorHost, connectorPort)
	}()

	//Start a new connector and migrate.
	//Then run postUpgrade
	dbContainer, closer, err := testhelpers.StartDatabaseImage(ctx, t, cleanImage, tmpDir, dataDir, *printLogs, extensionState)
	if err != nil {
		t.Fatal("Error setting up container", err)
	}

	defer func() { _ = closer.Close() }()

	t.Logf("upgrading versions %v => %v", prevVersion, version.Promscale)
	connectURL := testhelpers.PgConnectURL(*testDatabase, testhelpers.NoSuperuser)
	migrateToVersion(t, connectURL, version.Promscale, "azxtestcommit")
	testhelpers.MakePromUserPromAdmin(t, DBName)

	if extensionState.UsesMultinode() {
		//add a node after upgrade; this tests strictly more functionality since we already have one node set up before
		addNode2(t, *testDatabase)
	}
	t.Log("Running postUpgrade")
	postUpgrade(dbContainer, tmpDir)

}

// Run a DB and connector at the current version. Run preRestart then restart the db
// then run postRestart. A restart is necessary because we need a restart in the
// upgrade path to change the extension that is available. But, a restart causes
// Sequences to skip values. So, in order to have equivalent data, we need to make
// sure that both the upgrade and this pristine path both have restarts.
func withNewDBAtCurrentVersion(t testing.TB, DBName string, extensionState testhelpers.TestOptions,
	preRestart func(container testcontainers.Container, connectURL string, db *pgxpool.Pool, tmpDir string),
	postRestart func(container testcontainers.Container, connectURL string, db *pgxpool.Pool, tmpDir string)) {
	var err error
	ctx := context.Background()

	tmpDir, err := testhelpers.TempDir("update_test_out")
	if err != nil {
		log.Fatal(err)
	}
	dataDir, err := testhelpers.TempDir("update_test_data")
	if err != nil {
		log.Fatal(err)
	}

	_, cleanImage, err := getDBImages(extensionState, nil)
	if err != nil {
		t.Fatal("unable to get docker image", err)
	}

	func() {
		container, closer, err := testhelpers.StartDatabaseImage(ctx, t, cleanImage, tmpDir, dataDir, *printLogs, extensionState)
		if err != nil {
			fmt.Println("Error setting up container", err)
			os.Exit(1)
		}

		defer func() { _ = closer.Close() }()
		testhelpers.WithDB(t, DBName, testhelpers.NoSuperuser, true, extensionState, func(_ *pgxpool.Pool, t testing.TB, connectURL string) {
			migrateToVersion(t, connectURL, version.Promscale, "azxtestcommit")

			testhelpers.MakePromUserPromAdmin(t, DBName)

			// need to get a new pool after the Migrate to catch any GUC changes made during Migrate
			db, err := testhelpers.PgxPoolWithRegisteredTypes(connectURL)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			preRestart(container, connectURL, db, tmpDir)
		})
	}()
	container, closer, err := testhelpers.StartDatabaseImage(ctx, t, cleanImage, tmpDir, dataDir, *printLogs, extensionState)
	if err != nil {
		fmt.Println("Error setting up container", err)
		os.Exit(1)
	}

	if extensionState.UsesMultinode() {
		addNode2(t, *testDatabase)
	}
	defer func() { _ = closer.Close() }()
	connectURL := testhelpers.PgConnectURL(*testDatabase, testhelpers.Superuser)
	db, err := pgxpool.New(context.Background(), connectURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	postRestart(container, connectURL, db, tmpDir)
}

func migrateToVersion(t testing.TB, connectURL string, version string, commitHash string) {
	err := extension.InstallUpgradeTimescaleDBExtensions(connectURL, extension.ExtensionMigrateOptions{Install: true, Upgrade: true, UpgradePreRelease: true})
	if err != nil {
		t.Fatal(err)
	}

	migratePool, err := pgx.Connect(context.Background(), connectURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = migratePool.Close(context.Background()) }()
	err = pgmodel.Migrate(migratePool, pgmodel.VersionInfo{Version: version, CommitHash: commitHash}, nil, extension.ExtensionMigrateOptions{Install: true, Upgrade: true, UpgradePreRelease: true})
	if err != nil {
		t.Fatal(err)
	}
}

func tsWriteReq(ts []prompb.TimeSeries) prompb.WriteRequest {
	return prompb.WriteRequest{
		Timeseries: ts,
	}
}

func writeReqToHttp(r prompb.WriteRequest) *bytes.Reader {
	data, _ := proto.Marshal(&r)
	body := snappy.Encode(nil, data)
	return bytes.NewReader(body)
}

func doWrite(t *testing.T, client *http.Client, url string, data ...[]prompb.TimeSeries) {
	for _, data := range data {
		body := writeReqToHttp(tsWriteReq(copyMetrics(data)))
		req, err := http.NewRequest("POST", url, body)
		if err != nil {
			t.Errorf("Error creating request: %v", err)
		}
		req.Header.Add("Content-Encoding", "snappy")
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 {
			t.Fatal("non-ok status:", resp.Status)
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func doIngest(t *testing.T, ingstr *ingestor.DBIngestor, data ...[]prompb.TimeSeries) {
	for _, data := range data {
		wr := ingestor.NewWriteRequest()
		wr.Timeseries = copyMetrics(data)
		_, _, err := ingstr.IngestMetrics(context.Background(), wr)
		if err != nil {
			t.Fatalf("ingest error: %v", err)
		}
		_ = ingstr.CompleteMetricCreation(context.Background())
	}
}

// deep copy the metrics since we mutate them, and don't want to invalidate the tests
func copyMetrics(metrics []prompb.TimeSeries) []prompb.TimeSeries {
	out := make([]prompb.TimeSeries, len(metrics))
	copy(out, metrics)
	for i := range out {
		out[i].Labels = make([]prompb.Label, len(metrics[i].Labels))
		out[i].Samples = make([]prompb.Sample, len(metrics[i].Samples))
		copy(out[i].Labels, metrics[i].Labels)
		copy(out[i].Samples, metrics[i].Samples)
	}
	return out
}

func TestExtensionUpgrade(t *testing.T) {
	var err error
	var ver string

	if true {
		t.Skip("Temporarily disabled test")
	}

	ctx := context.Background()

	buildPromscaleImageFromRepo(t)
	_, dbContainer, closer := startDB(t, ctx)
	defer closer.Close()

	// as the default installed version ext is rc4 in the test image downgrade it to rc2
	// to test upgrade flow.
	extVersion := "2.0.0-rc2"
	dropAndCreateExt(t, ctx, extVersion)

	db, err := pgx.Connect(ctx, testhelpers.PgConnectURL("postgres", testhelpers.Superuser))
	if err != nil {
		t.Fatal(err)
	}

	err = db.QueryRow(ctx, `SELECT extversion FROM pg_extension where extname='timescaledb'`).Scan(&ver)
	if err != nil {
		t.Fatal(err)
	}

	if ver != extVersion {
		t.Fatal("failed to verify upgrade extension with -upgrade-prerelease-extension false")
	}

	// start promscale & test upgrade-prerelease-extensions as false
	// Now the ext is rc2 it should be rc2 after promscale startup too
	func() {
		connectorImage := "timescale/promscale:latest"
		databaseName := "postgres"
		connector, err := testhelpers.StartConnectorWithImage(ctx, dbContainer, connectorImage, semver.MustParse(version.Promscale), *printLogs, []string{}, databaseName)
		if err != nil {
			t.Fatal(err)
		}
		defer testhelpers.StopContainer(ctx, connector, *printLogs, t)
		err = db.QueryRow(ctx, `SELECT extversion FROM pg_extension where extname='timescaledb'`).Scan(&ver)
		if err != nil {
			t.Fatal(err)
		}

		if ver != extVersion {
			t.Fatal("failed to verify upgrade extension with -upgrade-prerelease-extension false")
		}
		t.Logf("successfully tested extension upgrade flow with --upgrade-prereleases-extensions false")
	}()

	db.Close(ctx)

	// start a new connector and test --upgrade-prerelease-extensions as true
	// the default installed ext ver is rc2 now it should upgrade it to rc4
	func() {
		connectorImage := "timescale/promscale:latest"
		databaseName := "postgres"
		flags := []string{"-upgrade-prerelease-extensions", "true"}
		connector, err := testhelpers.StartConnectorWithImage(ctx, dbContainer, connectorImage, semver.MustParse(version.Promscale), *printLogs, flags, databaseName)
		if err != nil {
			t.Fatal(err)
		}
		defer testhelpers.StopContainer(ctx, connector, *printLogs, t)

		var versionStr string
		db, err = pgx.Connect(ctx, testhelpers.PgConnectURL("postgres", testhelpers.Superuser))
		if err != nil {
			t.Fatal(err)
		}
		err = db.QueryRow(ctx, `SELECT extversion FROM pg_extension where extname='timescaledb'`).Scan(&versionStr)
		if err != nil {
			t.Fatal(err)
		}

		db.Close(ctx)

		if versionStr != "2.0.0-rc4" {
			t.Fatal("failed to verify upgrade extension with -upgrade-prerelease-extension true")
		}
		t.Logf("successfully tested extension upgrade flow with --upgrade-prereleases-extensions true")
	}()
}

func TestMigrationFailure(t *testing.T) {
	var err error
	var ver string

	if true {
		t.Skip("Temporarily disabled test")
	}
	ctx := context.Background()

	buildPromscaleImageFromRepo(t)
	_, dbContainer, closer := startDB(t, ctx)
	defer closer.Close()

	// As the timescaleDB installed version is rc4, lets install the 1.7.3 ext version
	extVersion := "1.7.3"
	dropAndCreateExt(t, ctx, extVersion)

	db, err := pgx.Connect(ctx, testhelpers.PgConnectURL("postgres", testhelpers.Superuser))
	if err != nil {
		t.Fatal(err)
	}

	err = db.QueryRow(ctx, `SELECT extversion FROM pg_extension where extname='timescaledb'`).Scan(&ver)
	if err != nil {
		t.Fatal(err)
	}

	db.Close(ctx)
	if ver != extVersion {
		t.Fatal("failed to verify upgrade extension with -upgrade-prerelease-extension false")
	}

	// start promscale & test upgrade-extensions as false
	func() {
		connectorImage := "timescale/promscale:latest"
		databaseName := "postgres"
		flags := []string{"-upgrade-extensions", "false"}
		connector, err := testhelpers.StartConnectorWithImage(ctx, dbContainer, connectorImage, semver.MustParse(version.Promscale), *printLogs, flags, databaseName)
		if err != nil {
			t.Fatal(err)
		}
		defer testhelpers.StopContainer(ctx, connector, *printLogs, t)

		db, err = pgx.Connect(ctx, testhelpers.PgConnectURL("postgres", testhelpers.Superuser))
		if err != nil {
			t.Fatal(err)

		}
		err = db.QueryRow(ctx, `SELECT extversion FROM pg_extension where extname='timescaledb'`).Scan(&ver)
		if err != nil {
			t.Fatal(err)
		}

		db.Close(ctx)
		if ver != "1.7.3" {
			t.Fatal("failed to verify upgrade extension with -upgrade-prerelease-extension false")
		}
		t.Logf("successfully tested extension upgrade flow with --upgrade-prereleases-extensions false.")
	}()

	// start a new connector and test --upgrade-extensions as true which is by default set in flags
	// the migration should fail (upgrade path in tsdb isn't available) but promscale should be running.
	func() {
		connectorImage := "timescale/promscale:latest"
		databaseName := "postgres"
		connector, err := testhelpers.StartConnectorWithImage(ctx, dbContainer, connectorImage, semver.MustParse(version.Promscale), *printLogs, []string{}, databaseName)
		if err != nil {
			t.Fatal(err)
		}
		defer testhelpers.StopContainer(ctx, connector, *printLogs, t)

		var version string
		db, err = pgx.Connect(ctx, testhelpers.PgConnectURL("postgres", testhelpers.Superuser))
		if err != nil {
			t.Fatal(err)
		}
		err = db.QueryRow(ctx, `SELECT extversion FROM pg_extension where extname='timescaledb'`).Scan(&version)
		if err != nil {
			t.Fatal(err)
		}
		db.Close(ctx)

		if version != "1.7.3" {
			t.Fatal("failed to verify timescaleDB extension version")
		}

		// Now from the check we are know that migration failed from 1.7.3 to 1.7.4
		// as the upgrade script doesn't exist within timescaleDB image.
		// Now check promscale is still running on migration failure.
		exitCode, err := connector.Exec(context.Background(), []string{"echo", "hello"})
		if exitCode != 0 || err != nil {
			t.Fatal("promscale failed to run extension migration failure", err)
		}
		t.Logf("successfully tested extension upgrade flow with --upgrade-prereleases-extensions true where migration fails and promscale keeps running.")
	}()
}

func buildPromscaleImageFromRepo(t *testing.T) {
	t.Logf("building promscale image from the codebase")
	cmd := exec.Command("docker", "build", "-t", "timescale/promscale:latest", "./../../../", "--file", "./../../../build/Dockerfile")
	err := cmd.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("successfully built promscale:latest image from codebase.")
}

func startDB(t *testing.T, ctx context.Context) (*pgx.Conn, testcontainers.Container, io.Closer) {
	tmpDir, err := testhelpers.TempDir("update_test_out")
	if err != nil {
		t.Fatal(err)
	}

	dataDir, err := testhelpers.TempDir("update_test_data")
	if err != nil {
		t.Fatal(err)
	}

	extensionState := testhelpers.NewTestOptions(testhelpers.Timescale, constants.PromscaleExtensionContainer)

	dbContainer, closer, err := testhelpers.StartDatabaseImage(ctx, t, "timescaledev/promscale-extension:testing-extension-upgrade", tmpDir, dataDir, *printLogs, extensionState)
	if err != nil {
		t.Fatal("Error setting up container", err)
	}

	// need to get a new pool after the Migrate to catch any GUC changes made during Migrate
	db, err := pgx.Connect(ctx, testhelpers.PgConnectURL("postgres", testhelpers.Superuser))
	if err != nil {
		t.Fatal(err)
	}

	return db, dbContainer, closer
}

func dropAndCreateExt(t *testing.T, ctx context.Context, extVersion string) {
	// Drop existing installed extension & install a lower extension version to test upgrade
	db, err := pgx.Connect(ctx, testhelpers.PgConnectURL("postgres", testhelpers.Superuser))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx, `DROP EXTENSION timescaledb`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close(ctx)

	db, err = pgx.Connect(ctx, testhelpers.PgConnectURL("postgres", testhelpers.Superuser))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(ctx, fmt.Sprintf(`CREATE EXTENSION timescaledb version '%s'`, extVersion))
	if err != nil {
		t.Fatal(err)
	}
	db.Close(ctx)
}
