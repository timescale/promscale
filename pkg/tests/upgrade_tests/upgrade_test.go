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
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/docker/go-connections/nat"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel"
	. "github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/version"
)

var (
	testDatabase   = flag.String("database", "tmp_db_timescale_upgrade_test", "database to run integration tests on")
	useExtension   = flag.Bool("use-extension", true, "use the promscale extension")
	printLogs      = flag.Bool("print-logs", false, "print TimescaleDB logs")
	extensionState testhelpers.ExtensionState
)

var cleanImage = "timescale/timescaledb:latest-pg12"
var prevDBImage = "timescale/timescaledb:latest-pg12"

func TestMain(m *testing.M) {
	var code int
	// TODO Timescale 2.0 and regular postgres
	if *useExtension {
		cleanImage = "timescaledev/promscale-extension:latest-pg12"
		//TODO: note this uses the old timescale_prometheus_extra docker images
		prevDBImage = "timescaledev/timescale_prometheus_extra:0.1.1-pg12"
	}
	flag.Parse()
	extensionState.UseTimescaleDB()
	if *useExtension {
		extensionState.UsePromscale()
	}
	_ = log.Init(log.Config{
		Level: "debug",
	})
	code = m.Run()
	os.Exit(code)
}

func TestUpgradeFromPrev(t *testing.T) {
	upgradedDbInfo := getUpgradedDbInfo(t, false)
	pristineDbInfo := getPristineDbInfo(t, false)

	if !reflect.DeepEqual(pristineDbInfo, upgradedDbInfo) {
		printDbInfoDifferences(t, pristineDbInfo, upgradedDbInfo)
	}
}

// TestUpgradeFromPrevNoData tests migrations with no ingested data.
// See issue: https://github.com/timescale/promscale/issues/330
func TestUpgradeFromPrevNoData(t *testing.T) {
	upgradedDbInfo := getUpgradedDbInfo(t, true)
	pristineDbInfo := getPristineDbInfo(t, true)

	if !reflect.DeepEqual(pristineDbInfo, upgradedDbInfo) {
		printDbInfoDifferences(t, pristineDbInfo, upgradedDbInfo)
	}
}

func getUpgradedDbInfo(t *testing.T, noData bool) (upgradedDbInfo dbInfo) {
	// we test that upgrading from the previous version gives the correct output
	// by induction, this property should hold true for any chain of versions
	prevVersion := semver.MustParse(version.EarliestUpgradeTestVersion)

	// TODO we could probably improve performance of this test by 2x if we
	//      gathered the db info in parallel. Unfortunately our db runner doesn't
	//      support this yet
	withDBStartingAtOldVersionAndUpgrading(t, *testDatabase, prevVersion,
		/* preUpgrade */
		func(dbContainer testcontainers.Container, dbTmpDir string, connectorHost string, connectorPort nat.Port) {
			if noData {
				return
			}
			client := http.Client{}
			defer client.CloseIdleConnections()

			writeUrl := fmt.Sprintf("http://%s/write", net.JoinHostPort(connectorHost, connectorPort.Port()))

			doWrite(t, &client, writeUrl, preUpgradeData1, preUpgradeData2)
		},
		/* postUpgrade */
		func(dbContainer testcontainers.Container, dbTmpDir string) {
			connectURL := testhelpers.PgConnectURL(*testDatabase, testhelpers.NoSuperuser)

			db, err := pgxpool.Connect(context.Background(), connectURL)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			if !noData {
				ingestor, err := ingestor.NewPgxIngestor(pgxconn.NewPgxConn(db))
				if err != nil {
					t.Fatalf("error connecting to DB: %v", err)
				}

				doIngest(t, ingestor, postUpgradeData1, postUpgradeData2)

				ingestor.Close()

			}
			upgradedDbInfo = getDbInfo(t, dbContainer, dbTmpDir, db)
		})
	return
}

func getPristineDbInfo(t *testing.T, noData bool) (pristineDbInfo dbInfo) {
	withNewDBAtCurrentVersion(t, *testDatabase,
		/* preRestart */
		func(container testcontainers.Container, _ string, db *pgxpool.Pool, tmpDir string) {
			if noData {
				return
			}
			ingestor, err := ingestor.NewPgxIngestor(pgxconn.NewPgxConn(db))
			if err != nil {
				t.Fatalf("error connecting to DB: %v", err)
			}
			defer ingestor.Close()

			doIngest(t, ingestor, preUpgradeData1, preUpgradeData2)
		},
		/* postRestart */
		func(container testcontainers.Container, _ string, db *pgxpool.Pool, tmpDir string) {
			if !noData {
				ingestor, err := ingestor.NewPgxIngestor(pgxconn.NewPgxConn(db))
				if err != nil {
					t.Fatalf("error connecting to DB: %v", err)
				}
				defer ingestor.Close()

				doIngest(t, ingestor, postUpgradeData1, postUpgradeData2)
			}
			pristineDbInfo = getDbInfo(t, container, tmpDir, db)
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

func printDbInfoDifferences(t *testing.T, pristineDbInfo dbInfo, upgradedDbInfo dbInfo) {
	t.Errorf("upgrade differences")
	if !reflect.DeepEqual(upgradedDbInfo.schemaNames, pristineDbInfo.schemaNames) {
		t.Logf("different schemas\nexpected:\n\t%v\ngot:\n\t%v", pristineDbInfo.schemaNames, upgradedDbInfo.schemaNames)
	}
	if !reflect.DeepEqual(upgradedDbInfo.extensions, pristineDbInfo.extensions) {
		t.Logf("different extensions\nexpected:\n\t%v\ngot:\n\t%v", pristineDbInfo.extensions, upgradedDbInfo.extensions)
	}
	pristineSchemas := make(map[string]schemaInfo)
	for _, schema := range pristineDbInfo.schemas {
		pristineSchemas[schema.name] = schema
	}
	for _, schema := range upgradedDbInfo.schemas {
		expected, ok := pristineSchemas[schema.name]
		if !ok {
			t.Logf("extra schema %s", schema.name)
			continue
		}
		tablesDiff := schema.tables != expected.tables
		functionsDiff := schema.functions != expected.functions
		privilegesDiff := schema.privileges != expected.privileges
		indicesDiff := schema.indices != expected.indices
		triggersDiff := schema.triggers != expected.triggers
		dataDiff := !reflect.DeepEqual(schema.data, expected.data)
		if tablesDiff || functionsDiff || privilegesDiff || indicesDiff || triggersDiff || dataDiff {
			t.Logf("differences in schema: %s", schema.name)
		}
		if tablesDiff {
			t.Logf("tables\nexpected:\n\t%s\ngot:\n\t%s", expected.tables, schema.tables)
		}
		if functionsDiff {
			t.Logf("functions\nexpected:\n\t%s\ngot:\n\t%s", expected.functions, schema.functions)
		}
		if privilegesDiff {
			t.Logf("privileges\nexpected:\n\t%s\ngot:\n\t%s", expected.privileges, schema.privileges)
		}
		if indicesDiff {
			t.Logf("indices\nexpected:\n\t%s\ngot:\n\t%s", expected.indices, schema.indices)
		}
		if triggersDiff {
			t.Logf("triggers\nexpected:\n\t%s\ngot:\n\t%s", expected.triggers, schema.triggers)
		}
		if dataDiff {
			t.Logf("data\nexpected:\n\t%+v\ngot:\n\t%+v", expected.data, schema.data)
		}
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

	// Start a db with the prev extension and a prev connector as well
	// Then run preUpgrade and shut everything down.
	func() {
		dbContainer, closer, err := testhelpers.StartDatabaseImage(ctx, prevDBImage, tmpDir, dataDir, *printLogs, extensionState)
		if err != nil {
			t.Fatal("Error setting up container", err)
		}

		defer func() { _ = closer.Close() }()

		db, err := testhelpers.DbSetup(*testDatabase, testhelpers.NoSuperuser)
		if err != nil {
			t.Fatal(err)
			return
		}
		db.Close()

		connectorImage := "timescale/promscale:" + prevVersion.String()
		connector, err := testhelpers.StartConnectorWithImage(context.Background(), connectorImage, *printLogs, []string{}, *testDatabase)
		if err != nil {
			log.Fatal(err.Error())
		}
		defer testhelpers.StopContainer(ctx, connector, *printLogs)

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
	dbContainer, closer, err := testhelpers.StartDatabaseImage(ctx, cleanImage, tmpDir, dataDir, *printLogs, extensionState)
	if err != nil {
		t.Fatal("Error setting up container", err)
	}

	defer func() { _ = closer.Close() }()

	t.Logf("upgrading versions %v => %v", prevVersion, version.Version)
	connectURL := testhelpers.PgConnectURL(*testDatabase, testhelpers.NoSuperuser)
	migrateToVersion(t, connectURL, version.Version, "azxtestcommit")

	t.Log("Running postUpgrade")
	postUpgrade(dbContainer, tmpDir)

}

// Run a DB and connector at the current version. Run preRestart then restart the db
// then run postRestart. A restart is necessary because we need a restart in the
// upgrade path to change the extension that is available. But, a restart causes
// Sequences to skip values. So, in order to have equivalent data, we need to make
// sure that both the upgrade and this pristine path both have restarts.
func withNewDBAtCurrentVersion(t testing.TB, DBName string,
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

	func() {
		container, closer, err := testhelpers.StartDatabaseImage(ctx, cleanImage, tmpDir, dataDir, *printLogs, extensionState)
		if err != nil {
			fmt.Println("Error setting up container", err)
			os.Exit(1)
		}

		defer func() { _ = closer.Close() }()
		testhelpers.WithDB(t, DBName, testhelpers.NoSuperuser, func(_ *pgxpool.Pool, t testing.TB, connectURL string) {
			migrateToVersion(t, connectURL, version.Version, "azxtestcommit")

			// need to get a new pool after the Migrate to catch any GUC changes made during Migrate
			db, err := pgxpool.Connect(context.Background(), connectURL)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			preRestart(container, connectURL, db, tmpDir)
		})
	}()
	container, closer, err := testhelpers.StartDatabaseImage(ctx, cleanImage, tmpDir, dataDir, *printLogs, extensionState)
	if err != nil {
		fmt.Println("Error setting up container", err)
		os.Exit(1)
	}

	defer func() { _ = closer.Close() }()
	connectURL := testhelpers.PgConnectURL(*testDatabase, testhelpers.NoSuperuser)
	db, err := pgxpool.Connect(context.Background(), connectURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	postRestart(container, connectURL, db, tmpDir)
}

func migrateToVersion(t testing.TB, connectURL string, version string, commitHash string) {
	err := pgmodel.InstallUpgradeTimescaleDBExtensions(connectURL, true, false)
	if err != nil {
		t.Fatal(err)
	}

	migratePool, err := pgx.Connect(context.Background(), connectURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = migratePool.Close(context.Background()) }()
	err = Migrate(migratePool, pgmodel.VersionInfo{Version: version, CommitHash: commitHash}, true, false)
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

		_, _ = io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
}

func doIngest(t *testing.T, ingestor *ingestor.DBIngestor, data ...[]prompb.TimeSeries) {
	for _, data := range data {
		_, err := ingestor.Ingest(copyMetrics(data), &prompb.WriteRequest{})
		if err != nil {
			t.Fatalf("ingest error: %v", err)
		}
		_ = ingestor.CompleteMetricCreation()
	}
}

var schemas []string = []string{
	"_prom_catalog",
	"_prom_ext",
	"_timescaledb_cache",
	"_timescaledb_catalog",
	"_timescaledb_config",
	"_timescaledb_internal",
	"information_schema",
	"pg_catalog",
	"pg_temp_1",
	"pg_toast",
	"pg_toast_temp_1",
	"prom_api",
	"prom_data",
	"prom_data_series",
	"prom_info",
	"prom_metric",
	"prom_series",
	"public",
	"timescaledb_information",
}

var ourSchemas []string = []string{
	"public",
	"_prom_catalog",
	"_prom_ext",
	"prom_api",
	"prom_data",
	"prom_data_series",
	"prom_info",
	"prom_metric",
	"prom_series",
}

type dbInfo struct {
	schemaNames []string
	schemas     []schemaInfo
	extensions  string // \dx
}

type schemaInfo struct {
	name       string
	tables     string // \d+
	functions  string // \df+
	privileges string // \dp+
	indices    string // \di
	triggers   string // \dy
	data       []tableInfo
}

type tableInfo struct {
	name   string
	values []string
}

var replaceChildren = regexp.MustCompile("timescaledb_internal\\._hyper_.*\n")

func getDbInfo(t *testing.T, container testcontainers.Container, outputDir string, db *pgxpool.Pool) (info dbInfo) {

	info.schemaNames = getSchemas(t, db)
	if !reflect.DeepEqual(info.schemaNames, schemas) {
		t.Errorf(
			"unexpected schemas.\nexpected\n\t%v\ngot\n\t%v",
			info.schemas,
			schemas,
		)
	}

	info.extensions = getPsqlInfo(t, container, outputDir, "\\dx")
	info.schemas = make([]schemaInfo, len(ourSchemas))
	for i, schema := range ourSchemas {
		info := &info.schemas[i]
		info.name = schema
		info.tables = getPsqlInfo(t, container, outputDir, "\\d+ "+schema+".*")
		info.tables = replaceChildren.ReplaceAllLiteralString(info.tables, "timescaledb_internal._hyper_*\n")
		info.functions = getPsqlInfo(t, container, outputDir, "\\df+ "+schema+".*")
		info.privileges = getPsqlInfo(t, container, outputDir, "\\dp "+schema+".*")
		// not using \di+ since the sizes are too noisy, and the descriptions
		// will be in tables anyway
		info.indices = getPsqlInfo(t, container, outputDir, "\\di "+schema+".*")
		info.triggers = getPsqlInfo(t, container, outputDir, "\\dy "+schema+".*")
		info.data = getTableInfosForSchema(t, db, schema)
	}
	return
}

func getPsqlInfo(t *testing.T, container testcontainers.Container, outputDir string, query string) string {
	i, err := container.Exec(
		context.Background(),
		[]string{"bash", "-c", "psql -U postgres -d " + *testDatabase + " -c '" + query + "' &> /testdata/output.out"},
	)
	if err != nil {
		t.Fatal(err)
	}

	output := readOutput(t, outputDir)

	if i != 0 {
		t.Logf("psql error. output: %s", output)
	}
	return output
}

func readOutput(t *testing.T, outputDir string) string {
	outputFile := outputDir + "/output.out"
	output, err := ioutil.ReadFile(outputFile)
	if err != nil {
		t.Errorf("error reading psql output: %v", err)
	}
	return string(output)
}

func getSchemas(t *testing.T, db *pgxpool.Pool) (out []string) {
	row := db.QueryRow(
		context.Background(),
		"SELECT array_agg(nspname::TEXT order by nspname::TEXT) FROM pg_namespace",
	)
	err := row.Scan(&out)
	if err != nil {
		t.Errorf("could not discover schemas due to: %v", err)
	}
	return
}

func getTableInfosForSchema(t *testing.T, db *pgxpool.Pool, schema string) (out []tableInfo) {
	row := db.QueryRow(
		context.Background(),
		"SELECT array_agg(relname::TEXT order by relname::TEXT) "+
			"FROM pg_class "+
			"WHERE relnamespace=$1::TEXT::regnamespace AND relkind='r'",
		schema,
	)
	var tables []string
	err := row.Scan(&tables)
	if err != nil {
		t.Errorf("could not get table info for schema \"%s\" due to: %v", schema, err)
		return
	}

	out = make([]tableInfo, len(tables))
	batch := pgx.Batch{}
	for _, table := range tables {
		batch.Queue(fmt.Sprintf(
			"SELECT array_agg((tbl.*)::TEXT order by (tbl.*)::TEXT) from %s tbl",
			pgx.Identifier{schema, table}.Sanitize(),
		))
	}
	results := db.SendBatch(context.Background(), &batch)
	defer results.Close()
	for i, table := range tables {
		out[i].name = table
		err := results.QueryRow().Scan(&out[i].values)
		if err != nil {
			t.Errorf("error querying values from table %s: %v",
				pgx.Identifier{schema, table}.Sanitize(), err)
		}
	}
	return
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

func TestUpgradeExtensions(t *testing.T) {
	buildPromscaleImageFromRepo(t)
	testPrereleaseExtensionUpgrade(t)
	testExtMigrationFailure(t)
	os.Exit(0)
}

func testPrereleaseExtensionUpgrade(t *testing.T) {
	var err error
	var version string
	ctx := context.Background()

	db, dbContainer, closer := startDB(t, ctx)
	defer closer.Close()

	defer testhelpers.StopContainer(ctx, dbContainer, false)

	// as the default installed version ext is rc4 in the test image downgrade it to rc2
	// to test upgrade flow.
	extVersion := "2.0.0-rc2"
	dropAndCreateExt(t, ctx, extVersion)

	db, err = pgx.Connect(ctx, testhelpers.PgConnectURL("postgres", testhelpers.Superuser))
	if err != nil {
		t.Fatal(err)
	}

	err = db.QueryRow(ctx, `SELECT extversion FROM pg_extension where extname='timescaledb'`).Scan(&version)
	if err != nil {
		t.Fatal(err)
	}

	if version != extVersion {
		t.Fatal("failed to verify upgrade extension with -upgrade-prerelease-extension false")
	}

	// start promscale & test upgrade-prerelease-extensions as false
	// Now the ext is rc2 it should be rc2 after promscale startup too
	func() {
		connectorImage := "timescale/promscale:latest"
		databaseName := "postgres"
		connector, err := testhelpers.StartConnectorWithImage(ctx, connectorImage, *printLogs, []string{}, databaseName)
		if err != nil {
			t.Fatal(err)
		}
		defer testhelpers.StopContainer(ctx, connector, *printLogs)
		err = db.QueryRow(ctx, `SELECT extversion FROM pg_extension where extname='timescaledb'`).Scan(&version)
		if err != nil {
			t.Fatal(err)
		}

		if version != extVersion {
			t.Fatal("failed to verify upgrade extension with -upgrade-prerelease-extension false")
		}
		t.Logf("successfully tested extension upgrade flow with --upgrade-prereleases-extensions false")
	}()

	db.Close(ctx)

	// start a new connector and test --upgrade-prerelease-extensions as true
	// the default installed ext version is rc2 now it should upgrade it to rc4
	func() {
		connectorImage := "timescale/promscale:latest"
		databaseName := "postgres"
		flags := []string{"-upgrade-prerelease-extensions", "true"}
		connector, err := testhelpers.StartConnectorWithImage(ctx, connectorImage, *printLogs, flags, databaseName)
		if err != nil {
			t.Fatal(err)
		}
		defer testhelpers.StopContainer(ctx, connector, *printLogs)

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
		if version != "2.0.0-rc4" {
			t.Fatal("failed to verify upgrade extension with -upgrade-prerelease-extension true")
		}
		t.Logf("successfully tested extension upgrade flow with --upgrade-prereleases-extensions true")
	}()
}

func testExtMigrationFailure(t *testing.T) {
	var err error
	var version string
	ctx := context.Background()

	db, dbContainer, closer := startDB(t, ctx)
	defer testhelpers.StopContainer(ctx, dbContainer, false)

	defer closer.Close()

	// start promscale & test upgrade-extensions as false
	func() {
		connectorImage := "timescale/promscale:latest"
		databaseName := "postgres"
		connector, err := testhelpers.StartConnectorWithImage(ctx, connectorImage, *printLogs, []string{}, databaseName)
		if err != nil {
			t.Fatal(err)
		}
		defer testhelpers.StopContainer(ctx, connector, *printLogs)

		err = db.QueryRow(ctx, `SELECT extversion FROM pg_extension where extname='timescaledb'`).Scan(&version)
		if err != nil {
			t.Fatal(err)
		}

		db.Close(ctx)
		if version != "2.0.0-rc4" {
			t.Fatal("failed to verify upgrade extension with -upgrade-prerelease-extension false")
		}
		t.Logf("successfully tested extension upgrade flow with --upgrade-prereleases-extensions false.")
	}()

	// As the timescaleDB installed version is rc4, lets install the 1.7.3 ext version
	extVersion := "1.7.3"
	dropAndCreateExt(t, ctx, extVersion)

	db, err = pgx.Connect(ctx, testhelpers.PgConnectURL("postgres", testhelpers.Superuser))
	if err != nil {
		t.Fatal(err)
	}

	err = db.QueryRow(ctx, `SELECT extversion FROM pg_extension where extname='timescaledb'`).Scan(&version)
	if err != nil {
		t.Fatal(err)
	}

	db.Close(ctx)
	if version != extVersion {
		t.Fatal("failed to verify upgrade extension with -upgrade-prerelease-extension false")
	}

	// start a new connector and test --upgrade-extensions as true which is by default set in flags
	// the migration should fail (upgrade path in tsdb isn't available) but promscale should be running.
	func() {
		connectorImage := "timescale/promscale:latest"
		databaseName := "postgres"
		connector, err := testhelpers.StartConnectorWithImage(ctx, connectorImage, *printLogs, []string{}, databaseName)
		if err != nil {
			t.Fatal(err)
		}
		defer testhelpers.StopContainer(ctx, connector, *printLogs)

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
	t.Logf("building promscle image from the codebase")
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

	dbContainer, closer, err := testhelpers.StartDatabaseImage(ctx, "timescaledev/promscale-extension:testing-extension-upgrade", tmpDir, dataDir, *printLogs, extensionState)
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
