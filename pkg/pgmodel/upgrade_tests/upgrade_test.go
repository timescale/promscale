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
	"reflect"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/docker/go-connections/nat"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/timescale/timescale-prometheus/pkg/internal/testhelpers"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/prompb"
	"github.com/timescale/timescale-prometheus/pkg/version"

	_ "github.com/jackc/pgx/v4/stdlib"

	. "github.com/timescale/timescale-prometheus/pkg/pgmodel"
)

var (
	testDatabase = flag.String("database", "tmp_db_timescale_upgrade_test", "database to run integration tests on")
	useExtension = flag.Bool("use-extension", true, "use the timescale_prometheus_extra extension")
	printLogs    = flag.Bool("print-logs", false, "print TimescaleDB logs")
)

var cleanImage = "timescale/timescaledb:latest-pg12"

func TestMain(m *testing.M) {
	var code int
	if *useExtension {
		cleanImage = "timescaledev/timescale_prometheus_extra:latest-pg12"
	}
	flag.Parse()
	_ = log.Init("debug")
	code = m.Run()
	os.Exit(code)
}

func TestUpgradeFromPrev(t *testing.T) {
	upgradedDbInfo := getUpgradedDbInfo(t)
	pristineDbInfo := getPristineDbInfo(t)

	if !reflect.DeepEqual(pristineDbInfo, upgradedDbInfo) {
		printDbInfoDifferences(t, pristineDbInfo, upgradedDbInfo)
	}
}

func getUpgradedDbInfo(t *testing.T) (upgradedDbInfo dbInfo) {
	// we test that upgrading from the previous version gives the correct output
	// by induction, this property should hold true for any chain of versions
	prevVersion := semver.MustParse(version.Version)
	toPreviousVersion(&prevVersion)

	// TODO we could probably improve performance of this test by 2x if we
	//      gathered the db info in parallel. Unfortunately our db runner doesn't
	//      support this yet
	prevImage := "timescale/timescale-prometheus:" + prevVersion.String()
	withDBAndConnector(t, *testDatabase, prevImage,
		func(dbContainer testcontainers.Container, dbTmpDir string, connectorHost string, connectorPort nat.Port) {
			client := http.Client{}
			defer client.CloseIdleConnections()

			writeUrl := fmt.Sprintf("http://%s/write", net.JoinHostPort(connectorHost, connectorPort.Port()))
			t.Log(writeUrl)

			doWrite(t, &client, writeUrl, preUpgradeData1, preUpgradeData2)

			t.Logf("upgrading versions %v => %v", prevVersion, version.Version)
			// TODO this should be NoSuperuser expect for the extwlist error
			connectURL := testhelpers.PgConnectURL(*testDatabase, testhelpers.Superuser)
			migrateToVersion(t, connectURL, version.Version, "azxtestcommit")

			db, err := pgxpool.Connect(context.Background(), connectURL)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ingestor, err := pgmodel.NewPgxIngestor(db)
			if err != nil {
				t.Fatalf("error connecting to DB: %v", err)
			}

			doIngest(t, ingestor, postUpgradeData1, postUpgradeData2)

			ingestor.Close()
			upgradedDbInfo = getDbInfo(t, dbContainer, dbTmpDir, db)
		})
	return
}

func getPristineDbInfo(t *testing.T) (pristineDbInfo dbInfo) {
	withNewDBAtCurrentVersion(t, *testDatabase,
		func(container testcontainers.Container, _ string, db *pgxpool.Pool, tmpDir string) {
			defer db.Close()
			ingestor, err := pgmodel.NewPgxIngestor(db)
			if err != nil {
				t.Fatalf("error connecting to DB: %v", err)
			}

			doIngest(t, ingestor, preUpgradeData1, preUpgradeData2, postUpgradeData1, postUpgradeData2)

			ingestor.Close()
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
				{Name: MetricNameLabelName, Value: "test"},
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
				{Name: MetricNameLabelName, Value: "test2"},
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
				{Name: MetricNameLabelName, Value: "test"},
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
				{Name: MetricNameLabelName, Value: "test3"},
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

func withDBAndConnector(t testing.TB, DBName string, connectorImage string,
	f func(dbContainer testcontainers.Container, dbTmpDir string, connectorHost string, connectorPort nat.Port)) {
	var err error
	ctx := context.Background()

	tmpDir, err := testhelpers.TempDir("update_test_out")
	if err != nil {
		log.Fatal(err)
	}

	dbContainer, err := testhelpers.StartPGContainerWithImage(ctx, cleanImage, tmpDir, *printLogs)
	if err != nil {
		t.Fatal("Error setting up container", err)
	}

	defer testhelpers.StopContainer(ctx, dbContainer, *printLogs)

	db, err := testhelpers.DbSetup(*testDatabase, testhelpers.Superuser)
	if err != nil {
		t.Fatal(err)
		return
	}
	db.Close()

	connector, err := testhelpers.StartConnectorWithImage(context.Background(), connectorImage, *printLogs, *testDatabase)
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
	f(dbContainer, tmpDir, connectorHost, connectorPort)
}

func withNewDBAtCurrentVersion(t testing.TB, DBName string,
	f func(container testcontainers.Container, connectURL string, db *pgxpool.Pool, tmpDir string)) {
	var err error
	ctx := context.Background()

	tmpDir, err := testhelpers.TempDir("update_test_out")
	if err != nil {
		log.Fatal(err)
	}

	container, err := testhelpers.StartPGContainerWithImage(ctx, cleanImage, tmpDir, *printLogs)
	if err != nil {
		fmt.Println("Error setting up container", err)
		os.Exit(1)
	}

	defer func() {
		if err != nil {
			panic(err)
		}

		if *printLogs {
			_ = container.StopLogProducer()
		}

		_ = container.Terminate(ctx)
	}()
	// TODO this should be NoSuperuser expect for the extwlist error
	testhelpers.WithDB(t, DBName, testhelpers.Superuser, func(_ *pgxpool.Pool, t testing.TB, connectURL string) {
		migrateToVersion(t, connectURL, version.Version, "azxtestcommit")

		// need to get a new pool after the Migrate to catch any GUC changes made during Migrate
		db, err := pgxpool.Connect(context.Background(), connectURL)
		if err != nil {
			t.Fatal(err)
		}
		f(container, connectURL, db, tmpDir)
	})
}

func migrateToVersion(t testing.TB, connectURL string, version string, commitHash string) {
	migratePool, err := pgxpool.Connect(context.Background(), connectURL)
	if err != nil {
		t.Fatal(err)
	}
	defer migratePool.Close()
	err = Migrate(migratePool, pgmodel.VersionInfo{Version: version, CommitHash: commitHash})
	if err != nil {
		t.Fatal(err)
	}
}

func toPreviousVersion(version *semver.Version) {
	// our versions must match the schema X.Y.Z[.<pre-release>.A][.dev.C]
	// where capital letters are numbers, and <pre-release> is some arbitrary
	// pre-release tag.
	// We skip .dev versions as they won't have a docker image, and go to the
	// last "released" one
	if len(version.Pre) >= 2 && version.Pre[len(version.Pre)-2].VersionStr == "dev" {
		version.Pre = version.Pre[:len(version.Pre)-2]
	}

	if len(version.Pre) > 0 {
		if len(version.Pre) != 2 ||
			!version.Pre[len(version.Pre)-1].IsNum ||
			version.Pre[len(version.Pre)-2].IsNum {
			panic(fmt.Sprintf("version \"%v\" does not match our version spec", version))
		}
		lastPreDigit := &version.Pre[len(version.Pre)-1]
		if lastPreDigit.VersionNum > 0 {
			lastPreDigit.VersionNum -= 1
			return
		}
	}

	version.Pre = nil

	if version.Patch > 0 {
		version.Patch -= 1
		return
	}

	if version.Minor > 0 {
		version.Minor -= 1
		return
	}

	version.Major -= 1
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
		resp, err := client.Post(url, "application/x-www-form-urlencoded; param=value", body)
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

func doIngest(t *testing.T, ingestor *DBIngestor, data ...[]prompb.TimeSeries) {
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

func getDbInfo(t *testing.T, container testcontainers.Container, outputDir string, db *pgxpool.Pool) (info dbInfo) {

	info.schemaNames = getSchemas(t, db)
	if !reflect.DeepEqual(info.schemaNames, schemas) {
		t.Errorf(
			"unexpected schemas.\nexpected\n\t%v\ngot\n\t%v",
			info.schemas,
			schemas,
		)
	}

	info.schemas = make([]schemaInfo, len(ourSchemas))
	for i, schema := range ourSchemas {
		info := &info.schemas[i]
		info.name = schema
		info.tables = getPsqlInfo(t, container, outputDir, "\\d+ "+schema+".*")
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
