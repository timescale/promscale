// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package upgrade_tests

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/testcontainers/testcontainers-go"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
)

type dbSnapshot struct {
	schemaNames       []string
	schemas           []schemaInfo
	extensions        string // \dx
	schemaOutputs     string // \dn+
	defaultPrivileges string // \ddp
}

var schemas = []string{
	"_prom_catalog",
	"_prom_ext",
	"_ps_catalog",
	"_ps_trace",
	"_timescaledb_cache",
	"_timescaledb_catalog",
	"_timescaledb_config",
	"_timescaledb_internal",
	"information_schema",
	"pg_catalog",
	"pg_toast",
	"prom_api",
	"prom_data",
	"prom_data_exemplar",
	"prom_data_series",
	"prom_info",
	"prom_metric",
	"prom_series",
	"ps_tag",
	"ps_trace",
	"public",
	"timescaledb_information",
}

var schemasWOTimescaleDB = []string{
	"_prom_catalog",
	"_prom_ext",
	"_ps_catalog",
	"_ps_trace",
	"information_schema",
	"pg_catalog",
	"pg_toast",
	"prom_api",
	"prom_data",
	"prom_data_exemplar",
	"prom_data_series",
	"prom_info",
	"prom_metric",
	"prom_series",
	"ps_tag",
	"ps_trace",
	"public",
}

var ourSchemas = []string{
	"public",
	"_prom_catalog",
	"_prom_ext",
	"_ps_catalog",
	"_ps_trace",
	"prom_api",
	"prom_data",
	"prom_data_exemplar",
	"prom_data_series",
	"prom_info",
	"prom_metric",
	"prom_series",
	"ps_tag",
	"ps_trace",
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

func printOutputDiff(t *testing.T, upgradedOuput string, pristineOutput string, msg string) bool {
	if upgradedOuput == pristineOutput {
		return false
	}
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(upgradedOuput, pristineOutput, false)
	t.Logf("%s:\n%v", msg, dmp.DiffPrettyText(diffs))
	return true
}

func PrintDbSnapshotDifferences(t *testing.T, pristineDbInfo dbSnapshot, upgradedDbInfo dbSnapshot) {
	t.Errorf("upgrade differences")
	printOutputDiff(t, fmt.Sprintf("%+v", upgradedDbInfo.schemaNames), fmt.Sprintf("%+v", pristineDbInfo.schemaNames), "schema names differ")
	printOutputDiff(t, upgradedDbInfo.extensions, pristineDbInfo.extensions, "extension outputs differ")
	printOutputDiff(t, upgradedDbInfo.schemaOutputs, pristineDbInfo.schemaOutputs, "schema outputs differ")
	printOutputDiff(t, upgradedDbInfo.defaultPrivileges, pristineDbInfo.defaultPrivileges, "default privileges differ")

	pristineSchemas := make(map[string]schemaInfo)
	for _, schema := range pristineDbInfo.schemas {
		pristineSchemas[schema.name] = schema
	}
	for _, upgradedSchema := range upgradedDbInfo.schemas {
		pristineSchema, ok := pristineSchemas[upgradedSchema.name]
		if !ok {
			t.Logf("extra schema %s", upgradedSchema.name)
			continue
		}
		printOutputDiff(t, upgradedSchema.tables, pristineSchema.tables, "tables differ for schema "+upgradedSchema.name)
		printOutputDiff(t, upgradedSchema.functions, pristineSchema.functions, "functions differ for schema "+upgradedSchema.name)
		printOutputDiff(t, upgradedSchema.privileges, pristineSchema.privileges, "privileges differ for schema "+upgradedSchema.name)
		printOutputDiff(t, upgradedSchema.indices, pristineSchema.indices, "indices differ for schema "+upgradedSchema.name)
		printOutputDiff(t, upgradedSchema.triggers, pristineSchema.triggers, "triggers differ for schema "+upgradedSchema.name)
		printOutputDiff(t, fmt.Sprintf("%+v", upgradedSchema.data), fmt.Sprintf("%+v", pristineSchema.data), "data differs for schema "+upgradedSchema.name)
	}
}

var replaceChildren = regexp.MustCompile("timescaledb_internal\\._hyper_.*\n")
var replaceDistChildren = regexp.MustCompile("timescaledb_internal\\._dist_hyper_.*\n")
var replaceSatisfiesOID = regexp.MustCompile("satisfies_hash_partition.{3,20}::oid")

func expectedSchemas(extstate testhelpers.ExtensionState) []string {
	considerSchemas := schemas
	if !extstate.UsesTimescaleDB() {
		considerSchemas = schemasWOTimescaleDB
	}
	return considerSchemas
}

func SnapshotDB(t *testing.T, container testcontainers.Container, dbName, outputDir string, db *pgxpool.Pool, extstate testhelpers.ExtensionState) (info dbSnapshot) {
	info.schemaNames = getSchemas(t, db)
	considerSchemas := expectedSchemas(extstate)

	if !reflect.DeepEqual(info.schemaNames, considerSchemas) {
		t.Errorf(
			"unexpected schemas.\nexpected\n\t%v\ngot\n\t%v",
			considerSchemas,
			info.schemaNames,
		)
	}

	info.extensions = getPsqlInfo(t, container, dbName, outputDir, "\\dx")
	info.schemaOutputs = getPsqlInfo(t, container, dbName, outputDir, "\\dn+")
	info.defaultPrivileges = getPsqlInfo(t, container, dbName, outputDir, "\\ddp")
	info.schemas = make([]schemaInfo, len(ourSchemas))
	for i, schema := range ourSchemas {
		info := &info.schemas[i]
		info.name = schema
		info.tables = getPsqlInfo(t, container, dbName, outputDir, "\\d+ "+schema+".*")
		info.tables = replaceChildren.ReplaceAllLiteralString(info.tables, "timescaledb_internal._hyper_*\n")
		info.tables = replaceDistChildren.ReplaceAllLiteralString(info.tables, "timescaledb_internal._dist_hyper_*\n")
		info.tables = replaceSatisfiesOID.ReplaceAllLiteralString(info.tables, "satisfies_hash_partition('xxx'::oid")
		info.functions = getPsqlInfo(t, container, dbName, outputDir, "\\df+ "+schema+".*")
		info.privileges = getPsqlInfo(t, container, dbName, outputDir, "\\dp "+schema+".*")
		// not using \di+ since the sizes are too noisy, and the descriptions
		// will be in tables anyway
		info.indices = getPsqlInfo(t, container, dbName, outputDir, "\\di "+schema+".*")
		info.triggers = getPsqlInfo(t, container, dbName, outputDir, "\\dy "+schema+".*")
		info.data = getTableInfosForSchema(t, db, schema)
	}
	return
}

func GetDbInfoIgnoringTable(t *testing.T, container testcontainers.Container, dbName, outputDir string, db *pgxpool.Pool, ignoreTableSchema string, ignoreTableName string, extState testhelpers.ExtensionState) dbSnapshot {
	snapshot := SnapshotDB(t, container, dbName, outputDir, db, extState)
	return ClearTableFromSnapshot(snapshot, ignoreTableSchema, ignoreTableName)
}

func ClearTableFromSnapshot(snap dbSnapshot, schemaName, tableName string) dbSnapshot {
	for s := range snap.schemas {
		if schemaName != "" && snap.schemas[s].name == schemaName {
			snap.schemas[s] = schemaInfo{}
			continue
		}
		for t := range snap.schemas[s].data {
			if snap.schemas[s].data[t].name == tableName {
				snap.schemas[s].data[t].values = []string{}
			}
		}
	}
	return snap
}

func getSchemas(t *testing.T, db *pgxpool.Pool) (out []string) {
	row := db.QueryRow(
		context.Background(),
		`SELECT array_agg(nspname::TEXT order by nspname::TEXT) FROM pg_namespace 
				WHERE nspname::TEXT != 'timescaledb_experimental'
				AND nspname::TEXT NOT LIKE 'pg_temp%'
				AND nspname::TEXT NOT LIKE 'pg_toast_temp%'`,
	)
	err := row.Scan(&out)
	if err != nil {
		t.Errorf("could not discover schemas due to: %v", err)
	}
	return
}

func getPsqlInfo(t *testing.T, container testcontainers.Container, dbName string, outputDir string, query string) string {
	i, err := container.Exec(
		context.Background(),
		[]string{"bash", "-c", "psql -U postgres -d " + dbName + " -c '" + query + "' &> /testdata/output.out"},
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
	output, err := ioutil.ReadFile(filepath.Clean(outputFile))
	if err != nil {
		t.Errorf("error reading psql output: %v", err)
	}
	return string(output)
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
