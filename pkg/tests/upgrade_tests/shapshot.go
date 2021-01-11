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
	"github.com/testcontainers/testcontainers-go"
)

type dbSnapshot struct {
	schemaNames []string
	schemas     []schemaInfo
	extensions  string // \dx
}

var schemas = []string{
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

var schemasWOTimescaleDB = []string{
	"_prom_catalog",
	"_prom_ext",
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
}

var ourSchemas = []string{
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

func PrintDbSnapshotDifferences(t *testing.T, pristineDbInfo dbSnapshot, upgradedDbInfo dbSnapshot) {
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

var replaceChildren = regexp.MustCompile("timescaledb_internal\\._hyper_.*\n")

func SnapshotDB(t *testing.T, container testcontainers.Container, dbName, outputDir string, db *pgxpool.Pool, useTimescaleDB bool) (info dbSnapshot) {
	info.schemaNames = getSchemas(t, db)
	considerSchemas := schemas
	if !useTimescaleDB {
		considerSchemas = schemasWOTimescaleDB
	}
	if !reflect.DeepEqual(info.schemaNames, considerSchemas) {
		t.Errorf(
			"unexpected schemas.\nexpected\n\t%v\ngot\n\t%v",
			considerSchemas,
			info.schemaNames,
		)
	}

	info.extensions = getPsqlInfo(t, container, dbName, outputDir, "\\dx")
	info.schemas = make([]schemaInfo, len(ourSchemas))
	for i, schema := range ourSchemas {
		info := &info.schemas[i]
		info.name = schema
		info.tables = getPsqlInfo(t, container, dbName, outputDir, "\\d+ "+schema+".*")
		info.tables = replaceChildren.ReplaceAllLiteralString(info.tables, "timescaledb_internal._hyper_*\n")
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

func GetDbInfoIgnoringTable(t *testing.T, container testcontainers.Container, dbName, outputDir string, db *pgxpool.Pool, ignoreTableSchema string, ignoreTableName string, useTimescaleDB bool) dbSnapshot {
	snapshot := SnapshotDB(t, container, dbName, outputDir, db, useTimescaleDB)
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
		"SELECT array_agg(nspname::TEXT order by nspname::TEXT) FROM pg_namespace",
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
