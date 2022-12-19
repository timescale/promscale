// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package upgrade_tests

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/testcontainers/testcontainers-go"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
)

type dbSnapshot struct {
	schemaNames       []string
	schemas           []schemaInfo
	users             string // \du
	extensions        string // \dx
	promscale         string // \dx+ promscale
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
	operators  string // \do
	data       []tableInfo
}

type tableInfo struct {
	name   string
	values []string
}

func (s dbSnapshot) writeToFile(f *os.File) error {
	w := bufio.NewWriter(f)
	if _, err := w.WriteString(s.users); err != nil {
		return err
	}
	if _, err := w.WriteString(s.extensions); err != nil {
		return err
	}
	if _, err := w.WriteString(s.promscale); err != nil {
		return err
	}
	if _, err := w.WriteString(s.schemaOutputs); err != nil {
		return err
	}
	if _, err := w.WriteString(s.defaultPrivileges); err != nil {
		return err
	}

	for _, si := range s.schemas {
		if _, err := w.WriteString(si.name); err != nil {
			return err
		}
		if _, err := w.WriteString(si.tables); err != nil {
			return err
		}
		if _, err := w.WriteString(si.functions); err != nil {
			return err
		}
		if _, err := w.WriteString(si.privileges); err != nil {
			return err
		}
		if _, err := w.WriteString(si.indices); err != nil {
			return err
		}
		if _, err := w.WriteString(si.triggers); err != nil {
			return err
		}
		if _, err := w.WriteString(si.operators); err != nil {
			return err
		}
		for _, ti := range si.data {
			if _, err := w.WriteString(fmt.Sprintf("%s.%s\n", si.name, ti.name)); err != nil {
				return err
			}
			for _, v := range ti.values {
				if _, err := w.WriteString(fmt.Sprintf("%s\n", v)); err != nil {
					return err
				}
			}
		}
	}
	return w.Flush()
}

func printOutputDiff(t *testing.T, before string, after string, msg string) bool {
	if before == after {
		return false
	}
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(before, after, false)
	t.Logf("%s:\n%v", msg, dmp.DiffPrettyText(diffs))
	return true
}

func PrintDbSnapshotDifferences(t *testing.T, before dbSnapshot, after dbSnapshot) {
	t.Errorf("upgrade differences")
	printOutputDiff(t, fmt.Sprintf("%+v", before.schemaNames), fmt.Sprintf("%+v", after.schemaNames), "schema names differ")
	printOutputDiff(t, before.users, after.users, "users differ")
	printOutputDiff(t, before.extensions, after.extensions, "extension outputs differ")
	printOutputDiff(t, before.promscale, after.promscale, "promscale extension members differ")
	printOutputDiff(t, before.schemaOutputs, after.schemaOutputs, "schema outputs differ")
	printOutputDiff(t, before.defaultPrivileges, after.defaultPrivileges, "default privileges differ")

	allSchemas := make(map[string]struct{})
	beforeSchemas := make(map[string]schemaInfo)
	for _, beforeSchema := range before.schemas {
		beforeSchemas[beforeSchema.name] = beforeSchema
		allSchemas[beforeSchema.name] = struct{}{}
	}
	afterSchemas := make(map[string]schemaInfo)
	for _, afterSchema := range after.schemas {
		afterSchemas[afterSchema.name] = afterSchema
		allSchemas[afterSchema.name] = struct{}{}
	}

	for schemaName := range allSchemas {
		beforeSchema, ok := beforeSchemas[schemaName]
		if !ok {
			printOutputDiff(t, "", schemaName, "schemas differ")
			continue
		}
		afterSchema, ok := afterSchemas[schemaName]
		if !ok {
			printOutputDiff(t, schemaName, "", "schemas differ")
			continue
		}
		printOutputDiff(t, beforeSchema.tables, afterSchema.tables, "tables differ for schema "+schemaName)
		printOutputDiff(t, beforeSchema.functions, afterSchema.functions, "functions differ for schema "+schemaName)
		printOutputDiff(t, beforeSchema.privileges, afterSchema.privileges, "privileges differ for schema "+schemaName)
		printOutputDiff(t, beforeSchema.indices, afterSchema.indices, "indices differ for schema "+schemaName)
		printOutputDiff(t, beforeSchema.triggers, afterSchema.triggers, "triggers differ for schema "+schemaName)
		printOutputDiff(t, beforeSchema.operators, afterSchema.operators, "operators differ for schema "+schemaName)
		printOutputDiff(t, fmt.Sprintf("%+v", beforeSchema.data), fmt.Sprintf("%+v", afterSchema.data), "data differs for schema "+schemaName)
	}
}

var replaceChildren = regexp.MustCompile("timescaledb_internal\\._hyper_.*\n")
var replaceDistChildren = regexp.MustCompile("timescaledb_internal\\._dist_hyper_.*\n")
var replaceSatisfiesOID = regexp.MustCompile("satisfies_hash_partition.{3,20}::oid")
var replaceComprChildren = regexp.MustCompile("_timescaledb_internal\\._compressed_hypertable_.*\n")

func expectedSchemas(extstate testhelpers.TestOptions) []string {
	considerSchemas := schemas
	if !extstate.UsesTimescaleDB() {
		considerSchemas = schemasWOTimescaleDB
	}
	return considerSchemas
}

func clearSearchPath(t *testing.T, db *pgxpool.Pool) {
	var username string
	err := db.QueryRow(context.Background(), "select current_user").Scan(&username)
	if err != nil {
		t.Errorf("failed to select current_user")
		return
	}
	_, err = db.Exec(context.Background(), fmt.Sprintf("alter user %s set search_path to pg_catalog", pgx.Identifier{username}.Sanitize()))
	if err != nil {
		t.Errorf("failed to set search_path: %v", err)
	}
}

func SnapshotDB(t *testing.T, container testcontainers.Container, dbName, outputDir string, db *pgxpool.Pool, extstate testhelpers.TestOptions) (info dbSnapshot) {
	clearSearchPath(t, db)
	info.schemaNames = getSchemas(t, db)
	considerSchemas := expectedSchemas(extstate)

	if !reflect.DeepEqual(info.schemaNames, considerSchemas) {
		t.Errorf(
			"unexpected schemas.\nexpected\n\t%v\ngot\n\t%v",
			considerSchemas,
			info.schemaNames,
		)
	}

	info.users = getPsqlInfo(t, container, dbName, outputDir, "\\du")
	info.extensions = getPsqlInfo(t, container, dbName, outputDir, "\\dx")
	info.promscale = getPsqlInfo(t, container, dbName, outputDir, "\\dx+ promscale")
	info.promscale = replaceComprChildren.ReplaceAllLiteralString(info.promscale, "_timescaledb_internal._compressed_hypertable_*\n")
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
		info.operators = getPsqlInfo(t, container, dbName, outputDir, "\\do "+schema+".*")
		info.data = getTableInfosForSchema(t, db, schema)
	}
	return
}

func GetDbInfoIgnoringTable(t *testing.T, container testcontainers.Container, dbName, outputDir string, db *pgxpool.Pool, ignoreTableSchema string, ignoreTableName string, extState testhelpers.TestOptions) dbSnapshot {
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
	output, err := os.ReadFile(filepath.Clean(outputFile))
	if err != nil {
		t.Errorf("error reading psql output: %v", err)
	}
	return string(output)
}

func getTableInfosForSchema(t *testing.T, db *pgxpool.Pool, schema string) (out []tableInfo) {
	row := db.QueryRow(
		context.Background(),
		`SELECT array_agg(relname::TEXT order by relname::TEXT)
			FROM pg_class 
			WHERE relnamespace=$1::TEXT::regnamespace AND relkind='r'
			AND (relnamespace, relname) != ('_ps_catalog'::regnamespace, 'migration')
			`,
		schema,
	)
	var tables []string
	err := row.Scan(&tables)
	if err != nil {
		t.Errorf("could not get table info for schema \"%s\" due to: %v", schema, err)
		return
	}

	numTables := len(tables)
	if schema == "_ps_catalog" {
		numTables = numTables + 1
	}
	out = make([]tableInfo, numTables)
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

	// special handling for _ps_catalog.migration. we don't want to compare applied_at or applied_at_version
	if schema == "_ps_catalog" {
		row = db.QueryRow(context.Background(), "select array_agg(x.name order by x.applied_at) from _ps_catalog.migration x")
		out[numTables-1].name = "migration"
		if err = row.Scan(&out[numTables-1].values); err != nil {
			t.Errorf("error querying values from table _ps_catalog.migration: %v", err)
			return
		}
	}
	return
}
