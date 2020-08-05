// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel/migrations"
)

const (
	timescaleInstall            = "CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;"
	extensionInstall            = "CREATE EXTENSION IF NOT EXISTS timescale_prometheus_extra WITH SCHEMA %s;"
	metadataUpdateWithExtension = "SELECT update_tsprom_metadata($1, $2, $3)"
	metadataUpdateNoExtension   = "INSERT INTO _timescaledb_catalog.metadata(key, value, include_in_telemetry) VALUES ($1, $2, $3) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, include_in_telemetry = EXCLUDED.include_in_telemetry"
	createMigrationsTable       = "CREATE TABLE IF NOT EXISTS prom_schema_migrations (version text not null primary key)"
	getVersion                  = "SELECT version FROM prom_schema_migrations LIMIT 1"
	setVersion                  = "INSERT INTO prom_schema_migrations (version) VALUES ($1)"
	truncateMigrationsTable     = "TRUNCATE prom_schema_migrations"

	preinstallScripts = "preinstall"
	versionScripts    = "versions"
	idempotentScripts = "idempotent"

	// TODO: once we have a schema upgrade version for migration scripts,
	// add tests and remove this flag
	upgradeUntested = true
)

var (
	ExtensionIsInstalled = false

	toc = map[string][]string{
		"idempotent": {
			"base.sql",
			"matcher-functions.sql",
		},
	}
	migrateMutex = &sync.Mutex{}
)

type VersionInfo struct {
	Version    string
	CommitHash string
}

type prefixedName struct {
	prefix int
	name   string
}

type prefixedNames []prefixedName

func (p prefixedNames) Len() int {
	return len(p)
}

func (p prefixedNames) Less(i, j int) bool {
	return p[i].prefix < p[j].prefix
}

func (p prefixedNames) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p prefixedNames) getNames() []string {
	names := make([]string, len(p))
	for i, e := range p {
		names[i] = e.name
	}
	return names
}

// Migrate performs a database migration to the latest version
func Migrate(db *pgxpool.Pool, versionInfo VersionInfo) (err error) {
	migrateMutex.Lock()
	defer migrateMutex.Unlock()
	ExtensionIsInstalled = false

	// Getting an early connection to install the extra extension.
	// TODO: Investigate why this is required. Installing the extension on the
	// migration connection fails, thats why this is necessary. This should be removed.
	conn, err := db.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("could not acquire connection: %w", err)
	}

	defer conn.Release()

	appVersion, err := semver.Make(versionInfo.Version)
	if err != nil {
		return fmt.Errorf("app version is not semver format, aborting migration")
	}
	if err := ensureVersionTable(db); err != nil {
		return fmt.Errorf("error ensuring version table: %w", err)
	}
	dbVersion, err := getDBVersion(db)
	if err != nil {
		return fmt.Errorf("failed to get the version from database: %w", err)
	}

	// If already at correct version, nothing to migrate.
	if dbVersion.Compare(appVersion) == 0 {
		installExtension(conn)

		metadataUpdate(db, ExtensionIsInstalled, "version", versionInfo.Version)
		metadataUpdate(db, ExtensionIsInstalled, "commit_hash", versionInfo.CommitHash)
		return nil
	}

	// Error if at a greater version.
	if dbVersion.Compare(appVersion) > 0 {
		return fmt.Errorf("schema version is above the application version, cannot migrate")
	}

	tx, err := db.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("unable to start transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	_, err = tx.Exec(context.Background(), timescaleInstall)
	if err != nil {
		return fmt.Errorf("timescaledb failed to install due to %w", err)
	}

	// No version in DB.
	if dbVersion.Compare(semver.Version{}) == 0 {
		if err = execMigrationFiles(tx, preinstallScripts); err != nil {
			return err
		}
	} else if err = upgradeVersion(tx, dbVersion, appVersion); err != nil {
		return err
	}
	if err = execMigrationFiles(tx, idempotentScripts); err != nil {
		return err
	}
	if err = setDBVersion(tx, &appVersion); err != nil {
		return fmt.Errorf("error setting clean app version to DB: %w", err)
	}

	if err = tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("unable to commit migration transaction: %w", err)
	}

	installExtension(conn)

	metadataUpdate(db, ExtensionIsInstalled, "version", versionInfo.Version)
	metadataUpdate(db, ExtensionIsInstalled, "commit_hash", versionInfo.CommitHash)

	return nil
}

func ensureVersionTable(db *pgxpool.Pool) error {
	_, err := db.Exec(context.Background(), createMigrationsTable)
	if err != nil {
		return fmt.Errorf("error creating migration table: %w", err)
	}

	return nil
}

func getDBVersion(db *pgxpool.Pool) (semver.Version, error) {
	var version semver.Version
	res, err := db.Query(context.Background(), getVersion)

	if err != nil {
		return version, err
	}

	for res.Next() {
		err = res.Scan(&version)
	}

	if err != nil {
		return version, err
	}

	return version, nil
}

// execMigrationFiles finds all the migration files in a directory, orders them
// (either by ToC or by their numerical prefix) and executes them in a transaction.
func execMigrationFiles(tx pgx.Tx, dirName string) error {
	f, err := migrations.MigrationFiles.Open(dirName)
	if err != nil {
		return fmt.Errorf("unable to get migration scripts: name %s, err %w", dirName, err)
	}

	var (
		entries []string
		stat    os.FileInfo
		file    http.File
	)

	if myToC, ok := toc[dirName]; ok {
		// If exists, use ToC to order the migration files before executing them.
		entries = make([]string, 0, len(myToC))
		for _, fileName := range myToC {
			fullName := filepath.Join(dirName, fileName)
			file, err = migrations.MigrationFiles.Open(fullName)
			if err != nil {
				return fmt.Errorf("unable to get migration script from toc: name %s, err %w", fullName, err)
			}

			if stat, err = file.Stat(); err != nil {
				return fmt.Errorf("unable to stat migration script from toc: name %s, err %w", fullName, err)
			}

			// Ignoring directories.
			if stat.IsDir() {
				log.Warn("msg", "Ignoring directory entry in migration ToC", "dir", dirName)
				continue
			}

			entries = append(entries, fileName)
		}
	} else {
		// Otherwise, order the files by their numeric prefix, delimited by `-` (if one exists).
		fileEntries, err := f.Readdir(-1)
		if err != nil {
			return fmt.Errorf("unable to read migration scripts directory: name %s, err %w", dirName, err)
		}

		entries = orderFilesNaturally(fileEntries)
	}

	for _, e := range entries {
		fileName := filepath.Join(dirName, e)
		f, err := migrations.MigrationFiles.Open(fileName)
		if err != nil {
			return fmt.Errorf("unable to get migration script: name %s, err %w", fileName, err)
		}
		contents, err := replaceSchemaNames(f)
		if err != nil {
			return fmt.Errorf("unable to read migration script: name %s, err %w", fileName, err)
		}
		_, err = tx.Exec(context.Background(), string(contents))
		if err != nil {
			return fmt.Errorf("error executing migration script: name %s, err %w", fileName, err)
		}
	}

	return nil
}

// orderFilesNaturally orders the file names by their numberic prefix, ignoring
// directories or files which are not formatted correctly.
func orderFilesNaturally(entries []os.FileInfo) []string {
	pp := prefixedNames{}
	var (
		prefix int
		name   string
	)

	for _, entry := range entries {
		// Ignoring directories.
		if entry.IsDir() {
			log.Warn("msg", "Ignoring directory while ordering migration script files naturally", "dirname", entry.Name())
			continue
		}

		_, err := fmt.Sscanf(entry.Name(), "%d-%s", &prefix, &name)
		// Ignore malformated file names.
		if err != nil {
			log.Warn("msg", "Ignoring malformed file name in migration scripts", "filename", entry.Name())
			continue
		}
		pp = append(pp, prefixedName{prefix, entry.Name()})
	}

	sort.Sort(pp)
	return pp.getNames()
}

func replaceSchemaNames(r io.ReadCloser) (string, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r)
	if err != nil {
		return "", err
	}
	err = r.Close()
	if err != nil {
		return "", err
	}
	s := buf.String()
	s = strings.ReplaceAll(s, "SCHEMA_CATALOG", catalogSchema)
	s = strings.ReplaceAll(s, "SCHEMA_EXT", extSchema)
	s = strings.ReplaceAll(s, "SCHEMA_PROM", promSchema)
	s = strings.ReplaceAll(s, "SCHEMA_SERIES", seriesViewSchema)
	s = strings.ReplaceAll(s, "SCHEMA_METRIC", metricViewSchema)
	s = strings.ReplaceAll(s, "SCHEMA_DATA_SERIES", dataSeriesSchema)
	s = strings.ReplaceAll(s, "SCHEMA_DATA", dataSchema)
	s = strings.ReplaceAll(s, "SCHEMA_INFO", infoSchema)
	return s, err
}

// upgradeVersion finds all the versions between `from` and `to`, sorts them
// using semantic version ordering and applies them sequentially in the supplied transaction.
func upgradeVersion(tx pgx.Tx, from, to semver.Version) error {
	if upgradeUntested {
		return fmt.Errorf("unsupported version upgrade reached, aborting migration")
	}
	f, err := migrations.MigrationFiles.Open(versionScripts)
	if err != nil {
		return fmt.Errorf("unable to open migration scripts: %w", err)
	}

	entries, err := f.Readdir(-1)
	if err != nil {
		return fmt.Errorf("unable to get migration scripts: %w", err)
	}

	versions := make(semver.Versions, 0, len(entries))

	for _, e := range entries {
		version, err := semver.Make(e.Name())

		// Ignoring malformated entries.
		if err != nil {
			log.Warn("msg", "Ignoring malformed dir name in migration version directories (not semver)", "dirname", e.Name(), "err", err.Error())
			continue
		}

		versions = append(versions, version)
	}

	sort.Sort(versions)

	for _, e := range entries {
		version, err := semver.Make(e.Name())
		if err != nil {
			log.Warn("msg", "ignoring sql migration script version", "versionName", e.Name())
			continue
		}

		if from.Compare(version) < 0 && to.Compare(version) >= 0 {
			if err = execMigrationFiles(tx, filepath.Join("versions", e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func installExtension(conn *pgxpool.Conn) {
	_, extErr := conn.Exec(context.Background(), fmt.Sprintf(extensionInstall, extSchema))
	if extErr != nil {
		log.Warn("msg", "timescale_prometheus_extra extension not installed", "cause", extErr)
		ExtensionIsInstalled = false
	} else {
		ExtensionIsInstalled = true
	}
}

func setDBVersion(tx pgx.Tx, version *semver.Version) error {
	if _, err := tx.Exec(context.Background(), truncateMigrationsTable); err != nil {
		return fmt.Errorf("unable to truncate migrations table: %w", err)
	}

	if _, err := tx.Exec(context.Background(), setVersion, version.String()); err != nil {
		return fmt.Errorf("unable to set version in DB: %w", err)
	}

	return nil
}

func metadataUpdate(db *pgxpool.Pool, withExtension bool, key string, value string) {
	/* Ignore error if it doesn't work */
	if withExtension {
		_, _ = db.Exec(context.Background(), metadataUpdateWithExtension, key, value, true)
	} else {
		_, _ = db.Exec(context.Background(), metadataUpdateNoExtension, key, value, true)
	}
}
