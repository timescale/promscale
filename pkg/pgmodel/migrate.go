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
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel/migrations"
	"github.com/timescale/timescale-prometheus/pkg/version"
)

const (
	timescaleInstall            = "CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;"
	metadataUpdateWithExtension = "SELECT update_tsprom_metadata($1, $2, $3)"
	metadataUpdateNoExtension   = "INSERT INTO _timescaledb_catalog.metadata(key, value, include_in_telemetry) VALUES ('timescale_prometheus_' || $1, $2, $3) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, include_in_telemetry = EXCLUDED.include_in_telemetry"
	createMigrationsTable       = "CREATE TABLE IF NOT EXISTS prom_schema_migrations (version text not null primary key)"
	getVersion                  = "SELECT version FROM prom_schema_migrations LIMIT 1"
	setVersion                  = "INSERT INTO prom_schema_migrations (version) VALUES ($1)"
	truncateMigrationsTable     = "TRUNCATE prom_schema_migrations"

	preinstallScripts = "preinstall"
	versionScripts    = "versions/dev"
	idempotentScripts = "idempotent"
)

var (
	ExtensionIsInstalled = false

	tableOfContets = map[string][]string{
		"idempotent": {
			"base.sql",
			"matcher-functions.sql",
		},
	}
	migrateMutex = &sync.Mutex{}

	//Format of migration files. e.g. 6-foo.sql
	migrationFileNameRegexp = regexp.MustCompile(`([[:digit:]]+)-[[:word:]]+.sql`)
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

	appVersion, err := semver.Make(versionInfo.Version)
	if err != nil {
		return fmt.Errorf("app version is not semver format, aborting migration")
	}

	mig := NewMigrator(db, migrations.MigrationFiles, tableOfContets)

	err = mig.Migrate(appVersion)
	if err != nil {
		return fmt.Errorf("Error encountered during migration: %w", err)
	}

	err = installExtension(db)
	if err != nil {
		return fmt.Errorf("Error encountered while installing extension: %w", err)
	}

	metadataUpdate(db, ExtensionIsInstalled, "version", versionInfo.Version)
	metadataUpdate(db, ExtensionIsInstalled, "commit_hash", versionInfo.CommitHash)
	return nil
}

type Migrator struct {
	db       *pgxpool.Pool
	sqlFiles http.FileSystem
	toc      map[string][]string
}

func NewMigrator(db *pgxpool.Pool, sqlFiles http.FileSystem, toc map[string][]string) *Migrator {
	return &Migrator{db: db, sqlFiles: sqlFiles, toc: toc}
}

func (t *Migrator) Migrate(appVersion semver.Version) error {
	if err := ensureVersionTable(t.db); err != nil {
		return fmt.Errorf("error ensuring version table: %w", err)
	}

	dbVersion, err := getDBVersion(t.db)
	if err != nil {
		return fmt.Errorf("failed to get the version from database: %w", err)
	}

	// If already at correct version, nothing to migrate on proper release.
	// On dev versions, idempotent files need to be reapplied.
	if dbVersion.Compare(appVersion) == 0 {
		devRelease := false
		for _, pre := range appVersion.Pre {
			if pre.String() == "dev" {
				devRelease = true
			}
		}

		if devRelease {
			tx, err := t.db.Begin(context.Background())
			if err != nil {
				return fmt.Errorf("unable to start transaction: %w", err)
			}
			defer func() {
				_ = tx.Rollback(context.Background())
			}()
			if err = t.execMigrationDir(tx, idempotentScripts); err != nil {
				return err
			}
			if err = tx.Commit(context.Background()); err != nil {
				return fmt.Errorf("unable to commit migration transaction: %w", err)
			}
			return nil
		}
		return nil
	}

	// Error if at a greater version.
	if dbVersion.Compare(appVersion) > 0 {
		return fmt.Errorf("schema version is above the application version, cannot migrate")
	}

	tx, err := t.db.Begin(context.Background())
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
		if err = t.execMigrationDir(tx, preinstallScripts); err != nil {
			return err
		}
	} else if err = t.upgradeVersion(tx, dbVersion, appVersion); err != nil {
		return err
	}
	if err = t.execMigrationDir(tx, idempotentScripts); err != nil {
		return err
	}
	if err = setDBVersion(tx, &appVersion); err != nil {
		return fmt.Errorf("error setting clean app version to DB: %w", err)
	}

	if err = tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("unable to commit migration transaction: %w", err)
	}

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
		return version, fmt.Errorf("Error getting DB version: %w", err)
	}

	for res.Next() {
		err = res.Scan(&version)
	}

	if err != nil {
		return version, fmt.Errorf("Error getting DB version: %w", err)
	}

	return version, nil
}

func (t *Migrator) execMigrationFile(tx pgx.Tx, fileName string) error {
	f, err := t.sqlFiles.Open(fileName)
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
	return nil
}

// execMigrationDir finds all the migration files in a directory, orders them
// (either by ToC or by their numerical prefix) and executes them in a transaction.
func (t *Migrator) execMigrationDir(tx pgx.Tx, dirName string) error {
	f, err := t.sqlFiles.Open(dirName)
	if err != nil {
		return fmt.Errorf("unable to get migration scripts: name %s, err %w", dirName, err)
	}

	var (
		entries []string
		stat    os.FileInfo
		file    http.File
	)

	if myToC, ok := t.toc[dirName]; ok {
		// If exists, use ToC to order the migration files before executing them.
		entries = make([]string, 0, len(myToC))
		for _, fileName := range myToC {
			fullName := filepath.Join(dirName, fileName)
			file, err = t.sqlFiles.Open(fullName)
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
		err := t.execMigrationFile(tx, fileName)
		if err != nil {
			return err
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

//A migration file is inside a directory that is a semver version number. The filename itself has the format
//<migration file number)-<description>.sql. That file correspond to the semver of <dirname>.<migration file number>
//where the migration file number is always part of prerelease tag.
//All app versions >= (inclusive) migration files's semver will include the migration file
//That is if we're on version `0.1.1-dev.3` then we'll include all sql files up to and including `0.1.1-dev/3-foo.sql`
func (t *Migrator) getMigrationFileVersion(dirName string, fileName string) (*semver.Version, error) {
	var migrationFileNumber int
	matches := migrationFileNameRegexp.FindStringSubmatch(fileName)
	if len(matches) < 2 {
		return nil, fmt.Errorf("unable to parse the migration file name %v", fileName)
	}
	n, err := fmt.Sscanf(matches[1], "%d", &migrationFileNumber)
	if n != 1 || err != nil {
		return nil, fmt.Errorf("unable to parse the migration file name %v: %w", fileName, err)
	}

	migrationFileVersion, err := semver.Make(dirName)
	if err != nil {
		return nil, fmt.Errorf("unable to parse version from directory %v: %w", dirName, err)
	}
	migrationNumberPreReleaseVersion, err := semver.NewPRVersion(fmt.Sprintf("%d", migrationFileNumber))
	if err != nil {
		return nil, fmt.Errorf("unable to create dev PR version: %w", err)
	}

	migrationFileVersion.Pre = append(migrationFileVersion.Pre, migrationNumberPreReleaseVersion)
	return &migrationFileVersion, nil
}

// upgradeVersion finds all the versions between `from` and `to`, sorts them
// using semantic version ordering and applies them sequentially in the supplied transaction.
func (t *Migrator) upgradeVersion(tx pgx.Tx, from, to semver.Version) error {
	devDirFile, err := t.sqlFiles.Open(versionScripts)
	if err != nil {
		return fmt.Errorf("unable to open %v directory: %w", versionScripts, err)
	}

	versionDirInfoEntries, err := devDirFile.Readdir(-1)
	if err != nil {
		return fmt.Errorf("unable to get %v directory entries: %w", versionScripts, err)
	}

	versions := make(semver.Versions, 0)
	versionMap := make(map[string]string)

	for _, versionDirInfo := range versionDirInfoEntries {
		if !versionDirInfo.IsDir() {
			if versionDirInfo.Name() == ".gitignore" {
				continue
			}
			return fmt.Errorf("Not a directory inside %v: %v", versionScripts, versionDirInfo.Name())
		}

		versionDirPath := versionScripts + "/" + versionDirInfo.Name()
		versionDirFile, err := t.sqlFiles.Open(versionDirPath)
		if err != nil {
			return fmt.Errorf("unable to open migration scripts inside %v: %w", versionDirPath, err)
		}

		migrationFileInfoEntries, err := versionDirFile.Readdir(-1)
		if err != nil {
			return fmt.Errorf("unable to get %v directory entries: %w", versionDirPath, err)
		}

		for _, migrationFileInfo := range migrationFileInfoEntries {
			migrationFileVersion, err := t.getMigrationFileVersion(versionDirInfo.Name(), migrationFileInfo.Name())
			if err != nil {
				return err
			}
			migrationFilePath := versionDirPath + "/" + migrationFileInfo.Name()

			_, existing := versionMap[migrationFileVersion.String()]
			if existing {
				return fmt.Errorf("Found two migration files with the same version: %v", migrationFileVersion.String())
			}
			versionMap[migrationFileVersion.String()] = migrationFilePath
			versions = append(versions, *migrationFileVersion)
		}
	}

	sort.Sort(versions)

	for _, v := range versions {
		//When comparing to the latest version use >= (INCLUSIVE). A migration file
		//that's marked as version X is part of that version
		if from.Compare(v) < 0 && to.Compare(v) >= 0 {
			filename := versionMap[v.String()]
			if err = t.execMigrationFile(tx, filename); err != nil {
				return err
			}
		}
	}
	return nil
}

func installExtension(conn *pgxpool.Pool) error {
	availableVersions, err := fetchAvailableExtensionVersions(conn)
	ExtensionIsInstalled = false
	if err != nil {
		return err
	}
	if len(availableVersions) == 0 {
		log.Warn("msg", "timescale_prometheus_extra is not available, proceeding without extension")
		return nil
	}

	newVersion, ok := getNewExtensionVersion(availableVersions)
	if !ok {
		log.Warn("msg", "timescale_prometheus_extra is not available at the right version, need version: %v, proceeding without extension", version.ExtVersionRangeString)
		return nil
	}

	currentVersion, isInstalled, err := fetchInstalledExtensionVersion(conn)
	if err != nil {
		return fmt.Errorf("Could not get the installed extension version: %w", err)
	}

	if !isInstalled {
		_, extErr := conn.Exec(context.Background(),
			fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS timescale_prometheus_extra WITH SCHEMA %s VERSION '%s'",
				extSchema, getSqlVersion(newVersion)))
		if extErr != nil {
			return extErr
		}
		ExtensionIsInstalled = true
		return extErr
	}

	comparator := currentVersion.Compare(newVersion)
	if comparator > 0 {
		//currentVersion greater than what we can handle, don't use the extension
		log.Warn("msg", "timescale_prometheus_extra at a greater version than supported by the connector: %v > %v, proceeding without extension", currentVersion, newVersion)
		ExtensionIsInstalled = false
		return nil
	} else if comparator == 0 {
		//Nothing to do we are at the correct version
		ExtensionIsInstalled = true
		return nil
	} else {
		//Upgrade to the right version
		_, err := conn.Exec(context.Background(),
			fmt.Sprintf("ALTER EXTENSION timescale_prometheus_extra UPDATE TO '%s'",
				getSqlVersion(newVersion)))
		if err != nil {
			return err
		}
		ExtensionIsInstalled = true
		return nil
	}
}

func fetchAvailableExtensionVersions(conn *pgxpool.Pool) (semver.Versions, error) {
	var versionStrings []string
	versions := make(semver.Versions, 0)
	err := conn.QueryRow(context.Background(),
		"SELECT array_agg(version) FROM pg_available_extension_versions WHERE name ='timescale_prometheus_extra'").Scan(&versionStrings)

	if err != nil {
		return versions, err
	}
	if len(versionStrings) == 0 {
		return versions, nil
	}

	for i := range versionStrings {
		vString := correctVersionString(versionStrings[i])
		v, err := semver.Parse(vString)
		if err != nil {
			return versions, fmt.Errorf("Could not parse available extra extension version %v: %w", vString, err)
		}
		versions = append(versions, v)
	}

	return versions, nil
}

func fetchInstalledExtensionVersion(conn *pgxpool.Pool) (semver.Version, bool, error) {
	var versionString string
	err := conn.QueryRow(context.Background(),
		"SELECT extversion  FROM pg_extension WHERE extname='timescale_prometheus_extra'").Scan(&versionString)
	if err != nil {
		if err == pgx.ErrNoRows {
			return semver.Version{}, false, nil
		}
		return semver.Version{}, true, err
	}

	versionString = correctVersionString(versionString)

	v, err := semver.Parse(versionString)
	if err != nil {
		return v, true, fmt.Errorf("Could not parse current timescale_prometheus_extra extension version %v: %w", versionString, err)
	}
	return v, true, nil
}

func correctVersionString(v string) string {
	//we originally published the extension as "0.1" which isn't a valid semver
	if v == "0.1" {
		return "0.1.0"
	}
	return v
}

func getSqlVersion(v semver.Version) string {
	if v.String() == "0.1.0" {
		return "0.1"
	}
	return v.String()
}

// getNewExtensionVersion returns the highest version allowed by ExtVersionRange
func getNewExtensionVersion(availableVersions semver.Versions) (semver.Version, bool) {
	//sort higher extensions first
	sort.Sort(sort.Reverse(availableVersions))
	for i := range availableVersions {
		if version.ExtVersionRange(availableVersions[i]) {
			return availableVersions[i], true
		}
	}
	return semver.Version{}, false
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
