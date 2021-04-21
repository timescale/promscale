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
	"strconv"
	"strings"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/migrations"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
)

const (
	createMigrationsTable   = "CREATE TABLE IF NOT EXISTS prom_schema_migrations (version text not null primary key)"
	getVersion              = "SELECT version FROM prom_schema_migrations LIMIT 1"
	setVersion              = "INSERT INTO prom_schema_migrations (version) VALUES ($1)"
	truncateMigrationsTable = "TRUNCATE prom_schema_migrations"

	preinstallScripts = "preinstall"
	versionScripts    = "versions/dev"
	idempotentScripts = "idempotent"
)

var (
	tableOfContets = map[string][]string{
		"idempotent": {
			"base.sql",
			"matcher-functions.sql",
			"ha.sql",
			"apply_permissions.sql", //should be last
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
func Migrate(db *pgx.Conn, versionInfo VersionInfo, extOptions extension.ExtensionMigrateOptions) (err error) {
	migrateMutex.Lock()
	defer migrateMutex.Unlock()

	appVersion, err := semver.Make(versionInfo.Version)
	if err != nil {
		return errors.ErrInvalidSemverFormat
	}

	mig := NewMigrator(db, migrations.MigrationFiles, tableOfContets)

	err = mig.Migrate(appVersion)
	if err != nil {
		return fmt.Errorf("Error encountered during migration: %w", err)
	}

	return nil
}

// CheckDependencies makes sure all project dependencies, including the DB schema
// the extension, are set up correctly. This will set the ExtensionIsInstalled
// flag and thus should only be called once, at initialization.
func CheckDependencies(db *pgx.Conn, versionInfo VersionInfo, migrationFailedDueToLockError bool, extOptions extension.ExtensionMigrateOptions) (err error) {
	if err = CheckSchemaVersion(context.Background(), db, versionInfo, migrationFailedDueToLockError); err != nil {
		return err
	}
	return extension.CheckVersions(db, migrationFailedDueToLockError, extOptions)
}

// CheckSchemaVersion checks the DB schema version without checking the extension
func CheckSchemaVersion(ctx context.Context, conn *pgx.Conn, versionInfo VersionInfo, migrationFailedDueToLockError bool) error {
	expectedVersion := semver.MustParse(versionInfo.Version)
	dbVersion, err := getSchemaVersionOnConnection(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to check schema version: %w", err)
	}
	if versionCompare := dbVersion.Compare(expectedVersion); versionCompare != 0 {
		if versionCompare < 0 && migrationFailedDueToLockError {
			return fmt.Errorf("Failed to acquire the migration lock to upgrade the schema version and unable to run with the old version. Please ensure that no other Promscale connectors with the old schema version are running. Received schema version %v but expected %v", dbVersion, expectedVersion)
		}
		return fmt.Errorf("Error while comparing schema version: received schema version %v but expected %v", dbVersion, expectedVersion)
	}
	return nil
}

type Migrator struct {
	db       *pgx.Conn
	sqlFiles http.FileSystem
	toc      map[string][]string
}

func NewMigrator(db *pgx.Conn, sqlFiles http.FileSystem, toc map[string][]string) *Migrator {
	return &Migrator{db: db, sqlFiles: sqlFiles, toc: toc}
}

func (t *Migrator) Migrate(appVersion semver.Version) error {
	if err := ensureVersionTable(t.db); err != nil {
		return fmt.Errorf("error ensuring version table: %w", err)
	}

	dbVersion, err := getSchemaVersion(t.db)
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
		return fmt.Errorf("schema version (%v) is above the application version (%v), cannot migrate", dbVersion, appVersion)
	}

	tx, err := t.db.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("unable to start transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

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

func ensureVersionTable(db *pgx.Conn) error {
	_, err := db.Exec(context.Background(), createMigrationsTable)
	if err != nil {
		return fmt.Errorf("error creating migration table: %w", err)
	}

	_, err = db.Exec(context.Background(), "GRANT SELECT ON prom_schema_migrations TO public")
	if err != nil {
		return fmt.Errorf("error creating migration table: %w", err)
	}

	return nil
}

func getSchemaVersion(db *pgx.Conn) (semver.Version, error) {
	return getSchemaVersionOnConnection(context.Background(), db)
}

func getSchemaVersionOnConnection(ctx context.Context, db *pgx.Conn) (semver.Version, error) {
	var version semver.Version
	res, err := db.Query(ctx, getVersion)
	if err != nil {
		return version, fmt.Errorf("Error getting DB version: %w", err)
	}
	defer res.Close()

	for res.Next() {
		err = res.Scan(&version)
	}
	if err != nil {
		return version, fmt.Errorf("Error getting DB version: %w", err)
	}
	err = res.Err()
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
		//special handling if we know the position of the error
		pgErr, ok := err.(*pgconn.PgError)
		if ok && pgErr.Position > 0 {
			strC := string(contents)
			code := strC[pgErr.Position-1:]
			return fmt.Errorf("error executing migration script: name %s, err %w, code at error position:\n  %s", fileName, err, code)

		}
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
	s = strings.ReplaceAll(s, "SCHEMA_CATALOG", schema.Catalog)
	s = strings.ReplaceAll(s, "SCHEMA_LOCK_ID", strconv.FormatInt(schema.LockID, 10))
	s = strings.ReplaceAll(s, "SCHEMA_EXT", schema.Ext)
	s = strings.ReplaceAll(s, "SCHEMA_PROM", schema.Prom)
	s = strings.ReplaceAll(s, "SCHEMA_SERIES", schema.SeriesView)
	s = strings.ReplaceAll(s, "SCHEMA_METRIC", schema.MetricView)
	s = strings.ReplaceAll(s, "SCHEMA_DATA_SERIES", schema.DataSeries)
	s = strings.ReplaceAll(s, "SCHEMA_DATA", schema.Data)
	s = strings.ReplaceAll(s, "SCHEMA_INFO", schema.Info)
	s = strings.ReplaceAll(s, "ADVISORY_LOCK_PREFIX_JOB", "12377")
	s = strings.ReplaceAll(s, "ADVISORY_LOCK_PREFIX_MAINTENACE", "12378")
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

func setDBVersion(tx pgx.Tx, version *semver.Version) error {
	if _, err := tx.Exec(context.Background(), truncateMigrationsTable); err != nil {
		return fmt.Errorf("unable to truncate migrations table: %w", err)
	}

	if _, err := tx.Exec(context.Background(), setVersion, version.String()); err != nil {
		return fmt.Errorf("unable to set version in DB: %w", err)
	}

	return nil
}
