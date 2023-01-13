// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package extension

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v5"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/version"
)

var (
	PromscaleExtensionVersion semver.Version
)

type ExtensionMigrateOptions struct {
	Install           bool
	Upgrade           bool
	UpgradePreRelease bool
}

// InstallUpgradeTimescaleDBExtensions installs or updates TimescaleDB
// Note that after this call any previous connections can break
// so this has to be called ahead of opening connections.
//
// Also this takes a connection string not a connection because for
// updates the ALTER has to be the first command on the connection
// thus we cannot reuse existing connections
func InstallUpgradeTimescaleDBExtensions(connstr string, extOptions ExtensionMigrateOptions) error {
	db, err := pgx.Connect(context.Background(), connstr)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close(context.Background()) }()

	err = MigrateExtension(db, "timescaledb", schema.Public, version.TimescaleVersionRange, version.TimescaleVersionRangeString, extOptions)
	if err != nil {
		return fmt.Errorf("could not install timescaledb: %w", err)
	}

	return nil

}

func InstallUpgradePromscaleExtensions(db *pgx.Conn, extOptions ExtensionMigrateOptions) error {
	err := MigrateExtension(db, "promscale", schema.Public, version.ExtVersionRange, version.ExtVersionRangeString, extOptions)
	if err != nil {
		log.Warn("msg", fmt.Sprintf("could not install promscale: %v. continuing without extension", err))
	}

	if err = CheckExtensionsVersion(db, false, extOptions); err != nil {
		return fmt.Errorf("error encountered while migrating extension: %w", err)
	}

	return nil
}

func CheckPromscaleVersion(conn *pgx.Conn) error {
	extensionVersion, b, err := FetchInstalledExtensionVersion(conn, "promscale")
	if err != nil {
		return err
	}
	if !b {
		return fmt.Errorf("the promscale extension is required but is not installed")
	}
	if !version.ExtVersionRange(extensionVersion) {
		return fmt.Errorf("the promscale extension is required but the installed version %v is not supported. supported versions: %v", extensionVersion, version.ExtVersionRangeString)
	}
	return nil
}

// CheckVersions is responsible for verifying the version compatibility of installed Postgresql database and extensions.
func CheckVersions(conn *pgx.Conn, migrationFailedDueToLockError bool, extOptions ExtensionMigrateOptions) error {
	if err := checkPgVersion(conn); err != nil {
		return fmt.Errorf("problem checking PostgreSQL version: %w", err)
	}
	if err := CheckExtensionsVersion(conn, migrationFailedDueToLockError, extOptions); err != nil {
		return fmt.Errorf("problem with extension: %w", err)
	}
	return nil
}

func checkPgVersion(conn *pgx.Conn) error {
	var versionString string
	if err := conn.QueryRow(context.Background(), "SHOW server_version_num;").Scan(&versionString); err != nil {
		return fmt.Errorf("error fetching postgresql version: %w", err)
	}
	// Semver valid versions need to satisfy major, minor and patch numbers respectively. Also, none
	// of these can be preceded by "0", otherwise would be rendered invalid.
	//
	// Postgres server_version_num outputs a 6 digit number. Out of the 6 digits, the first two and
	// the last two are the significant ones. The ones in the middle are always 0 unless Postgres
	// releases a 3 digit version (i.e., 100+ version). This means that middle two should be removed
	// since if they are not, then they will lead to invalid parsing of semver version.
	// Reference: https://bit.ly/3lOnUNh
	versionString = fmt.Sprintf("%s.%s.0", versionString[:2], trimLeadingZeros(versionString[4:]))
	v, err := semver.Parse(versionString)
	if err != nil {
		return fmt.Errorf("could not parse postgresql version string: %w", err)
	}
	if !version.VerifyPgVersion(v) {
		return fmt.Errorf("Incompatible postgresql version. Supported server version %s", version.PgVersionNumRange)
	}
	return nil
}

// CheckExtensionsVersion checks for the correct version and enables the extension if
// it is at the right version
func CheckExtensionsVersion(conn *pgx.Conn, migrationFailedDueToLockError bool, extOptions ExtensionMigrateOptions) error {
	if err := checkTimescaleDBVersion(conn); err != nil {
		return err
	}
	if err := checkPromscaleExtensionVersion(conn, migrationFailedDueToLockError, extOptions); err != nil {
		return fmt.Errorf("problem checking promscale extension version: %w", err)
	}
	return nil
}

func checkTimescaleDBVersion(conn *pgx.Conn) error {
	timescaleVersion, isInstalled, err := FetchInstalledExtensionVersion(conn, "timescaledb")
	if err != nil {
		return fmt.Errorf("could not get the installed extension version: %w", err)
	}
	if !isInstalled {
		return fmt.Errorf("the timescaledb extension is not installed, unable to run Promscale without timescaledb")
	}
	if !version.VerifyTimescaleVersion(timescaleVersion) {
		safeRanges := strings.Split(version.TimescaleVersionRangeString, " ")
		return fmt.Errorf("incompatible Timescaledb version: %s. Expected version within %s to %s", timescaleVersion.String(), safeRanges[0], safeRanges[1])
	}
	return nil
}

func checkPromscaleExtensionVersion(conn *pgx.Conn, migrationFailedDueToLockError bool, extOptions ExtensionMigrateOptions) error {
	currentVersion, newVersion, err := extensionVersions(conn, "promscale", version.ExtVersionRange, version.ExtVersionRangeString, extOptions)
	if err != nil {
		if err == errors.ErrExtUnavailable {
			//the promscale extension is optional
			return nil
		}
		return fmt.Errorf("could not get the extension versions: %w", err)
	}
	if (currentVersion == nil || currentVersion.Compare(*newVersion) < 0) && migrationFailedDueToLockError {
		log.Warn("msg", "Unable to install/update the Promscale extension; failed to acquire the lock. Ensure there are no other connectors running and try again.")
	}
	if currentVersion == nil {
		return fmt.Errorf("promscale extension is required but is not installed")
	}
	if version.ExtVersionRange(*currentVersion) {
		PromscaleExtensionVersion = *currentVersion
	} else {
		return fmt.Errorf("promscale extension is installed but on an unsupported version: %s. Expected: %s", *currentVersion, version.ExtVersionRangeString)
	}
	return nil
}

func extensionVersions(conn *pgx.Conn, extName string, validRange semver.Range, rangeString string, extOptions ExtensionMigrateOptions) (currentVersion *semver.Version, newVersion *semver.Version, err error) {
	availableVersions, err := fetchAvailableExtensionVersions(conn, extName)
	if err != nil {
		return nil, nil, fmt.Errorf("problem fetching available version: %w", err)
	}
	if len(availableVersions) == 0 {
		return nil, nil, errors.ErrExtUnavailable
	}

	defaultVersion, err := fetchDefaultExtensionVersions(conn, extName)
	if err != nil {
		return nil, nil, fmt.Errorf("problem fetching default version: %w", err)
	}

	current, isInstalled, err := FetchInstalledExtensionVersion(conn, extName)
	if err != nil {
		return nil, nil, fmt.Errorf("problem getting the installed extension version: %w", err)
	}

	new, ok := getNewExtensionVersion(extName, availableVersions, defaultVersion, validRange, isInstalled, extOptions.UpgradePreRelease, current)
	if !ok {
		return nil, nil, fmt.Errorf("The %v extension is not available at the right version, need version: %v and the default version is %s ", extName, rangeString, defaultVersion)
	}

	if !isInstalled {
		return nil, &new, nil
	}
	return &current, &new, nil
}

func MigrateExtension(conn *pgx.Conn, extName string, extSchemaName string, validRange semver.Range, rangeString string, extOptions ExtensionMigrateOptions) error {
	currentVersion, newVersion, err := extensionVersions(conn, extName, validRange, rangeString, extOptions)
	if err != nil {
		return err
	}
	isInstalled := currentVersion != nil

	if !isInstalled && !extOptions.Install {
		log.Info("msg", "skipping "+extName+" extension install as install extension is disabled.")
		return nil
	}

	if isInstalled && !extOptions.Upgrade {
		log.Info("msg", "skipping "+extName+" extension upgrade as upgrade extension is disabled. The current extension version is "+currentVersion.String())
		if !validRange(*currentVersion) {
			return fmt.Errorf("The %v extension is not installed at the right version and upgrades are disabled, need version: %v and the installed version is %s ", extName, rangeString, currentVersion)
		}
		return nil
	}

	if !isInstalled {
		var query string
		switch extName {
		case "timescaledb":
			query = fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s VERSION '%s'",
				extName, extSchemaName, getSqlVersion(*newVersion, extName))
		case "promscale":
			query = fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s VERSION '%s'",
				extName, getSqlVersion(*newVersion, extName))
		default:
			return fmt.Errorf("unknown extension: %s", extName)
		}
		_, extErr := conn.Exec(context.Background(), query)
		if extErr != nil {
			return extErr
		}
		log.Info("msg", "successfully created "+extName+" extension at version "+newVersion.String())
		return nil
	}

	comparator := currentVersion.Compare(*newVersion)
	if comparator > 0 {
		//check that we are within the accepted range
		if !validRange(*newVersion) {
			//currentVersion greater than what we can handle, don't use the extension
			return fmt.Errorf("the extension at a greater version than supported by the connector: %v > %v", currentVersion, newVersion)
		}
		//we may be at a higher extension version than what we'd auto upgrade to (e.g. higher than the default version)
		//and that may be fine, so do nothing.
		log.Info("msg", "using a higher extension version than expected", "extension_name", extName, "current_version", currentVersion, "expected_version", newVersion)
		return nil
	} else if comparator == 0 {
		//Nothing to do we are at the correct version
		return nil
	} else {
		//Upgrade to the right version
		connAlter := conn
		if extName == "timescaledb" {
			//TimescaleDB requires a fresh connection for altering
			//Note: all previously opened connections will become invalid
			connAlter, err = pgx.ConnectConfig(context.Background(), conn.Config())
			if err != nil {
				return err
			}
			defer func() { _ = connAlter.Close(context.Background()) }()
		}
		_, err := connAlter.Exec(context.Background(),
			fmt.Sprintf("ALTER EXTENSION %s UPDATE TO '%s'", extName,
				getSqlVersion(*newVersion, extName)))
		// if migration fails, Do not crash just log an error. As there is an extension already present.
		if err != nil {
			if !validRange(*currentVersion) {
				return fmt.Errorf("the %v extension is not installed at the right version and the upgrades failed, need version: %v and the installed version is %s and the upgrade failed with %w ", extName, rangeString, currentVersion, err)
			}
			log.Error("msg", fmt.Sprintf("Failed to migrate extension %v from %v to %v: %v", extName, currentVersion, newVersion, err))
			// We've failed to migrate but we are still in the valid extension range so we can continue to operate.
			// Eventually we can make this more strict later on
			return nil
		}
		log.Info("msg", "successfully updated extension", "extension_name", extName, "old_version", currentVersion, "new_version", newVersion)
	}

	return nil
}

func AreSupportedPromscaleExtensionVersionsAvailable(conn *pgx.Conn, extOptions ExtensionMigrateOptions) (bool, error) {
	_, _, err := extensionVersions(conn, "promscale", version.ExtVersionRange, version.ExtVersionRangeString, extOptions)
	return err == nil, err
}

func fetchAvailableExtensionVersions(conn *pgx.Conn, extName string) (semver.Versions, error) {
	var versionStrings []string
	versions := make(semver.Versions, 0)
	err := conn.QueryRow(context.Background(),
		"SELECT array_agg(version) FROM pg_available_extension_versions WHERE name = $1", extName).Scan(&versionStrings)

	if err != nil {
		return versions, err
	}
	if len(versionStrings) == 0 {
		return versions, nil
	}

	for i := range versionStrings {
		vString := correctVersionString(versionStrings[i], extName)
		// ignore mock ext versions
		ok := strings.HasPrefix(vString, "mock")
		if !ok {
			v, err := semver.Parse(vString)
			if err != nil {
				return versions, fmt.Errorf("Could not parse available extension version %v: %w", vString, err)
			}
			versions = append(versions, v)
		}
	}

	return versions, nil
}

func fetchDefaultExtensionVersions(conn *pgx.Conn, extName string) (semver.Version, error) {
	var versionString string
	err := conn.QueryRow(context.Background(),
		"SELECT default_version FROM pg_available_extensions WHERE name = $1", extName).Scan(&versionString)
	if err != nil {
		return semver.Version{}, err
	}

	versionString = correctVersionString(versionString, extName)
	v, err := semver.Parse(versionString)
	if err != nil {
		return v, fmt.Errorf("Could not parse default extension version %v: %w", versionString, err)
	}

	return v, nil
}

func FetchInstalledExtensionVersion(conn *pgx.Conn, extensionName string) (semver.Version, bool, error) {
	var versionString string
	if err := conn.QueryRow(
		context.Background(),
		"SELECT extversion FROM pg_extension WHERE extname=$1;",
		extensionName,
	).Scan(&versionString); err != nil {
		if err == pgx.ErrNoRows {
			return semver.Version{}, false, nil
		}
		return semver.Version{}, true, err
	}

	versionString = correctVersionString(versionString, extensionName)

	v, err := semver.Parse(versionString)
	if err != nil {
		return v, true, fmt.Errorf("could not parse current %s extension version %v: %w", extensionName, versionString, err)
	}
	return v, true, nil
}

func correctVersionString(v string, extName string) string {
	//we originally published the extension as "0.1" which isn't a valid semver
	if extName == "promscale" && v == "0.1" {
		return "0.1.0"
	}
	return v
}

func getSqlVersion(v semver.Version, extName string) string {
	if extName == "promscale" && v.String() == "0.1.0" {
		return "0.1"
	}
	return v.String()
}

// getNewExtensionVersion returns the highest version allowed by validRange
func getNewExtensionVersion(extName string,
	availableVersions semver.Versions,
	defaultVersion semver.Version,
	validRange semver.Range,
	validCurrentVersion, upgradePreRelease bool,
	currentVersion semver.Version) (semver.Version, bool) {
	//sort higher extensions first
	sort.Sort(sort.Reverse(availableVersions))
	printedWarning := false
	for i := range availableVersions {
		/* skip any versions above the default version for auto-upgrade */
		if availableVersions[i].GT(defaultVersion) {
			continue
		}

		// if upgradePreRelease is false skip the pre-releases.
		// if the current installed version is an rc version return it as an valid available version.
		if len(availableVersions[i].Pre) > 0 && !upgradePreRelease && availableVersions[i].String() != currentVersion.String() {
			log.Warn("msg", "skipping upgrade to prerelease version "+availableVersions[i].String()+", use -startup.upgrade-prerelease-extensions to force.")
			continue
		}

		/* Do not auto-upgrade across Major versions of extensions */
		if validCurrentVersion && currentVersion.Major != availableVersions[i].Major {
			/* Print a warning if there is a a non-prerelease newer major version available */
			if !printedWarning && availableVersions[i].Major > currentVersion.Major && len(availableVersions[i].Pre) == 0 {
				log.Warn("msg", "Newer major version of "+extName+" is available, but has to be upgraded manually with ALTER EXTENSION (we do not upgrade across major versions automatically).",
					"available_version", availableVersions[i].String())
				printedWarning = true
			}
			continue
		}
		if validRange(availableVersions[i]) {
			return availableVersions[i], true
		}
	}
	return semver.Version{}, false
}

// trimLeadingZeros removes the leading zeros passed in the version number.
func trimLeadingZeros(s string) string {
	if s = strings.TrimLeft(s, "0"); s == "" {
		return "0"
	}
	return s
}
