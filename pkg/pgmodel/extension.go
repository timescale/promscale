// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/version"
)

var (
	ExtensionIsInstalled = false
)

// checkVersions is responsible for verifying the version compatibility of installed Postgresql database and extensions.
func checkVersions(conn *pgx.Conn) error {
	if err := checkPgVersion(conn); err != nil {
		return fmt.Errorf("Problem checking PostgreSQL version: %w", err)
	}
	if err := checkExtensionsVersion(conn); err != nil {
		return fmt.Errorf("Problem checking Promescale extension version: %w", err)
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

// checkExtensionsVersion checks for the correct version and enables the extension if
// it is at the right version
func checkExtensionsVersion(conn *pgx.Conn) error {
	if err := checkTimescaleDBVersion(conn); err != nil {
		return err
	}
	if err := checkPromscaleExtensionVersion(conn); err != nil {
		return err
	}
	return nil
}

func checkTimescaleDBVersion(conn *pgx.Conn) error {
	timescaleVersion, isInstalled, err := fetchInstalledExtensionVersion(conn, "timescaledb")
	if err != nil {
		return fmt.Errorf("could not get the installed extension version: %w", err)
	}
	if !isInstalled {
		log.Warn("msg", "Running Promscale without TimescaleDB. Some features will be disabled.")
		return nil
	}
	switch version.VerifyTimescaleVersion(timescaleVersion) {
	case version.Warn:
		safeRanges := strings.Split(version.TimescaleVersionRangeString.Safe, " ")
		log.Warn(
			"msg",
			fmt.Sprintf(
				"Might lead to incompatibility issues due to TimescaleDB version. Expected version within %s to %s.",
				safeRanges[0], safeRanges[1],
			),
			"Installed Timescaledb version:", timescaleVersion.String(),
		)
	case version.Err:
		safeRanges := strings.Split(version.TimescaleVersionRangeString.Safe, " ")
		return fmt.Errorf("incompatible Timescaledb version: %s. Expected version within %s to %s", timescaleVersion.String(), safeRanges[0], safeRanges[1])
	case version.Safe:
	}
	return nil
}

func checkPromscaleExtensionVersion(conn *pgx.Conn) error {
	currentVersion, isInstalled, err := fetchInstalledExtensionVersion(conn, "promscale")
	if err != nil {
		return fmt.Errorf("could not get the installed extension version: %w", err)
	}
	if !isInstalled {
		ExtensionIsInstalled = false
		return nil
	}
	if version.ExtVersionRange(currentVersion) {
		ExtensionIsInstalled = true
	} else {
		ExtensionIsInstalled = false
	}
	return nil
}

func migrateExtension(conn *pgx.Conn, extName string, extSchemaName string, validRange semver.Range, rangeString string) error {
	availableVersions, err := fetchAvailableExtensionVersions(conn, extName)
	if err != nil {
		return err
	}
	if len(availableVersions) == 0 {
		return fmt.Errorf("the extension is not available")
	}

	defaultVersion, err := fetchDefaultExtensionVersions(conn, extName)
	if err != nil {
		return err
	}

	currentVersion, isInstalled, err := fetchInstalledExtensionVersion(conn, extName)
	if err != nil {
		return fmt.Errorf("Could not get the installed extension version: %w", err)
	}

	newVersion, ok := getNewExtensionVersion(extName, availableVersions, defaultVersion, validRange, isInstalled, currentVersion)
	if !ok {
		return fmt.Errorf("the extension is not available at the right version, need version: %v", rangeString)
	}

	if !isInstalled {
		_, extErr := conn.Exec(context.Background(),
			fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s VERSION '%s'",
				extName, extSchemaName, getSqlVersion(newVersion, extName)))
		if extErr != nil {
			return extErr
		}
		return nil
	}

	comparator := currentVersion.Compare(newVersion)
	if comparator > 0 {
		//currentVersion greater than what we can handle, don't use the extension
		return fmt.Errorf("the extension at a greater version than supported by the connector: %v > %v", currentVersion, newVersion)
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
				getSqlVersion(newVersion, extName)))
		if err != nil {
			return err
		}
	}

	return nil
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
		v, err := semver.Parse(vString)
		if err != nil {
			return versions, fmt.Errorf("Could not parse available extension version %v: %w", vString, err)
		}
		versions = append(versions, v)
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

func fetchInstalledExtensionVersion(conn *pgx.Conn, extensionName string) (semver.Version, bool, error) {
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
	validCurrentVersion bool,
	currentVersion semver.Version) (semver.Version, bool) {
	//sort higher extensions first
	sort.Sort(sort.Reverse(availableVersions))
	printedWarning := false
	for i := range availableVersions {
		/* skip any versions above the default version for auto-upgrade */
		if availableVersions[i].GT(defaultVersion) {
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
	return strings.TrimLeft(s, "0")
}
