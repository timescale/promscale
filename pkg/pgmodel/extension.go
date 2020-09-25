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
		return fmt.Errorf("timescaledb is not installed: %w", err)
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

func migrateExtension(conn *pgx.Conn) error {
	availableVersions, err := fetchAvailableExtensionVersions(conn)
	ExtensionIsInstalled = false
	if err != nil {
		return err
	}
	if len(availableVersions) == 0 {
		log.Warn("msg", "the promscale extension is not available, proceeding without extension")
		return nil
	}

	newVersion, ok := getNewExtensionVersion(availableVersions)
	if !ok {
		log.Warn("msg", "the promscale extension is not available at the right version, need version: %v, proceeding without extension", version.ExtVersionRangeString)
		return nil
	}

	currentVersion, isInstalled, err := fetchInstalledExtensionVersion(conn, "promscale")
	if err != nil {
		return fmt.Errorf("Could not get the installed extension version: %w", err)
	}

	if !isInstalled {
		_, extErr := conn.Exec(context.Background(),
			fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS promscale WITH SCHEMA %s VERSION '%s'",
				extSchema, getSqlVersion(newVersion)))
		if extErr != nil {
			return extErr
		}
		return checkExtensionsVersion(conn)
	}

	comparator := currentVersion.Compare(newVersion)
	if comparator > 0 {
		//currentVersion greater than what we can handle, don't use the extension
		log.Warn("msg", "the promscale extension at a greater version than supported by the connector: %v > %v, proceeding without extension", currentVersion, newVersion)
	} else if comparator == 0 {
		//Nothing to do we are at the correct version
	} else {
		//Upgrade to the right version
		_, err := conn.Exec(context.Background(),
			fmt.Sprintf("ALTER EXTENSION promscale UPDATE TO '%s'",
				getSqlVersion(newVersion)))
		if err != nil {
			return err
		}
	}

	return checkExtensionsVersion(conn)
}

func fetchAvailableExtensionVersions(conn *pgx.Conn) (semver.Versions, error) {
	var versionStrings []string
	versions := make(semver.Versions, 0)
	err := conn.QueryRow(context.Background(),
		"SELECT array_agg(version) FROM pg_available_extension_versions WHERE name ='promscale'").Scan(&versionStrings)

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

	if extensionName == "promscale" {
		versionString = correctVersionString(versionString)
	}

	v, err := semver.Parse(versionString)
	if err != nil {
		return v, true, fmt.Errorf("could not parse current %s extension version %v: %w", extensionName, versionString, err)
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
