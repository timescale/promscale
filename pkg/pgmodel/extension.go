// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"context"
	"fmt"
	"sort"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/version"
)

var (
	ExtensionIsInstalled = false
)

// checkExtensionVersion checks for the correct version and enables the extension if
// it is at the right version
func checkExtensionVersion(conn *pgxpool.Pool) error {
	currentVersion, isInstalled, err := fetchInstalledExtensionVersion(conn)
	if err != nil {
		return fmt.Errorf("Could not get the installed extension version: %w", err)
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

func migrateExtension(conn *pgxpool.Pool) error {
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
		return checkExtensionVersion(conn)
	}

	comparator := currentVersion.Compare(newVersion)
	if comparator > 0 {
		//currentVersion greater than what we can handle, don't use the extension
		log.Warn("msg", "timescale_prometheus_extra at a greater version than supported by the connector: %v > %v, proceeding without extension", currentVersion, newVersion)
	} else if comparator == 0 {
		//Nothing to do we are at the correct version
	} else {
		//Upgrade to the right version
		_, err := conn.Exec(context.Background(),
			fmt.Sprintf("ALTER EXTENSION timescale_prometheus_extra UPDATE TO '%s'",
				getSqlVersion(newVersion)))
		if err != nil {
			return err
		}
	}

	return checkExtensionVersion(conn)
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
