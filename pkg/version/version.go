// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package version

import "github.com/blang/semver/v4"

const (
	Safe = iota
	Warn
	Err
)

var (
	// Rules for Versioning:
	// A release version cannot contain `dev` in its pre-release tag.
	// The next development cycle MUST meet two requirements:
	// 1) The development cycle version must be higher in semver than the release.
	//    Thus you either need to increase the patch or pre-release tag. You
	//    SHOULD always increase the version by the smallest amount possible
	//    (e.g. prefer 0.1.2 to 0.2.0) as you can always increase the version for
	//    a release, but must not decrease it.
	// 2) It must include `dev.#` as last part of pre-release tag. The number
	//    usually represents the number of SQL migrations that happened in the cycle
	//    (see below) and should start at 0.
	//
	// Guidance for increasing versions after release:
	//    - If releasing a version without a pre-release tag, bump the patch and add `-dev.0`
	//      pre-release tag.
	//    - If releasing a version with a pre-release tag, add `.dev.0` to prelease tag. Do not
	//      bump the existing prelease tag.
	// Example if releasing 0.1.1, the next development cycle is 0.1.2-dev.0
	// Example if releasing 0.1.3-beta.1 the next development cycle is 0.1.3-beta.1.dev.0
	//
	// When introducing a new SQL migration script, you must bump the version of the app
	// since an app version must uniquely determine the state of the schema.
	// It is customary to bump the version by incrementing the numeral after
	// the `dev` tag. The SQL migration script name must correspond to the /new/ version.

	Promscale                           = "0.7.2-dev.2"
	PrevReleaseVersion                  = "0.7.1"
	PromMigrator                        = "0.0.3"
	CommitHash                          = ""
	EarliestUpgradeTestVersion          = "0.1.0"
	EarliestUpgradeTestVersionMultinode = "0.1.4" //0.1.4 earliest version that supports tsdb 2.0

	PgVersionNumRange       = ">=12.x <15.x" // Corresponds to range within pg 12.0 to pg 14.99
	pgAcceptedVersionsRange = semver.MustParseRange(PgVersionNumRange)

	TimescaleVersionRangeString = struct {
		Safe, Warn string
	}{
		Safe: ">=1.7.3 <2.99.0",
		Warn: ">=1.7.0 <1.7.3",
	}
	timescaleVersionSafeRange = semver.MustParseRange(TimescaleVersionRangeString.Safe)
	timescaleVersionWarnRange = semver.MustParseRange(TimescaleVersionRangeString.Warn)

	TimescaleVersionRangeFullString = TimescaleVersionRangeString.Safe + " || " + TimescaleVersionRangeString.Warn
	TimescaleVersionRange           = timescaleVersionSafeRange.OR(timescaleVersionWarnRange)

	// ExtVersionRangeString is a range of required promscale extension versions
	// support 0.1.x and 0.3.x
	ExtVersionRangeString = ">=0.1.0 <0.3.99"
	ExtVersionRange       = semver.MustParseRange(ExtVersionRangeString)
)

// VerifyPgVersion verifies the Postgresql version compatibility.
func VerifyPgVersion(version semver.Version) bool {
	return pgAcceptedVersionsRange(version)
}

// VerifyTimescaleVersion verifies version compatibility with Timescaledb.
func VerifyTimescaleVersion(version semver.Version) uint {
	if timescaleVersionSafeRange(version) {
		return Safe
	} else if timescaleVersionWarnRange(version) {
		return Warn
	}
	return Err
}
