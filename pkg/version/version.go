// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package version

import (
	"fmt"

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
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

	Promscale                  = "0.18.0-dev"
	PrevReleaseVersion         = "0.17.0"
	CommitHash                 = ""      // Comes from -ldflags settings
	Branch                     = ""      // Comes from -ldflags settings
	EarliestUpgradeTestVersion = "0.3.0" // 0.3.0 earliest version an image with correct extension versions exists

	PgVersionNumRange       = ">=12.x <16.x" // Corresponds to range within pg 12.0 to pg 15.99
	pgAcceptedVersionsRange = semver.MustParseRange(PgVersionNumRange)

	TimescaleVersionRangeString = ">=2.6.1 <2.99.0"
	TimescaleVersionRange       = semver.MustParseRange(TimescaleVersionRangeString)

	// ExtVersionRangeString is a range of required promscale extension versions
	ExtVersionRangeString = ">=0.8.0 <0.8.99"
	ExtVersionRange       = semver.MustParseRange(ExtVersionRangeString)

	// Expose build info through Prometheus metric
	buildInfoGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:      "build_info",
		Namespace: util.PromNamespace,
		Help:      "Shows Promscale build information like version, branch and commit",
		ConstLabels: prometheus.Labels{
			"version": Promscale,
			"commit":  CommitHash,
			"branch":  Branch,
		},
	})
)

// VerifyPgVersion verifies the Postgresql version compatibility.
func VerifyPgVersion(version semver.Version) bool {
	return pgAcceptedVersionsRange(version)
}

// VerifyTimescaleVersion verifies version compatibility with Timescaledb.
func VerifyTimescaleVersion(version semver.Version) bool {
	return TimescaleVersionRange(version)
}

func init() {
	prometheus.MustRegister(buildInfoGauge)
	buildInfoGauge.Set(1)
}

func Info() string {
	return fmt.Sprintf("Version: %s, Commit Hash: %s, Branch: %s", Promscale, CommitHash, Branch)
}
