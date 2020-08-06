package main

var (
	// Rules for Versioning:
	// A release version cannot contains `dev` in it's pre-release tag.
	// The next development cycle MUST meet two requirements:
	// 1) The development cycle version must be higher in semver than the release.
	//    Thus you either need to increase the patch or pre-release tag. You
	//    SHOULD always increase the version by the smallest amount possible
	//    (e.g. prefer 0.1.2 to 0.2.0)
	// 2) It must include `dev` as last part of prelease.
	//
	// Example if releasing 0.1.1, the next development cycle is 0.1.2-dev
	// Example if releasing 0.1.3-beta the next development cycle is 0.1.3-beta.1.dev
	//
	// When introducing a new SQL migration script always increment the numeral after
	// the `dev` tag. Add the SQL file corresponding to the /new/ version.
	Version    = "0.1.0-beta.2-dev.0"
	CommitHash = ""
)
