package version

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
	// Example if releasing 0.1.1, the next development cycle is 0.1.2-dev.0
	// Example if releasing 0.1.3-beta the next development cycle is 0.1.3-beta.1.dev.0
	//
	// When introducing a new SQL migration script, you must bump the version of the app
	// since an app version must uniquely determine the state o f the schema.
	// It is customary to bump the version by incrementing the numeral after
	// the `dev` tag. The SQL migration script name must correspond to the /new/ version.
	Version    = "0.1.0-beta.2"
	CommitHash = ""
)
