// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"bytes"
	"os"
	"runtime"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/dekobon/distro-detect/linux"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/version"
)

// BuildPlatform To fill this variable in build time, use linker flags.
// Example: go build -ldflags="-X github.com/timescale/promscale/pkg/telemetry.BuildPlatform=<any_string>" ./cmd/promscale/
var BuildPlatform string

type Metadata map[string]string

func promscaleMetadata() Metadata {
	metadata := Metadata{
		"version":        version.Promscale,
		"commit_hash":    version.CommitHash,
		"arch":           runtime.GOARCH,
		"os":             runtime.GOOS,
		"packager":       getPkgEnv(),
		"build_platform": BuildPlatform,
	}
	uname := unix.Utsname{}
	if err := unix.Uname(&uname); err != nil {
		log.Debug("msg", "error fetching uname", "error", err.Error())
		return metadata
	}
	// We cannot send [65]byte since its [65]byte for linux and [256]byte for darwin,
	// leading to type mismatch. Hence, we create a slice to handle both cases.
	metadata["os_sys_name"] = toString(uname.Sysname[:])
	metadata["os_node_name"] = toString(uname.Nodename[:])
	metadata["os_machine"] = toString(uname.Machine[:])

	turnOffPackageLogging()
	// uname gives off values for os_version, hence we need to use /etc/os-releases
	// to get the expected results.
	distro := linux.DiscoverDistro()
	metadata["os_version"] = distro.Version
	metadata["os_id"] = distro.ID

	return metadata
}

func turnOffPackageLogging() {
	linux.LogWarnf = func(format string, args ...interface{}) {}
	linux.LogErrorf = func(format string, args ...interface{}) {}
}

func getPkgEnv() string {
	pkg := os.Getenv("PROMSCALE_PKG")
	if pkg == "" {
		pkg = "unknown"
	}
	return pkg
}

func toString(prop []byte) string {
	return string(prop[:bytes.IndexByte(prop[:], 0)])
}

const tobsMetadataPrefix = "TOBS_TELEMETRY_"

func tobsMetadata() Metadata {
	env := os.Environ()
	metadata := make(Metadata)
	for _, envVar := range env {
		k, v := decode(envVar)
		if strings.HasPrefix(k, tobsMetadataPrefix) {
			metadata[strings.ToLower(k)] = v // Convert to lower case as metadata table should have everything in lowercase.
		}
	}
	return metadata
}

func decode(s string) (key, value string) {
	arr := strings.SplitN(s, "=", 2)
	return arr[0], arr[1]
}
