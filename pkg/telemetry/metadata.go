// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/timescale/promscale/pkg/version"
)

type (
	Metadata map[string]string
	Stats    map[string]string
)

func promscaleMetadata() (Metadata, error) {
	metadata := Metadata{
		"version":     version.Promscale,
		"commit_hash": version.CommitHash,
		"arch":        runtime.GOARCH,
		"os":          runtime.GOOS,
	}
	uname := unix.Utsname{}
	if err := unix.Uname(&uname); err != nil {
		return metadata, fmt.Errorf("syscall uname: %w", err)
	}
	// We cannot send [65]byte since its [65]byte for linux and [256]byte for darwin,
	// leading to type mismatch. Hence, we create a slice to handle both cases.
	metadata["os_sys_name"] = toString(uname.Sysname[:])
	metadata["os_node_name"] = toString(uname.Nodename[:])
	metadata["os_release"] = toString(uname.Release[:])
	metadata["os_version"] = toString(uname.Version[:])
	metadata["os_machine"] = toString(uname.Machine[:])
	return metadata, nil
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
	arr := strings.Split(s, "=")
	return arr[0], arr[1]
}
