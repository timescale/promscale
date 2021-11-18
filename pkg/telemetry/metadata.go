// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"

	"github.com/timescale/promscale/pkg/version"
)

type (
	Metadata map[string]string
	Stats    map[string]string
)

func promscaleMetadata() (Metadata, error) {
	uname := syscall.Utsname{}
	if err := syscall.Uname(&uname); err != nil {
		return nil, fmt.Errorf("syscall uname: %w", err)
	}
	return Metadata{
		"promscale_version":        version.Promscale,
		"promscale_commit_hash":    version.CommitHash,
		"promscale_arch":           runtime.GOARCH,
		"promscale_os":             runtime.GOOS,
		"promscale_os_sys_name":    toString(uname.Sysname),
		"promscale_os_node_name":   toString(uname.Nodename),
		"promscale_os_release":     toString(uname.Release),
		"promscale_os_version":     toString(uname.Version),
		"promscale_os_machine":     toString(uname.Machine),
		"promscale_os_domain_name": toString(uname.Domainname),
	}, nil
}

func toString(prop [65]int8) string {
	bSlice := make([]byte, 65)
	for i := 0; i < 65; i++ {
		bSlice[i] = byte(prop[i])
	}
	return string(bSlice)
}

const tobsMetadataPrefix = "tobs_telemetry_"

func tobsMetadata() Metadata {
	env := os.Environ()
	metadata := make(Metadata)
	for _, envVar := range env {
		k, v := decode(envVar)
		if strings.HasPrefix(k, tobsMetadataPrefix) {
			metadata[k] = v
		}
	}
	return metadata
}

func decode(s string) (key, value string) {
	arr := strings.Split(s, "=")
	return arr[0], arr[1]
}
