// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"os"
	"runtime"
	"strings"

	"github.com/timescale/promscale/pkg/version"
)

type (
	Metadata map[string]string
	Stats    map[string]string
)

func promscaleMetadata() Metadata {
	return Metadata{
		"promscale_version":     version.Promscale,
		"promscale_commit_hash": version.CommitHash,
		"promscale_arch":        runtime.GOARCH,
		"promscale_os":          runtime.GOOS,
	}
}

const tobsMetadataPrefix = "tobs_telemetry_"

func tobsMetadata() Metadata {
	env := os.Environ()
	metadata := make(Metadata)
	for _, envVar := range env {
		if strings.HasPrefix(envVar, tobsMetadataPrefix) {
			metadata[envVar] = os.Getenv(envVar)
		}
	}
	return metadata
}
