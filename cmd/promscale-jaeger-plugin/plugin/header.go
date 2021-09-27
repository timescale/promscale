// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package plugin

import (
	"net/http"

	"github.com/timescale/promscale/pkg/version"
)

const pluginVersion = "promscale-jaeger-plugin-version"

func applyValidityHeaders(req *http.Request) {
	req.Header.Add(pluginVersion, version.JaegerPluginVersion)
}
