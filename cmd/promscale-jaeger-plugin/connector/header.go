package connector

import (
	"github.com/timescale/promscale/pkg/version"
	"net/http"
)

const pluginVersion = "promscale-jaeger-plugin-version"

func applyValidityHeaders(req *http.Request) {
	req.Header.Add(pluginVersion, version.JaegerPluginVersion)
}
