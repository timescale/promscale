package plugin

import (
	"net/http"

	"github.com/timescale/promscale/pkg/version"
)

const pluginVersion = "promscale-jaeger-plugin-version"

func applyValidityHeaders(req *http.Request) {
	req.Header.Add(pluginVersion, version.JaegerPluginVersion)
}
