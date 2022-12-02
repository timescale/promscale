package constants

import (
	"os"
	"strings"
)

var (
	PromscaleExtensionVersion   string
	PromscaleExtensionContainer string
)

const rollupsDBImage = "ghcr.io/timescale/dev_promscale_extension:rollups-development-ts2.8-pg14"

func init() {
	content, err := os.ReadFile("../../../EXTENSION_VERSION")
	if err != nil {
		panic(err)
	}

	PromscaleExtensionVersion = strings.TrimSpace(string(content))
	//PromscaleExtensionContainer = "ghcr.io/timescale/dev_promscale_extension:" + PromscaleExtensionVersion + "-ts2-pg14"
	PromscaleExtensionContainer = rollupsDBImage // This will be removed once we plan to merge with master.
}
