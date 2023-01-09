package constants

import (
	"os"
	"strings"
)

var (
	PromscaleExtensionVersion   string
	PromscaleExtensionContainer string
)

func init() {
	content, err := os.ReadFile("../../../EXTENSION_VERSION")
	if err != nil {
		panic(err)
	}

	PromscaleExtensionVersion = strings.TrimSpace(string(content))
	PromscaleExtensionContainer = "ghcr.io/timescale/dev_promscale_extension:" + PromscaleExtensionVersion + "-ts2-pg15"
}
