// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package runner

import (
	"fmt"

	"github.com/timescale/promscale/pkg/version"
)

// ParseArgs parses the provided args and prints accordingly. This function should be called before ParseFlags in order
// to process the non-input flags like "-version".
func ParseArgs(args []string) (shouldProceed bool) {
	shouldProceed = true
	for _, flag := range args {
		flag = flag[1:]
		switch flag {
		case "version":
			shouldProceed = false
			fmt.Println(version.Version)
		}
	}
	return
}
