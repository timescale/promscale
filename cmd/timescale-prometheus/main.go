// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/runner"
	"github.com/timescale/timescale-prometheus/pkg/version"
)

func main() {
	logLevel := flag.String("log-level", "debug", "The log level to use [ \"error\", \"warn\", \"info\", \"debug\" ].")
	cfg := &runner.Config{}
	cfg, err := runner.ParseFlags(cfg)
	if err != nil {
		fmt.Println("Version: ", version.Version, "Commit Hash: ", version.CommitHash)
		fmt.Println("Fatal error: cannot parse flags ", err)
		os.Exit(1)
	}
	err = log.Init(*logLevel)
	if err != nil {
		fmt.Println("Version: ", version.Version, "Commit Hash: ", version.CommitHash)
		fmt.Println("Fatal error: cannot start logger", err)
		os.Exit(1)
	}
	err = runner.Run(cfg)
	if err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}
