// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package util

import (
	"github.com/timescale/timescale-prometheus/pkg/log"
)

func init() {
	err := log.Init("debug")
	if err != nil {
		panic(err)
	}
}
