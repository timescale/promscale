// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import "sync"

type reg struct {
	r sync.Map
}

func (r *reg) Update(telemetryName, value string) {
	r.r.Store(telemetryName, value)
}

func (r *reg) metadata() (m Metadata) {
	m = Metadata{}
	r.r.Range(func(telemetryName, value interface{}) bool {
		m[telemetryName.(string)] = value.(string)
		return true
	})
	return m
}

// Registry is a telemetry holder that is mutable and can be filled from anywhere in Promscale.
var Registry reg
