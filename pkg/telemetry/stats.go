// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

var stats = []telemetry{
	telemetrySQL{
		stat: "promscale_samples_count_last_hour",
		sql:  "SELECT sum(telemetry_ingested_samples) FROM _ps_catalog.promscale_connection_information",
	},
}
