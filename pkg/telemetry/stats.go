// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

// Whats the difference b/t stats and telemetry?
//
// Stats are reported by all Promscales and written into the promscale_instance_connection table.
//
// Telemetry is the aggregates that housekeeper performs, the final processing before writing
// into the _timescaledb_catalog.metadata table.

var telemetries = []telemetry{
	telemetrySQL{
		stat: "promscale_samples_count_last_hour",
		sql:  "SELECT sum(telemetry_ingested_samples) FROM _ps_catalog.promscale_connection_information",
	},
}
