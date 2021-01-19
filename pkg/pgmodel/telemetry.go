package pgmodel

import (
	"context"

	"github.com/jackc/pgx/v4"
)

const (
	metadataUpdateWithExtension = "SELECT update_tsprom_metadata($1, $2, $3)"
	metadataUpdateNoExtension   = "INSERT INTO _timescaledb_catalog.metadata(key, value, include_in_telemetry) VALUES ('promscale_' || $1, $2, $3) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, include_in_telemetry = EXCLUDED.include_in_telemetry"
)

func UpdateTelemetry(db *pgx.Conn, versionInfo VersionInfo, promscaleExtensionInstalled bool) {
	metadataUpdate(db, promscaleExtensionInstalled, "version", versionInfo.Version)
	metadataUpdate(db, promscaleExtensionInstalled, "commit_hash", versionInfo.CommitHash)
}

func metadataUpdate(db *pgx.Conn, promscaleExtensionInstalled bool, key string, value string) {
	/* Ignore error if it doesn't work */
	if promscaleExtensionInstalled {
		_, _ = db.Exec(context.Background(), metadataUpdateWithExtension, key, value, true)
	} else {
		_, _ = db.Exec(context.Background(), metadataUpdateNoExtension, key, value, true)
	}
}
