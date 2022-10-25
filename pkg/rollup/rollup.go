// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rollup

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"

	"github.com/timescale/promscale/pkg/util"
)

type DownsampleResolution struct {
	Label      string           `yaml:"label"`
	Resolution util.DayDuration `yaml:"resolution"`
	Retention  util.DayDuration `yaml:"retention"`
}

// EnsureRollupWith ensures "strictly" that the given new resolutions are applied in the database.
//
// Note: It follows a "strict" behaviour, meaning any existing resolutions of downsampling in
// the database will be removed, so that the all downsampling data in the database strictly
// matches the provided newResolutions.
//
// Example: If the DB already contains metric rollups for `short` and `long`, and in dataset-config,
// connector sees `very_short` and `long`, then EnsureRollupWith will remove the `short` downsampled data
// and create `very_short`, while not touching `long`.
func EnsureRollupWith(conn *pgx.Conn, newResolutions []DownsampleResolution) error {
	rows, err := conn.Query(context.Background(), "SELECT name, resolution, retention FROM _prom_catalog.rollup")
	if err != nil {
		return fmt.Errorf("querying existing resolutions: %w", err)
	}
	defer rows.Close()

	var existingResolutions []DownsampleResolution
	for rows.Next() {
		var lName string
		var resolution, retention time.Duration
		if err := rows.Scan(&lName, &resolution, &retention); err != nil {
			return fmt.Errorf("error scanning output rows for existing resolutions: %w", err)
		}
		existingResolutions = append(existingResolutions, DownsampleResolution{Label: lName, Resolution: util.DayDuration(resolution), Retention: util.DayDuration(retention)})
	}

	// Determine which resolutions need to be created and deleted from the DB.
	pendingCreation := diff(newResolutions, existingResolutions)
	pendingDeletion := diff(existingResolutions, newResolutions)

	// Delete rollups that are no longer required.
	if err = deleteRollups(conn, pendingDeletion); err != nil {
		return fmt.Errorf("delete rollups: %w", err)
	}

	// Create new rollups.
	if err = createRollups(conn, pendingCreation); err != nil {
		return fmt.Errorf("create rollups: %w", err)
	}
	return nil
}

func createRollups(conn *pgx.Conn, res []DownsampleResolution) error {
	for _, r := range res {
		_, err := conn.Exec(context.Background(), "CALL _prom_catalog.create_rollup($1, $2, $3)", r.Label, time.Duration(r.Resolution), time.Duration(r.Retention))
		if err != nil {
			return fmt.Errorf("error creating rollup for %s: %w", r.Label, err)
		}
	}
	return nil
}

func deleteRollups(conn *pgx.Conn, res []DownsampleResolution) error {
	for _, r := range res {
		_, err := conn.Exec(context.Background(), "CALL _prom_catalog.delete_rollup($1)", r.Label)
		if err != nil {
			return fmt.Errorf("error deleting rollup for %s: %w", r.Label, err)
		}
	}
	return nil
}

// diff returns the elements of a that are not in b.
//
// We need this since we want to support a "strict" behaviour in downsampling. This basically means, to have the exact
// downsampling data in the DB based on what's mentioned in the dataset-config.
//
// See the comment for EnsureRollupWith for example.
func diff(a, b []DownsampleResolution) []DownsampleResolution {
	var difference []DownsampleResolution
	for i := range a {
		found := false
		for j := range b {
			if a[i].Label == b[j].Label {
				found = true
				break
			}
		}
		if !found {
			difference = append(difference, a[i])
		}
	}
	return difference
}
