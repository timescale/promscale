// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rollup

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"

	"github.com/timescale/promscale/pkg/internal/day"
)

// Sync updates the rollups in the DB in accordance with the given resolutions. It handles:
//  1. Creating of new rollups
//  2. Deletion of rollups that have `delete: true`
//  3. Update retention duration of rollups that have same label name but different retention duration. If resolution of
//     existing rollups are updated, an error is returned
func Sync(ctx context.Context, conn *pgx.Conn, r Resolutions) error {
	rows, err := conn.Query(context.Background(), "SELECT name, resolution, retention FROM _prom_catalog.rollup")
	if err != nil {
		return fmt.Errorf("querying existing resolutions: %w", err)
	}
	defer rows.Close()

	existingResolutions := make(Resolutions)
	for rows.Next() {
		var lName string
		var resolution, retention time.Duration
		if err := rows.Scan(&lName, &resolution, &retention); err != nil {
			return fmt.Errorf("error scanning output rows for existing resolutions: %w", err)
		}
		existingResolutions[lName] = Definition{Resolution: day.Duration(resolution), Retention: day.Duration(retention)}
	}

	if err := errOnResolutionMismatch(existingResolutions, r); err != nil {
		return fmt.Errorf("error on existing resolution mismatch: %w", err)
	}

	if err := updateExistingRollups(ctx, conn, existingResolutions, r); err != nil {
		return fmt.Errorf("update existing rollups: %w", err)
	}

	// Delete rollups that are no longer required.
	if err = deleteRollups(ctx, conn, existingResolutions, r); err != nil {
		return fmt.Errorf("delete rollups: %w", err)
	}

	// Create new rollups.
	if err = createRollups(ctx, conn, existingResolutions, r); err != nil {
		return fmt.Errorf("create rollups: %w", err)
	}
	return nil
}

// errOnResolutionMismatch returns an error if a given resolution exists in the DB with a different resolution duration.
func errOnResolutionMismatch(existing, r Resolutions) error {
	for labelName, res := range r {
		if oldRes, exists := existing[labelName]; exists {
			if oldRes.Resolution != res.Resolution {
				return fmt.Errorf("existing rollup resolutions cannot be updated. Either keep the resolution of existing rollup labels same or remove them")
			}
		}
	}
	return nil
}

// updateExistingRollups updates the existing rollups retention if the new resolutions with a same name has
// different retention duration.
func updateExistingRollups(ctx context.Context, conn *pgx.Conn, existingRes, r Resolutions) error {
	var batch pgx.Batch
	for labelName, res := range r {
		if oldRes, exists := existingRes[labelName]; exists && oldRes.Retention != res.Retention {
			batch.Queue("UPDATE _prom_catalog.rollup SET retention = $1 WHERE name = $2", time.Duration(res.Retention), labelName)
		}
	}
	if batch.Len() > 0 {
		results := conn.SendBatch(ctx, &batch)
		if err := results.Close(); err != nil {
			return fmt.Errorf("error closing batch: %w", err)
		}
	}
	return nil
}

func createRollups(ctx context.Context, conn *pgx.Conn, existingRes, r Resolutions) error {
	var batch pgx.Batch
	for lName, res := range r {
		_, exists := existingRes[lName]
		if !exists && !res.Delete {
			batch.Queue("CALL _prom_catalog.create_rollup($1, $2, $3)", lName, time.Duration(res.Resolution), time.Duration(res.Retention))
		}
	}
	if batch.Len() > 0 {
		results := conn.SendBatch(ctx, &batch)
		if err := results.Close(); err != nil {
			return fmt.Errorf("error creating new rollups: %w", err)
		}
	}
	return nil
}

func deleteRollups(ctx context.Context, conn *pgx.Conn, existingRes, r Resolutions) error {
	var batch pgx.Batch
	for lName, res := range r {
		_, exists := existingRes[lName]
		if exists && res.Delete {
			// Delete the rollup only if it exists in the DB.
			batch.Queue("CALL _prom_catalog.delete_rollup($1)", lName)
		}
	}
	if batch.Len() > 0 {
		results := conn.SendBatch(ctx, &batch)
		if err := results.Close(); err != nil {
			return fmt.Errorf("error deleting new rollups: %w", err)
		}
	}
	return nil
}
