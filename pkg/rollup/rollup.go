// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rollup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/util"
)

type DownsampleResolution struct {
	label      string
	resolution util.DayDuration
	retention  util.DayDuration
}

// Parse parses the rollup resolution string and returns true if it respects the format of downsampling resolution,
// which is a string of comma separated label:resolution:retention.
// Example: short:5m:90d,long:1h:395d
func Parse(resolutionStr string) ([]DownsampleResolution, error) {
	resolutions := strings.Split(resolutionStr, ",")
	if len(resolutions) < 1 {
		return nil, fmt.Errorf("resolutions cannot be less than 1")
	}
	var r []DownsampleResolution
	for _, resolution := range resolutions {
		resolution = strings.TrimSpace(resolution)
		if resolution == "" {
			continue
		}

		items := strings.Split(resolution, ":")
		if len(items) != 3 {
			return nil, fmt.Errorf("expected items not found: needed in format of label:resolution:retention but found %v for %s resolution", strings.Join(items, ":"), resolution)
		}
		lName := items[0]

		var res util.DayDuration
		err := res.UnmarshalText([]byte(items[1]))
		if err != nil {
			return nil, fmt.Errorf("error parsing resolution %s: %w", items[1], err)
		}

		var retention util.DayDuration
		err = retention.UnmarshalText([]byte(items[2]))
		if err != nil {
			return nil, fmt.Errorf("error parsing retention %s: %w", items[2], err)
		}
		r = append(r, DownsampleResolution{lName, res, retention})
	}
	return r, nil
}

// EnsureRollupWith ensures "strictly" that the given new resolutions are applied in the database.
// Note: It follows a "strict" behaviour, meaning any existing resolutions of downsampling in
// the database will be removed, so that the all downsampling data in the database strictly
// matches the provided newResolutions.
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
		existingResolutions = append(existingResolutions, DownsampleResolution{label: lName, resolution: util.DayDuration(resolution), retention: util.DayDuration(retention)})
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
		_, err := conn.Exec(context.Background(), "CALL _prom_catalog.create_metric_rollup($1, $2, $3)", r.label, time.Duration(r.resolution), time.Duration(r.retention))
		if err != nil {
			return fmt.Errorf("error creating rollup for %s: %w", r.label, err)
		}
	}
	return nil
}

func deleteRollups(conn *pgx.Conn, res []DownsampleResolution) error {
	for _, r := range res {
		_, err := conn.Exec(context.Background(), "CALL _prom_catalog.delete_metric_rollup($1)", r.label)
		if err != nil {
			return fmt.Errorf("error deleting rollup for %s: %w", r.label, err)
		}
	}
	return nil
}

// diff returns the elements of a that are not in b.
func diff(a, b []DownsampleResolution) []DownsampleResolution {
	var difference []DownsampleResolution
	for i := range a {
		found := false
		for j := range b {
			if a[i].label == b[j].label {
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
