// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package downsample

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"

	"github.com/timescale/promscale/pkg/internal/day"
)

const (
	setDownsamplingStateSQL       = "SELECT prom_api.set_downsampling_state($1)"
	createDownsamplingSQL         = "CALL _prom_catalog.create_downsampling($1, $2, $3)"
	updateDownsamplingStateForSQL = "SELECT _prom_catalog.update_downsampling_state($1, $2)"
	downsamplePrefix              = "ds_" // Stands of downsample_
)

type Config struct {
	Interval  day.Duration `yaml:"interval"`
	Retention day.Duration `yaml:"retention"`
	disabled  bool
}

func (c Config) Name() string {
	return downsamplePrefix + c.Interval.Text()
}

func SetState(ctx context.Context, conn *pgx.Conn, state bool) error {
	_, err := conn.Exec(ctx, setDownsamplingStateSQL, state)
	if err != nil {
		return fmt.Errorf("error setting downsampling state: %w", err)
	}
	return nil
}

// Sync updates the downsampling cfgs in the DB in accordance with the given new cfgs. It:
//  1. Creates of new downsampling cfgs that are not in the database
//  2. Updates retention duration of downsampling cfgs that are present in the database but with a different retention duration
//  3. Disables refreshing of downsampling cfgs in the database that were not found in the new cfgs
//  4. Enables refreshing of downsampling cfgs in the database that are in the new cfgs but were previously disabled
func Sync(ctx context.Context, conn *pgx.Conn, cfgs []Config) error {
	newCfgs := make(map[string]Config) // These are the new downsampling cfgs that the user provided. Relation => schema_name: definition{}
	for _, c := range cfgs {
		newCfgs[c.Name()] = Config{Interval: c.Interval, Retention: c.Retention}
	}

	rows, err := conn.Query(ctx, "SELECT schema_name, resolution, retention, should_refresh FROM _prom_catalog.downsample")
	if err != nil {
		return fmt.Errorf("querying existing resolutions: %w", err)
	}
	defer rows.Close()

	existingCfgs := make(map[string]Config) // These are the existing downsampling cfgs in the database.
	for rows.Next() {
		var (
			schemaName          string
			shouldRefresh       bool
			interval, retention time.Duration
		)
		if err := rows.Scan(&schemaName, &interval, &retention, &shouldRefresh); err != nil {
			return fmt.Errorf("error scanning output rows for existing resolutions: %w", err)
		}
		existingCfgs[schemaName] = Config{Interval: day.Duration{T: interval}, Retention: day.Duration{T: retention}, disabled: !shouldRefresh}
	}

	// Update cfgs that have a different retention duration than the new cfgs.
	update := make(map[string]Config)
	for name, newDef := range newCfgs {
		existingDef, found := existingCfgs[name]
		if found && newDef.Retention.Duration() != existingDef.Retention.Duration() {
			update[name] = newDef
		}
	}
	if len(update) > 0 {
		if err = updateRetention(ctx, conn, update); err != nil {
			return fmt.Errorf("updating retention of downsampling cfg: %w", err)
		}
	}

	// Enable downsampling cfgs that were previously disabled.
	disabled := []string{}
	if err := conn.QueryRow(ctx, "SELECT array_agg(schema_name) FROM _prom_catalog.downsample WHERE NOT should_refresh").Scan(&disabled); err != nil {
		return fmt.Errorf("error fetching downsampling configs that are disabled: %w", err)
	}
	disabledDownsampleConfig := map[string]struct{}{}
	for _, n := range disabled {
		disabledDownsampleConfig[n] = struct{}{}
	}
	enable := []string{}
	for name := range newCfgs {
		if _, found := disabledDownsampleConfig[name]; found {
			enable = append(enable, name)
		}
	}
	if len(enable) > 0 {
		if err = updateState(ctx, conn, enable, true); err != nil {
			return fmt.Errorf("error enabling downsampling cfgs: %w", err)
		}
	}

	// Disable downsampling cfgs that are applied in the database but are not present in the new downsampling cfgs.
	disable := []string{}
	for existingName := range existingCfgs {
		if _, found := newCfgs[existingName]; !found {
			disable = append(disable, existingName)
		}
	}
	if len(disable) > 0 {
		if err = updateState(ctx, conn, disable, false); err != nil {
			return fmt.Errorf("error disabling downsampling cfgs: %w", err)
		}
	}

	// Create new downsampling cfgs that are not present in the database.
	createDownsamplingCfgs := make(map[string]Config)
	for newName, newDef := range newCfgs {
		if _, found := existingCfgs[newName]; !found {
			createDownsamplingCfgs[newName] = newDef
		}
	}
	if len(createDownsamplingCfgs) > 0 {
		if err = create(ctx, conn, createDownsamplingCfgs); err != nil {
			return fmt.Errorf("creating new downsampling configurations: %w", err)
		}
	}

	return nil
}

// updateRetention of existing downsampled cfgs.
func updateRetention(ctx context.Context, conn *pgx.Conn, cfgs map[string]Config) error {
	var batch pgx.Batch
	for schemaName, def := range cfgs {
		batch.Queue("UPDATE _prom_catalog.downsample SET retention = $1 WHERE schema_name = $2", def.Retention.Duration(), schemaName)
	}
	if batch.Len() > 0 {
		results := conn.SendBatch(ctx, &batch)
		if err := results.Close(); err != nil {
			return fmt.Errorf("error closing batch: %w", err)
		}
	}
	return nil
}

func create(ctx context.Context, conn *pgx.Conn, cfgs map[string]Config) error {
	var batch pgx.Batch
	for lName, def := range cfgs {
		batch.Queue(createDownsamplingSQL, lName, def.Interval.Duration(), def.Retention.Duration())
	}
	results := conn.SendBatch(ctx, &batch)
	if err := results.Close(); err != nil {
		return fmt.Errorf("error closing batch: %w", err)
	}
	return nil
}

// updateState enables or disables a particular downsampling cfg based on new state.
func updateState(ctx context.Context, conn *pgx.Conn, name []string, newState bool) error {
	var batch pgx.Batch
	for _, n := range name {
		batch.Queue(updateDownsamplingStateForSQL, n, newState)
	}
	results := conn.SendBatch(ctx, &batch)
	if err := results.Close(); err != nil {
		return fmt.Errorf("error closing batch: %w", err)
	}
	return nil
}
