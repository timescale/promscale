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
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/util"
)

const (
	setDownsamplingStateSQL       = "SELECT prom_api.set_downsampling_state($1)"
	createOrUpdateDownsamplingSQL = "CALL _prom_catalog.create_or_update_downsampling($1, $2, $3)"
	updateDownsamplingStateForSQL = "SELECT _prom_catalog.update_downsampling_state($1, $2)"
	downsamplePrefix              = "ds_"          // Stands of downsample_
	lockID                        = 55851985173278 // Choosen randomly
)

type Config struct {
	Interval      day.Duration `yaml:"interval"`
	Retention     day.Duration `yaml:"retention"`
	shouldRefresh bool
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
//  3. Enables refreshing of downsampling cfgs in the database that are in the new cfgs but were previously disabled
//  4. Disables refreshing of downsampling cfgs in the database that were not found in the new cfgs
func Sync(ctx context.Context, conn *pgx.Conn, cfgs []Config) error {
	pgLock, err := util.NewPgAdvisoryLock(lockID, conn.Config().ConnString())
	if err != nil {
		return fmt.Errorf("error getting lock for syncing downsampling config")
	}
	defer pgLock.Close()
	got, err := pgLock.GetAdvisoryLock() // To prevent failure when multiple Promscale start at the same time.
	if err != nil {
		return fmt.Errorf("error trying pg advisory_lock")
	}
	if !got {
		// Some other Promscale instance is already working on the downsampling.Sync()
		// Hence, we should skip.
		return nil
	}
	defer func() {
		if _, err = pgLock.Unlock(); err != nil {
			log.Error("msg", "error unlocking downsampling.Sync advisory_lock", "err", err.Error())
		}
	}()

	newCfgs := make(map[string]Config) // These are the new downsampling cfgs that the user provided. Relation => schema_name: definition{}
	for _, c := range cfgs {
		newCfgs[c.Name()] = Config{Interval: c.Interval, Retention: c.Retention}
	}

	existingCfgs, err := getExistingCfgs(ctx, conn)
	if err != nil {
		return fmt.Errorf("error fetching existing downsampling cfgs: %w", err)
	}

	createOrUpdate := make(map[string]Config)
	for newLabel, newCfg := range newCfgs {
		if existingCfg, found := existingCfgs[newLabel]; found {
			if !existingCfg.shouldRefresh || existingCfg.Retention.Duration() != newCfg.Retention.Duration() {
				createOrUpdate[newLabel] = newCfg
			}
			if existingCfg.Interval.Duration() != newCfg.Interval.Duration() {
				// This should never be the case since newlabel is schema specific. But we still check for safety purpose.
				return fmt.Errorf("interval mismatch: existing interval %v, new interval %v", existingCfg.Interval.Duration(), newCfg.Interval.Duration())
			}
		} else {
			createOrUpdate[newLabel] = newCfg
		}
	}

	if len(createOrUpdate) > 0 {
		if err = createOrUpdateDownsampling(ctx, conn, createOrUpdate); err != nil {
			return fmt.Errorf("error creating or updating given downsampling configs: %w", err)
		}
	}

	if err = disable(ctx, conn, newCfgs, existingCfgs); err != nil {
		return fmt.Errorf("error disabling downsampling configs: %w", err)
	}
	return nil
}

func getExistingCfgs(ctx context.Context, conn *pgx.Conn) (map[string]Config, error) {
	rows, err := conn.Query(ctx, "SELECT schema_name, resolution, retention, should_refresh FROM _prom_catalog.downsample")
	if err != nil {
		return nil, fmt.Errorf("querying existing resolutions: %w", err)
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
			return nil, fmt.Errorf("error scanning output rows for existing resolutions: %w", err)
		}
		existingCfgs[schemaName] = Config{Interval: day.Duration{T: interval}, Retention: day.Duration{T: retention}, shouldRefresh: shouldRefresh}
	}
	return existingCfgs, nil
}

// createOrUpdateDownsampling does 3 things:
// 1. It creates new downsampling configurations that are given in 'cfgs'
// 2. It updates the retention of a downsampling configuration if it is present in the database with the same lName
// 3. It enables a downsampling if it was previously disabled
// Refer to _prom_catalog.create_or_update_downsampling($1, $2, $3) to learn more.
func createOrUpdateDownsampling(ctx context.Context, conn *pgx.Conn, cfgs map[string]Config) error {
	var batch pgx.Batch
	for lName, def := range cfgs {
		batch.Queue(createOrUpdateDownsamplingSQL, lName, def.Interval.Duration(), def.Retention.Duration())
	}
	results := conn.SendBatch(ctx, &batch)
	if err := results.Close(); err != nil {
		return fmt.Errorf("error closing batch: %w", err)
	}
	return nil
}

// disable downsampling cfgs.
func disable(ctx context.Context, conn *pgx.Conn, newCfgs, existingCfgs map[string]Config) error {
	disable := []string{}
	for existingName := range existingCfgs {
		if _, found := newCfgs[existingName]; !found {
			disable = append(disable, existingName)
		}
	}
	if len(disable) == 0 {
		return nil
	}

	var batch pgx.Batch
	for _, n := range disable {
		batch.Queue(updateDownsamplingStateForSQL, n, false)
	}
	results := conn.SendBatch(ctx, &batch)
	if err := results.Close(); err != nil {
		return fmt.Errorf("error closing batch: %w", err)
	}
	return nil
}
