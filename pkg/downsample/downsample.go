// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package downsample

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"

	"github.com/timescale/promscale/pkg/internal/day"
	"github.com/timescale/promscale/pkg/util"
)

const (
	setGlobalDownsamplingStateSQL = "SELECT prom_api.set_global_downsampling_state($1)"
	applyDownsampleConfigSQL      = "SELECT _prom_catalog.apply_downsample_config($1::jsonb)"
	downsamplePrefix              = "ds_"          // Stands for downsample_
	lockID                        = 55985173312278 // Choosen randomly
)

type Config struct {
	Interval  day.Duration `yaml:"interval"`
	Retention day.Duration `yaml:"retention"`
}

func (c Config) Name() string {
	return downsamplePrefix + c.Interval.String()
}

func SetState(ctx context.Context, conn *pgx.Conn, state bool) error {
	_, err := conn.Exec(ctx, setGlobalDownsamplingStateSQL, state)
	return errors.WithMessage(err, "error setting downsampling state")
}

type cfgWithName struct {
	Name      string `json:"schema_name"`
	Interval  string `json:"ds_interval"`
	Retention string `json:"retention"`
}

// Sync the given downsampling configurations with the database.
func Sync(ctx context.Context, conn *pgx.Conn, cfgs []Config) error {
	pgLock, err := util.NewPgAdvisoryLock(lockID, conn.Config().ConnString())
	if err != nil {
		return fmt.Errorf("error getting lock for syncing downsampling config")
	}
	defer pgLock.Close()

	try := func() (bool, error) {
		got, err := pgLock.GetAdvisoryLock() // To prevent failure when multiple Promscale start at the same time.
		return got, errors.WithMessage(err, "error trying pg advisory_lock")
	}

	got, err := try()
	if err != nil {
		return err
	}
	if !got {
		// Wait for sometime and try again. If we still did not get the lock, throw an error.
		time.Sleep(time.Second * 5)
		got, err = try()
		if err != nil {
			return err
		}
		if !got {
			return fmt.Errorf("timeout: unable to take the advisory lock for syncing downsampling state")
		}
	}

	var applyCfgs []cfgWithName
	for i := range cfgs {
		c := cfgs[i]
		applyCfgs = append(applyCfgs, cfgWithName{Name: c.Name(), Interval: c.Interval.String(), Retention: c.Retention.String()})
	}
	if len(applyCfgs) > 0 {
		str, err := json.Marshal(applyCfgs)
		if err != nil {
			return fmt.Errorf("error marshalling configs: %w", err)
		}
		if _, err = conn.Exec(ctx, applyDownsampleConfigSQL, str); err != nil {
			return fmt.Errorf("error applying downsample config: %w", err)
		}
	}
	return nil
}
