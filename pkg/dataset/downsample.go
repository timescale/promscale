// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package dataset

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/rollup"
	"github.com/timescale/promscale/pkg/util"
)

const defaultDownsampleState = true

var (
	setDefaultDownsampleStateSQL = "SELECT prom_api.set_automatic_downsample($1)"

	defaultDownsampleStateVar   = defaultDownsampleState
	defaultDownsampleResolution = []rollup.DownsampleResolution{
		{
			Label:      "short",
			Resolution: util.DayDuration(5 * time.Minute),
			Retention:  util.DayDuration(90 * 24 * time.Hour),
		},
		{
			Label:      "long",
			Resolution: util.DayDuration(time.Hour),
			Retention:  util.DayDuration(395 * 24 * time.Hour),
		},
	}
)

type Downsample struct {
	Automatic  *bool                         `yaml:"automatic,omitempty"`
	Resolution []rollup.DownsampleResolution `yaml:"resolutions,omitempty"`
}

func (d *Downsample) Apply(conn *pgx.Conn) error {
	d.applyDefaults()

	log.Info("msg", fmt.Sprintf("Setting metric automatic downsample to %t", *d.Automatic))
	if _, err := conn.Exec(context.Background(), setDefaultDownsampleStateSQL, d.Automatic); err != nil {
		return err
	}

	if *d.Automatic {
		if err := rollup.EnsureRollupWith(conn, d.Resolution); err != nil {
			return fmt.Errorf("ensure rollup with: %w", err)
		}
	}
	return nil
}

func (d *Downsample) applyDefaults() {
	if d.Automatic == nil {
		// In default case, we plan to downsample data.
		d.Automatic = &defaultDownsampleStateVar
	}
	if d.Resolution == nil {
		d.Resolution = defaultDownsampleResolution
	}
}
