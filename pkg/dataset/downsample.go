// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package dataset

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/rollup"
)

const (
	defaultDownsampleState      = true
	defaultDownsampleResolution = "short:5m:90d,long:1h:395d" // label:resolution:retention
)

var (
	setDefaultDownsampleStateSQL      = "SELECT prom_api.set_automatic_downsample($1)"
	setDefaultDownsampleResolutionSQL = "SELECT prom_api.set_downsample_resolution($1)"

	defaultDownsampleStateVar = defaultDownsampleState
)

type Downsample struct {
	Automatic  *bool  `yaml:"automatic,omitempty"`
	Resolution string `yaml:"resolution,omitempty"`
}

func (d *Downsample) Apply(conn *pgx.Conn) error {
	d.applyDefaults()

	queries := map[string]interface{}{
		setDefaultDownsampleStateSQL: d.Automatic,
	}
	if *d.Automatic {
		if err := handleDownsampling(conn, d.Resolution); err != nil {
			return fmt.Errorf("handle downsample: %w", err)
		}
		queries[setDefaultDownsampleResolutionSQL] = d.Resolution
		log.Info("msg", fmt.Sprintf("Setting metric downsample resolution to %s", d.Resolution))
	}
	log.Info("msg", fmt.Sprintf("Setting metric automatic downsample to %t", *d.Automatic))

	for sql, param := range queries {
		if _, err := conn.Exec(context.Background(), sql, param); err != nil {
			return err
		}
	}
	return nil
}

func (d *Downsample) applyDefaults() {
	if d.Automatic == nil {
		// In default case, we plan to downsample data.
		d.Automatic = &defaultDownsampleStateVar
	}
	if d.Resolution == "" {
		d.Resolution = defaultDownsampleResolution
	}
}

func handleDownsampling(conn *pgx.Conn, resolutionStr string) error {
	downsampleResolutions, err := rollup.Parse(resolutionStr)
	if err != nil {
		return fmt.Errorf("error parsing downsample resolution: %w", err)
	}
	if err = rollup.EnsureRollupWith(conn, downsampleResolutions); err != nil {
		return fmt.Errorf("ensure rollup with: %w", err)
	}
	return nil
}
