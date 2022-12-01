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

	"github.com/timescale/promscale/pkg/internal/day"
	"github.com/timescale/promscale/pkg/log"
)

const (
	setDefaultDownsampleStateSQL = "SELECT prom_api.set_automatic_downsample($1)"

	// short and long represent system resolutions.
	short = "short"
	long  = "long"
)

var (
	defaultDownsampleState = false
	useDefaultResolution   = false
	systemResolution       = map[string]Definition{
		short: {
			Resolution: day.Duration(5 * time.Minute),
			Retention:  day.Duration(90 * 24 * time.Hour),
		},
		long: {
			Resolution: day.Duration(time.Hour),
			Retention:  day.Duration(395 * 24 * time.Hour),
		},
	}
)

type Config struct {
	Enabled              *bool `yaml:"enabled,omitempty"`
	UseDefaultResolution *bool `yaml:"use_default_resolution"`
	Resolutions          `yaml:"resolutions,omitempty"`
}

type Definition struct {
	Resolution day.Duration `yaml:"resolution"`
	Retention  day.Duration `yaml:"retention"`
	Delete     bool         `yaml:"delete"`
}

type Resolutions map[string]Definition

func (d *Config) Apply(ctx context.Context, conn *pgx.Conn) error {
	d.applyDefaults()

	if containsSystemResolutions(d.Resolutions) {
		return fmt.Errorf("'short' and 'long' are system resolutions. These cannot be applied as rollup labels")
	}

	log.Info("msg", fmt.Sprintf("Setting automatic metric downsample to %t", *d.Enabled))
	if _, err := conn.Exec(context.Background(), setDefaultDownsampleStateSQL, d.Enabled); err != nil {
		return err
	}

	if *d.Enabled {
		if *d.UseDefaultResolution {
			d.Resolutions["short"] = systemResolution["short"]
			d.Resolutions["long"] = systemResolution["long"]
		}
		if err := Sync(ctx, conn, d.Resolutions); err != nil {
			return fmt.Errorf("ensure rollup with: %w", err)
		}
	}
	return nil
}

func (d *Config) applyDefaults() {
	if d.Enabled == nil {
		d.Enabled = &defaultDownsampleState
	}
	if d.UseDefaultResolution == nil {
		d.UseDefaultResolution = &useDefaultResolution
	}
}

func containsSystemResolutions(r Resolutions) bool {
	for k := range r {
		k = strings.ToLower(k)
		if k == short || k == long {
			return true
		}
	}
	return false
}
