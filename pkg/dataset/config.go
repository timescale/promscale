// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package dataset

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"gopkg.in/yaml.v2"

	"github.com/timescale/promscale/pkg/internal/day"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/rollup"
)

const (
	defaultMetricChunkInterval   = 8 * time.Hour
	defaultMetricCompression     = true
	defaultMetricHALeaseRefresh  = 10 * time.Second
	defaultMetricHALeaseTimeout  = 1 * time.Minute
	defaultMetricRetentionPeriod = 90 * 24 * time.Hour
	defaultTraceRetentionPeriod  = 30 * 24 * time.Hour
)

var (
	setDefaultMetricChunkIntervalSQL    = "SELECT prom_api.set_default_chunk_interval($1)"
	setDefaultMetricCompressionSQL      = "SELECT prom_api.set_default_compression_setting($1)"
	setDefaultMetricHAReleaseRefreshSQL = `SELECT _prom_catalog.set_default_value('ha_lease_refresh', $1::text)`
	setDefaultMetricHAReleaseTimeoutSQL = `SELECT _prom_catalog.set_default_value('ha_lease_timeout', $1::text)`
	setDefaultMetricRetentionPeriodSQL  = "SELECT prom_api.set_default_retention_period($1)"
	setDefaultTraceRetentionPeriodSQL   = "SELECT ps_trace.set_trace_retention_period($1)"

	defaultMetricCompressionVar = defaultMetricCompression
)

// Config represents a dataset config.
type Config struct {
	Metrics `yaml:"metrics"`
	Traces  `yaml:"traces"`
}

// Metrics contains dataset configuration options for metrics data.
type Metrics struct {
	ChunkInterval   day.Duration   `yaml:"default_chunk_interval"`
	Compression     *bool          `yaml:"compress_data"` // Using pointer to check if the value was set.
	HALeaseRefresh  day.Duration   `yaml:"ha_lease_refresh"`
	HALeaseTimeout  day.Duration   `yaml:"ha_lease_timeout"`
	RetentionPeriod day.Duration   `yaml:"default_retention_period"`
	Rollup          *rollup.Config `yaml:"rollup,omitempty"`
}

// Traces contains dataset configuration options for traces data.
type Traces struct {
	RetentionPeriod day.Duration `yaml:"default_retention_period"`
}

// NewConfig creates a new dataset config based on the configuration YAML contents.
func NewConfig(contents string) (cfg Config, err error) {
	err = yaml.Unmarshal([]byte(contents), &cfg)
	return cfg, err
}

// Apply applies the configuration to the database via the supplied DB connection.
func (c *Config) Apply(ctx context.Context, conn *pgx.Conn) error {
	c.applyDefaults()

	if c.Metrics.Rollup != nil {
		if err := c.Metrics.Rollup.Apply(ctx, conn); err != nil {
			return fmt.Errorf("error applying configuration for downsampling: %w", err)
		}
	}

	log.Info("msg", fmt.Sprintf("Setting metric dataset default chunk interval to %s", c.Metrics.ChunkInterval))
	log.Info("msg", fmt.Sprintf("Setting metric dataset default compression to %t", *c.Metrics.Compression))
	log.Info("msg", fmt.Sprintf("Setting metric dataset default high availability lease refresh to %s", c.Metrics.HALeaseRefresh))
	log.Info("msg", fmt.Sprintf("Setting metric dataset default high availability lease timeout to %s", c.Metrics.HALeaseTimeout))
	log.Info("msg", fmt.Sprintf("Setting metric dataset default retention period to %s", c.Metrics.RetentionPeriod))
	log.Info("msg", fmt.Sprintf("Setting trace dataset default retention period to %s", c.Traces.RetentionPeriod))

	queries := map[string]interface{}{
		setDefaultMetricChunkIntervalSQL:    time.Duration(c.Metrics.ChunkInterval),
		setDefaultMetricCompressionSQL:      c.Metrics.Compression,
		setDefaultMetricHAReleaseRefreshSQL: time.Duration(c.Metrics.HALeaseRefresh),
		setDefaultMetricHAReleaseTimeoutSQL: time.Duration(c.Metrics.HALeaseTimeout),
		setDefaultMetricRetentionPeriodSQL:  time.Duration(c.Metrics.RetentionPeriod),
		setDefaultTraceRetentionPeriodSQL:   time.Duration(c.Traces.RetentionPeriod),
	}

	for sql, param := range queries {
		if _, err := conn.Exec(context.Background(), sql, param); err != nil {
			return err
		}
	}

	return nil
}

func (c *Config) applyDefaults() {
	if c.Metrics.ChunkInterval <= 0 {
		c.Metrics.ChunkInterval = day.Duration(defaultMetricChunkInterval)
	}
	if c.Metrics.Compression == nil {
		c.Metrics.Compression = &defaultMetricCompressionVar
	}
	if c.Metrics.HALeaseRefresh <= 0 {
		c.Metrics.HALeaseRefresh = day.Duration(defaultMetricHALeaseRefresh)
	}
	if c.Metrics.HALeaseTimeout <= 0 {
		c.Metrics.HALeaseTimeout = day.Duration(defaultMetricHALeaseTimeout)
	}
	if c.Metrics.RetentionPeriod <= 0 {
		c.Metrics.RetentionPeriod = day.Duration(defaultMetricRetentionPeriod)
	}
	if c.Traces.RetentionPeriod <= 0 {
		c.Traces.RetentionPeriod = day.Duration(defaultTraceRetentionPeriod)
	}
}
