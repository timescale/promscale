package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testCompressionSetting = true

func TestNewConfig(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		cfg   Config
		err   string
	}{
		{
			name:  "invalid config yaml",
			input: "invalid",
			err:   "yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `invalid` into dataset.Config",
		},
		{
			name: "invalid duration format 1",
			input: `metrics:
  default_retention_period: d3d`,
			err: `time: invalid duration "d3d"`,
		},
		{
			name: "invalid duration format 2",
			input: `metrics:
  default_retention_period: 3d2h2`,
			err: `time: invalid duration "3d2h2"`,
		},
		{
			name: "invalid duration format 3",
			input: `metrics:
  default_retention_period: 3d2d`,
			err: `time: invalid duration "3d2d"`,
		},
		{
			name: "duration in days and hours",
			input: `metrics:
  default_retention_period: 3d2h`,
			cfg: Config{
				Metrics: Metrics{
					RetentionPeriod: DayDuration(((3 * 24) + 2) * time.Hour),
				},
			},
		},
		{
			name: "happy path",
			input: `metrics:
  default_chunk_interval: 3h
  compress_data: true
  ha_lease_refresh: 2m
  ha_lease_timeout: 5s
  default_retention_period: 30d
traces:
  default_retention_period: 15d`,
			cfg: Config{
				Metrics: Metrics{
					ChunkInterval:   DayDuration(3 * time.Hour),
					Compression:     &testCompressionSetting,
					HALeaseRefresh:  DayDuration(2 * time.Minute),
					HALeaseTimeout:  DayDuration(5 * time.Second),
					RetentionPeriod: DayDuration(30 * 24 * time.Hour),
				},
				Traces: Traces{
					RetentionPeriod: DayDuration(15 * 24 * time.Hour),
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {

			cfg, err := NewConfig(c.input)

			if c.err != "" {
				require.EqualError(t, err, c.err)
				return
			}

			require.Equal(t, cfg, c.cfg)
		})
	}
}

func TestApplyDefaults(t *testing.T) {
	c := Config{}
	c.applyDefaults()

	require.Equal(
		t,
		Config{
			Metrics: Metrics{
				ChunkInterval:   DayDuration(defaultMetricChunkInterval),
				Compression:     &defaultMetricCompressionVar,
				HALeaseRefresh:  DayDuration(defaultMetricHALeaseRefresh),
				HALeaseTimeout:  DayDuration(defaultMetricHALeaseTimeout),
				RetentionPeriod: DayDuration(defaultMetricRetentionPeriod),
			},
			Traces: Traces{
				RetentionPeriod: DayDuration(defaultTraceRetentionPeriod),
			},
		},
		c,
	)

	untouched := Config{
		Metrics: Metrics{
			ChunkInterval:   DayDuration(3 * time.Hour),
			Compression:     &testCompressionSetting,
			HALeaseRefresh:  DayDuration(2 * time.Minute),
			HALeaseTimeout:  DayDuration(5 * time.Second),
			RetentionPeriod: DayDuration(30 * 24 * time.Hour),
		},
		Traces: Traces{
			RetentionPeriod: DayDuration(15 * 24 * time.Hour),
		},
	}

	copyConfig := untouched
	copyConfig.applyDefaults()

	require.Equal(t, untouched, copyConfig)
}
