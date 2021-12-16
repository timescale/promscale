// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package cache

import (
	"flag"
	"io/ioutil"
	"os"
	"testing"

	"github.com/peterbourgon/ff/v3"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/limits"
)

func fullyParse(t *testing.T, args []string, lcfg *limits.Config, expectError bool) Config {
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	config := &Config{}
	ParseFlags(fs, config)
	err := ff.Parse(fs, args)
	if !expectError {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		return Config{}
	}
	require.NoError(t, Validate(config, *lcfg))
	return *config
}

func TestParse(t *testing.T) {
	config := fullyParse(t, []string{}, &limits.Config{TargetMemoryBytes: 100000}, false)
	require.Equal(t, uint64(50000), config.SeriesCacheMemoryMaxBytes)

	config = fullyParse(t, []string{"-metrics.cache.series.max-bytes", "60%"}, &limits.Config{TargetMemoryBytes: 200000}, false)
	require.Equal(t, uint64(120000), config.SeriesCacheMemoryMaxBytes)

	fullyParse(t, []string{"-metrics.cache.series.max-bytes", "60"}, &limits.Config{TargetMemoryBytes: 100000}, true)

	config = fullyParse(t, []string{"-metrics.cache.series.max-bytes", "60000"}, &limits.Config{TargetMemoryBytes: 200000}, false)
	require.Equal(t, uint64(60000), config.SeriesCacheMemoryMaxBytes)
}
