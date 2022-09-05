// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package limits

import (
	"flag"
	"io"
	"os"
	"testing"

	"github.com/peterbourgon/ff/v3"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/limits/mem"
)

func TestPercent(t *testing.T) {
	t.Parallel()

	inputs := []struct {
		val     string
		percent uint64
		err     bool
	}{
		{"80%", 80, false},
		{"80% ", 80, false},
		{"80 % ", 80, true},
		{" 30% ", 30, false},
		{" 80j% ", 80, true},
		{"64.2%", 64, true},
	}
	for _, tc := range inputs {
		var tm PercentageAbsoluteBytesFlag
		gotErr := tm.Set(tc.val)
		require.Equal(t, tc.err, gotErr != nil, "input %v got error %v", tc.val, gotErr)
		if gotErr != nil {
			continue
		}
		require.Equal(t, Percentage, tm.kind)
		require.Equal(t, tc.percent, tm.value)
	}
}

func TestBytes(t *testing.T) {
	t.Parallel()

	inputs := []struct {
		val   string
		bytes uint64
		err   bool
	}{
		{"80000", 80000, false},
		{"80000 ", 80000, false},
		{"80 j ", 80, true},
		{" 30000 ", 30000, false},
		{" 80j ", 80, true},
		{"64000.2", 64000, true},
		{"999", 999, true},
		{"1001", 1001, false},
	}
	for _, tc := range inputs {
		var tm PercentageAbsoluteBytesFlag
		gotErr := tm.Set(tc.val)
		require.Equal(t, tc.err, gotErr != nil, "input %v got error %v", tc.val, gotErr)
		if gotErr != nil {
			continue
		}
		require.Equal(t, Absolute, tm.kind)
		require.Equal(t, tc.bytes, tm.value, "input %v", tc.val)
	}
}

func TestString(t *testing.T) {
	t.Parallel()

	var tm PercentageAbsoluteBytesFlag
	gotErr := tm.Set("80% ")
	require.NoError(t, gotErr)
	require.Equal(t, "80%", tm.String())
	gotErr = tm.Set(" 3000 ")
	require.NoError(t, gotErr)
	require.Equal(t, "3000", tm.String())
}

func fullyParse(t *testing.T, args []string, expectError bool) Config {
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	config := &Config{}
	ParseFlags(fs, config)
	err := ff.Parse(fs, args)
	if !expectError {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		return Config{}
	}
	require.NoError(t, Validate(config))
	return *config
}

func TestParse(t *testing.T) {
	config := fullyParse(t, []string{}, false)
	require.Equal(t, PercentageAbsoluteBytesFlag{value: 80, kind: Percentage}, config.targetMemoryFlag)
	require.Equal(t, uint64(float64(mem.SystemMemory())*0.8), config.TargetMemoryBytes)

	config = fullyParse(t, []string{"-cache.memory-target", "60%"}, false)
	require.Equal(t, PercentageAbsoluteBytesFlag{value: 60, kind: Percentage}, config.targetMemoryFlag)
	require.Equal(t, uint64(float64(mem.SystemMemory())*0.6), config.TargetMemoryBytes)

	fullyParse(t, []string{"-cache.memory-target", "60"}, true)

	config = fullyParse(t, []string{"-cache.memory-target", "60000"}, false)
	require.Equal(t, PercentageAbsoluteBytesFlag{value: 60000, kind: Absolute}, config.targetMemoryFlag)
	require.Equal(t, uint64(60000), config.TargetMemoryBytes)
}
