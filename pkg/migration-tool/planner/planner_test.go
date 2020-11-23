package planner

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/util/testutil"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

func TestTimeDeltaDetermination(t *testing.T) {
	minute := time.Minute.Milliseconds()
	cases := []struct {
		name               string
		numBytes           int
		prevTimeDelta      int64
		expOutputTimeDelta int64
	}{
		{
			name:               "start_case",
			numBytes:           0,
			prevTimeDelta:      0,
			expOutputTimeDelta: 1 * minute,
		},
		{
			name:               "bytes_less_than_limit",
			numBytes:           utils.Megabyte * 5,
			prevTimeDelta:      11 * minute,
			expOutputTimeDelta: 12 * minute,
		},
		{
			name:               "bytes_equal_than_limit",
			numBytes:           utils.Megabyte * 25,
			prevTimeDelta:      11 * minute,
			expOutputTimeDelta: 11 * minute,
		},
		{
			name:               "bytes_greater_than_limit",
			numBytes:           utils.Megabyte*25 + 1,
			prevTimeDelta:      10 * minute,
			expOutputTimeDelta: 5 * minute,
		},
		{
			name:               "time_less_than_limit",
			numBytes:           utils.Megabyte * 10,
			prevTimeDelta:      11 * minute,
			expOutputTimeDelta: 12 * minute,
		},
		{
			name:               "time_equal_than_limit",
			numBytes:           utils.Megabyte * 10,
			prevTimeDelta:      120 * minute,
			expOutputTimeDelta: 120 * minute,
		},
		{
			name:               "time_greater_than_limit_but_size_in_safe_limits",
			numBytes:           utils.Megabyte * 10,
			prevTimeDelta:      120*minute + 1,
			expOutputTimeDelta: 120 * minute,
		},
		{
			name:               "time_and_bytes_greater_than_respective_limits",
			numBytes:           utils.Megabyte*25 + 1,
			prevTimeDelta:      120*minute + 1,
			expOutputTimeDelta: 60 * minute,
		},
		{
			name:               "time_and_bytes_random_greater_than_respective_limits",
			numBytes:           utils.Megabyte * 300,
			prevTimeDelta:      120 * minute,
			expOutputTimeDelta: 60 * minute,
		},
	}

	for _, c := range cases {
		outTimeDelta := determineTimeDelta(c.numBytes, c.prevTimeDelta)
		testutil.Equals(t, c.expOutputTimeDelta, outTimeDelta, c.name)
	}
}

func TestNumBlockCreation(t *testing.T) {
	cases := []struct {
		name                       string
		startT                     int64
		endT                       int64
		ExpectedNumBlocksCreations int
	}{
		{
			name: "",
		},
	}
}
