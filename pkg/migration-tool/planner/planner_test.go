// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package planner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

var slabSizeLimit int64 = utils.Megabyte * 500

func TestTimeDeltaDetermination(t *testing.T) {
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
			name:               "bytes_less_than_limit_2",
			numBytes:           utils.Megabyte*500 - 1,
			prevTimeDelta:      11 * minute,
			expOutputTimeDelta: 11 * minute,
		},
		{
			name:               "bytes_equal_than_limit",
			numBytes:           utils.Megabyte * 500,
			prevTimeDelta:      11 * minute,
			expOutputTimeDelta: 11 * minute,
		},
		{
			name:               "bytes_greater_than_limit",
			numBytes:           utils.Megabyte*500 + 1,
			prevTimeDelta:      10 * minute,
			expOutputTimeDelta: 5 * minute,
		},
		{
			name:               "bytes_greater_than_limit_inc_region",
			numBytes:           utils.Megabyte*250 + 1,
			prevTimeDelta:      10 * minute,
			expOutputTimeDelta: 10 * minute,
		},
		{
			name:               "bytes_equal_to_limit_inc_region",
			numBytes:           utils.Megabyte * 250,
			prevTimeDelta:      10 * minute,
			expOutputTimeDelta: 11 * minute,
		},
		{
			name:               "bytes_less_than_limit_inc_region",
			numBytes:           utils.Megabyte*250 - 1,
			prevTimeDelta:      10 * minute,
			expOutputTimeDelta: 11 * minute,
		},
		{
			name:               "bytes_less_than_just_limit",
			numBytes:           utils.Megabyte*500 - 1,
			prevTimeDelta:      10 * minute,
			expOutputTimeDelta: 10 * minute,
		},
		{
			name:               "bytes_equal_to_limit",
			numBytes:           utils.Megabyte * 500,
			prevTimeDelta:      10 * minute,
			expOutputTimeDelta: 10 * minute,
		},
		{
			name:               "bytes_greater_than_limit",
			numBytes:           utils.Megabyte*500 + 1,
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
			numBytes:           utils.Megabyte*500 + 1,
			prevTimeDelta:      120*minute + 1,
			expOutputTimeDelta: 60 * minute,
		},
		{
			name:               "time_and_bytes_random_greater_than_respective_limits",
			numBytes:           utils.Megabyte * 3000,
			prevTimeDelta:      200 * minute,
			expOutputTimeDelta: 100 * minute,
		},
	}
	cfg := new(Config)
	cfg.LaIncrement = time.Minute
	cfg.MaxReadDuration = time.Hour * 2
	for _, c := range cases {
		outTimeDelta := cfg.determineTimeDelta(int64(c.numBytes), slabSizeLimit, c.prevTimeDelta)
		assert.Equal(t, c.expOutputTimeDelta, outTimeDelta, c.name)
	}
}

func TestNumSlabCreation(t *testing.T) {
	cases := []struct {
		name                      string
		startT                    int64
		endT                      int64
		bytesIncrement            int64 // When using linear increase method, the amount of size increase remains linear mostly, hence this makes a perfect case to test size increments in real-world.
		fails                     bool
		expectedNumSlabsCreations int
	}{
		{
			name:                      "instant_query",
			startT:                    155 * minute,
			endT:                      155 * minute,
			bytesIncrement:            2 * utils.Megabyte,
			expectedNumSlabsCreations: 1,
			fails:                     true,
		},
		{
			name:                      "instant_query_violate_limit",
			startT:                    155 * minute,
			endT:                      155 * minute,
			bytesIncrement:            slabSizeLimit,
			expectedNumSlabsCreations: 1,
			fails:                     true,
		},
		{
			name:                      "normal_time_range_1",
			startT:                    155 * minute,
			endT:                      165 * minute,
			bytesIncrement:            1 * utils.Megabyte,
			expectedNumSlabsCreations: 4,
		},
		{
			name:                      "normal_time_range_2",
			startT:                    100 * minute,
			endT:                      101 * minute,
			bytesIncrement:            1 * utils.Megabyte,
			expectedNumSlabsCreations: 1,
		},
		{
			name:                      "normal_time_range_3",
			startT:                    100 * minute,
			endT:                      120 * minute,
			bytesIncrement:            1 * utils.Megabyte,
			expectedNumSlabsCreations: 6,
		},
		{
			name:                      "normal_time_range_4",
			startT:                    100 * minute,
			endT:                      125 * minute,
			bytesIncrement:            1 * utils.Megabyte,
			expectedNumSlabsCreations: 7,
		},
		{
			name:                      "normal_time_range_5",
			startT:                    100 * minute,
			endT:                      200 * minute,
			bytesIncrement:            1 * utils.Megabyte,
			expectedNumSlabsCreations: 14,
		},
		{
			name:                      "normal_time_range_6",
			startT:                    100 * minute,
			endT:                      200 * minute,
			bytesIncrement:            2 * utils.Megabyte,
			expectedNumSlabsCreations: 14,
		},
		{
			name:                      "normal_time_range_7",
			startT:                    100 * minute,
			endT:                      2000 * minute,
			bytesIncrement:            2 * utils.Megabyte,
			expectedNumSlabsCreations: 62,
		},
		{
			name:                      "normal_time_range_8",
			startT:                    100 * minute,
			endT:                      20000 * minute,
			bytesIncrement:            2 * utils.Megabyte,
			expectedNumSlabsCreations: 226,
		},
		{
			name:                      "normal_time_range_9",
			startT:                    100 * minute,
			endT:                      200 * minute,
			bytesIncrement:            10 * utils.Megabyte,
			expectedNumSlabsCreations: 14,
		},
		{
			name:                      "normal_time_range_10",
			startT:                    100 * minute,
			endT:                      200 * minute,
			bytesIncrement:            9 * utils.Megabyte,
			expectedNumSlabsCreations: 14,
		},
		{
			name:                      "normal_time_range_10_1",
			startT:                    100 * minute,
			endT:                      110 * minute,
			bytesIncrement:            100 * utils.Megabyte,
			expectedNumSlabsCreations: 5,
		},
		{
			name:                      "normal_time_range_11",
			startT:                    100 * minute,
			endT:                      110 * minute,
			bytesIncrement:            200 * utils.Megabyte,
			expectedNumSlabsCreations: 6,
		},
		{
			name:                      "normal_time_range_11_1",
			startT:                    100 * minute,
			endT:                      110 * minute,
			bytesIncrement:            250 * utils.Megabyte,
			expectedNumSlabsCreations: 6,
		},
		{
			name:                      "start_less_than_end",
			startT:                    100 * minute,
			endT:                      100*minute - 1,
			bytesIncrement:            10 * utils.Megabyte,
			expectedNumSlabsCreations: 122,
			fails:                     true,
		},
	}

	for _, c := range cases {
		planConfig := &Config{
			Mint:               c.startT,
			Maxt:               c.endT,
			ProgressEnabled:    false,
			SlabSizeLimitBytes: slabSizeLimit,
			NumStores:          2,
			LaIncrement:        time.Minute,
			MaxReadDuration:    time.Hour * 2,
		}
		plan, _, err := Init(planConfig)
		if c.fails {
			assert.Error(t, err, c.name)
			continue
		}
		plan.Quiet = true
		assert.NoError(t, err, c.name)
		slabCount := 0
		var bytesPrev int64 = 0
		for plan.ShouldProceed() {
			b, err := plan.NextSlab()
			slabCount++
			// Assume fetching happened here.
			if bytesPrev <= slabSizeLimit/2 {
				bytesPrev += c.bytesIncrement
			}
			b.plan.update(int(bytesPrev))
			if bytesPrev > slabSizeLimit {
				// In ideal condition, the drop in time range will drop the size by the same amount.
				bytesPrev /= 2
			}
			assert.NoError(t, err)
		}
		assert.Equal(t, c.expectedNumSlabsCreations, slabCount, c.name)
	}
}
