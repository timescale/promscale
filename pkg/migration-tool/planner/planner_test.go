package planner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

var minute = time.Minute.Milliseconds()

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
			expOutputTimeDelta: 12 * minute,
		},
		{
			name:               "bytes_equal_than_limit",
			numBytes:           utils.Megabyte * 500,
			prevTimeDelta:      11 * minute,
			expOutputTimeDelta: 11 * minute,
		},
		{
			name:               "bytes_greater_than_limit_but_within_permittable_bounds",
			numBytes:           utils.Megabyte*500 + 1,
			prevTimeDelta:      10 * minute,
			expOutputTimeDelta: 10 * minute,
		},
		{
			name:               "bytes_greater_than_limit_but_within_permittable_bounds_2",
			numBytes:           utils.Megabyte*525 - 1, // 5% of total is the permittable bounds limit.
			prevTimeDelta:      10 * minute,
			expOutputTimeDelta: 10 * minute,
		},
		{
			name:               "bytes_greater_than_limit_but_equal_to_permittable_bounds",
			numBytes:           utils.Megabyte * 525,
			prevTimeDelta:      10 * minute,
			expOutputTimeDelta: 10 * minute,
		},
		{
			name:               "bytes_greater_than_limit_and_beyond_permittable_bounds",
			numBytes:           utils.Megabyte*525 + 1,
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
			numBytes:           utils.Megabyte*525 + 1,
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

	for _, c := range cases {
		outTimeDelta := determineTimeDelta(int64(c.numBytes), c.prevTimeDelta)
		assert.Equal(t, c.expOutputTimeDelta, outTimeDelta, c.name)
	}
}

func TestNumBlockCreation(t *testing.T) {
	// TODO: Make sure that block progress bars do not get created during tests. They fill the logs and make things untidy.
	cases := []struct {
		name                       string
		startT                     int64
		endT                       int64
		bytesIncrement             int64 // When using linear increase method, the amount of size increase remains linear mostly, hence this makes a perfect case to test size increments in real-world.
		fails                      bool
		expectedNumBlocksCreations int
	}{
		{
			name:                       "instant_query",
			startT:                     155 * minute,
			endT:                       155 * minute,
			bytesIncrement:             2 * utils.Megabyte,
			expectedNumBlocksCreations: 1,
			fails:                      true,
		},
		{
			name:                       "instant_query_violate_limit",
			startT:                     155 * minute,
			endT:                       155 * minute,
			bytesIncrement:             responseDataSizeHalfLimit,
			expectedNumBlocksCreations: 1,
			fails:                      true,
		},
		{
			name:                       "normal_time_range_1",
			startT:                     155 * minute,
			endT:                       165 * minute,
			bytesIncrement:             1 * utils.Megabyte,
			expectedNumBlocksCreations: 4,
		},
		{
			name:                       "normal_time_range_2",
			startT:                     100 * minute,
			endT:                       101 * minute,
			bytesIncrement:             1 * utils.Megabyte,
			expectedNumBlocksCreations: 1,
		},
		{
			name:                       "normal_time_range_3",
			startT:                     100 * minute,
			endT:                       120 * minute,
			bytesIncrement:             1 * utils.Megabyte,
			expectedNumBlocksCreations: 6,
		},
		{
			name:                       "normal_time_range_4",
			startT:                     100 * minute,
			endT:                       125 * minute,
			bytesIncrement:             1 * utils.Megabyte,
			expectedNumBlocksCreations: 7,
		},
		{
			name:                       "normal_time_range_5",
			startT:                     100 * minute,
			endT:                       200 * minute,
			bytesIncrement:             1 * utils.Megabyte,
			expectedNumBlocksCreations: 14,
		},
		{
			name:                       "normal_time_range_6",
			startT:                     100 * minute,
			endT:                       200 * minute,
			bytesIncrement:             2 * utils.Megabyte,
			expectedNumBlocksCreations: 14,
		},
		{
			name:                       "normal_time_range_7",
			startT:                     100 * minute,
			endT:                       2000 * minute,
			bytesIncrement:             2 * utils.Megabyte,
			expectedNumBlocksCreations: 62,
		},
		{
			name:                       "normal_time_range_8",
			startT:                     100 * minute,
			endT:                       20000 * minute,
			bytesIncrement:             2 * utils.Megabyte,
			expectedNumBlocksCreations: 226,
		},
		{
			name:                       "normal_time_range_9",
			startT:                     100 * minute,
			endT:                       200 * minute,
			bytesIncrement:             10 * utils.Megabyte,
			expectedNumBlocksCreations: 14,
		},
		{
			name:                       "normal_time_range_10",
			startT:                     100 * minute,
			endT:                       200 * minute,
			bytesIncrement:             9 * utils.Megabyte,
			expectedNumBlocksCreations: 14,
		},
		{
			name:                       "normal_time_range_10",
			startT:                     100 * minute,
			endT:                       110 * minute,
			bytesIncrement:             100 * utils.Megabyte,
			expectedNumBlocksCreations: 4,
		},
		{
			name:                       "normal_time_range_11",
			startT:                     100 * minute,
			endT:                       110 * minute,
			bytesIncrement:             200 * utils.Megabyte,
			expectedNumBlocksCreations: 7,
		},
		{
			name:                       "normal_time_range_11",
			startT:                     100 * minute,
			endT:                       110 * minute,
			bytesIncrement:             250 * utils.Megabyte,
			expectedNumBlocksCreations: 32,
		},
		{
			name:                       "start_less_than_end",
			startT:                     100 * minute,
			endT:                       100*minute - 1,
			bytesIncrement:             10 * utils.Megabyte,
			expectedNumBlocksCreations: 122,
			fails:                      true,
		},
	}

	for _, c := range cases {
		planConfig := &Plan{Mint: c.startT, Maxt: c.endT, ProgressEnabled: false, IsTest: true}
		_, err := Init(planConfig)
		if c.fails {
			assert.Error(t, err, c.name)
			continue
		}
		assert.NoError(t, err, c.name)
		blockCount := 0
		var bytesPrev int64 = 0
		for planConfig.ShouldProceed() {
			b, err := planConfig.NextBlock()
			// Assume fetching happened here.
			bytesPrev += c.bytesIncrement
			b.plan.update(int(bytesPrev))
			if bytesPrev > responseDataSizeHalfLimit {
				// In ideal condition, the drop in time range will drop the size by the same amount.
				bytesPrev /= 2
			}
			assert.NoError(t, err)
			blockCount++
		}
		assert.Equal(t, c.expectedNumBlocksCreations, blockCount, c.name)
	}
}
