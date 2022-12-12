// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rollup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDecider(t *testing.T) {
	r := &Decider{
		scrapeInterval:      DefaultScrapeInterval,
		downsamplingEnabled: true,
		rollups: []rollupInfo{
			{"5_minute", 5 * time.Minute},
			{"15_minute", 15 * time.Minute},
			{"1_hour", time.Hour},
			{"1_week", 7 * 24 * time.Hour},
		},
	}
	tcs := []struct {
		name               string
		min                time.Duration
		max                time.Duration
		expectedSchemaName string
	}{
		{
			name:               "1 sec",
			min:                0,
			max:                time.Second,
			expectedSchemaName: DefaultSchema,
		}, {
			name:               "5 min",
			min:                0,
			max:                5 * time.Minute,
			expectedSchemaName: DefaultSchema,
		}, {
			name:               "30 mins",
			min:                0,
			max:                30 * time.Minute,
			expectedSchemaName: DefaultSchema,
		}, {
			name:               "1 hour",
			min:                0,
			max:                time.Hour,
			expectedSchemaName: DefaultSchema,
		}, {
			// DRY RUN
			// -------
			//
			// Assumed default scrape interval being 30 secs
			// raw -> 2,880			<-- Falls in the acceptable range.
			name:               "1 day",
			min:                0,
			max:                24 * time.Hour,
			expectedSchemaName: DefaultSchema,
		},
		{
			// DRY RUN on 500 - 5000 logic
			// --------
			//
			// Assumed default scrape interval being 30 secs
			// raw 		-> 20,160
			//
			// And, when using following rollup intervals, num samples:
			// 5 mins 	-> 2,016		<-- Falls in the acceptable range.
			// 15 mins 	-> 672
			// 1 hour 	-> 168
			// 1 week 	-> 1
			name:               "7 days",
			min:                0,
			max:                7 * 24 * time.Hour,
			expectedSchemaName: "5_minute",
		},
		{
			name:               "30 days",
			min:                0,
			max:                30 * 24 * time.Hour,
			expectedSchemaName: "15_minute",
		}, {
			// DRY RUN on 500 - 5000 logic
			// --------
			//
			// Assumed default scrape interval being 30 secs
			// raw 		-> 20,160
			//
			// And, when using following rollup intervals, num samples:
			// 5 mins 	-> 1,051,200		<-- Falls in the acceptable range.
			// 15 mins 	-> 35,040
			// 1 hour 	-> 8,760
			// 1 week 	-> 52
			name:               "1 year",
			min:                0,
			max:                12 * 30 * 24 * time.Hour,
			expectedSchemaName: "1_week",
		}, {
			name:               "100 years",
			min:                0,
			max:                100 * 12 * 30 * 24 * time.Hour,
			expectedSchemaName: "1_week",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			schemaName := r.Decide(int64(tc.min.Seconds()), int64(tc.max.Seconds()))
			require.Equal(t, tc.expectedSchemaName, schemaName, tc.name)
		})
	}
}
