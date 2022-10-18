// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rollup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/util"
)

func TestParse(t *testing.T) {
	tcs := []struct {
		name             string
		input            string
		numResolutions   int
		parsedResolution []DownsampleResolution
		shouldFail       bool
	}{
		{
			name:  "default entries",
			input: "short:5m:90d,long:1h:395d",
			parsedResolution: []DownsampleResolution{
				{"short", util.DayDuration(5 * time.Minute), util.DayDuration(90 * 24 * time.Hour)},
				{"long", util.DayDuration(time.Hour), util.DayDuration(395 * 24 * time.Hour)},
			},
			numResolutions: 2,
		},
		{
			name:  "default entries with space",
			input: "short:5m:90d,  long:1h:395d",
			parsedResolution: []DownsampleResolution{
				{"short", util.DayDuration(5 * time.Minute), util.DayDuration(90 * 24 * time.Hour)},
				{"long", util.DayDuration(time.Hour), util.DayDuration(395 * 24 * time.Hour)},
			},
			numResolutions: 2,
		},
		{
			name:  "only short",
			input: "short:5m:90d",
			parsedResolution: []DownsampleResolution{
				{"short", util.DayDuration(5 * time.Minute), util.DayDuration(90 * 24 * time.Hour)},
			},
			numResolutions: 1,
		},
		{
			name:  "only short with comma",
			input: "short:5m:90d,",
			parsedResolution: []DownsampleResolution{
				{"short", util.DayDuration(5 * time.Minute), util.DayDuration(90 * 24 * time.Hour)},
			},
			numResolutions: 1,
		},
		{
			name:  "only short with space",
			input: "short:5m:90d,  ",
			parsedResolution: []DownsampleResolution{
				{"short", util.DayDuration(5 * time.Minute), util.DayDuration(90 * 24 * time.Hour)},
			},
			numResolutions: 1,
		},
		{
			name:  "mix",
			input: "short:5m:90d,long:1h:395d, very_long:2d:365d , very_short:1m:90d",
			parsedResolution: []DownsampleResolution{
				{"short", util.DayDuration(5 * time.Minute), util.DayDuration(90 * 24 * time.Hour)},
				{"long", util.DayDuration(time.Hour), util.DayDuration(395 * 24 * time.Hour)},
				{"very_long", util.DayDuration(2 * 24 * time.Hour), util.DayDuration(365 * 24 * time.Hour)},
				{"very_short", util.DayDuration(time.Minute), util.DayDuration(90 * 24 * time.Hour)},
			},
			numResolutions: 4,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			resolutions, err := Parse(tc.input)
			if tc.shouldFail {
				require.Error(t, err, tc.name)
				return
			}
			require.NoError(t, err, tc.name)
			require.Equal(t, tc.numResolutions, len(resolutions), tc.name)
			require.Equal(t, tc.parsedResolution, resolutions, tc.name)
		})
	}
}

func TestDiff(t *testing.T) {
	tcs := []struct {
		name           string
		a, b, expected []DownsampleResolution
	}{
		{
			name:     "some inclusive elements",
			a:        []DownsampleResolution{{label: "a"}, {label: "b"}, {label: "c"}, {label: "d"}},
			b:        []DownsampleResolution{{label: "c"}, {label: "d"}, {label: "e"}},
			expected: []DownsampleResolution{{label: "a"}, {label: "b"}},
		},
		{
			name:     "b superset of a",
			a:        []DownsampleResolution{{label: "c"}, {label: "d"}},
			b:        []DownsampleResolution{{label: "c"}, {label: "d"}, {label: "e"}},
			expected: []DownsampleResolution(nil),
		},
		{
			name:     "a empty",
			a:        []DownsampleResolution{},
			b:        []DownsampleResolution{{label: "c"}, {label: "d"}, {label: "e"}},
			expected: []DownsampleResolution(nil),
		},
		{
			name:     "all elements exclusive",
			a:        []DownsampleResolution{{label: "a"}},
			b:        []DownsampleResolution{{label: "c"}, {label: "d"}, {label: "e"}},
			expected: []DownsampleResolution{{label: "a"}},
		},
		{
			name:     "same",
			a:        []DownsampleResolution{{label: "a"}, {label: "b"}, {label: "c"}, {label: "d"}},
			b:        []DownsampleResolution{{label: "a"}, {label: "b"}, {label: "c"}, {label: "d"}},
			expected: []DownsampleResolution(nil),
		},
		{
			name:     "empty",
			a:        []DownsampleResolution{},
			b:        []DownsampleResolution{},
			expected: []DownsampleResolution(nil),
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, diff(tc.a, tc.b), tc.name)
		})
	}
}
