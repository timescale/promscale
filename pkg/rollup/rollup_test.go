// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rollup

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiff(t *testing.T) {
	tcs := []struct {
		name           string
		a, b, expected []DownsampleResolution
	}{
		{
			name:     "some inclusive elements",
			a:        []DownsampleResolution{{Label: "a"}, {Label: "b"}, {Label: "c"}, {Label: "d"}},
			b:        []DownsampleResolution{{Label: "c"}, {Label: "d"}, {Label: "e"}},
			expected: []DownsampleResolution{{Label: "a"}, {Label: "b"}},
		},
		{
			name:     "b superset of a",
			a:        []DownsampleResolution{{Label: "c"}, {Label: "d"}},
			b:        []DownsampleResolution{{Label: "c"}, {Label: "d"}, {Label: "e"}},
			expected: []DownsampleResolution(nil),
		},
		{
			name:     "a empty",
			a:        []DownsampleResolution{},
			b:        []DownsampleResolution{{Label: "c"}, {Label: "d"}, {Label: "e"}},
			expected: []DownsampleResolution(nil),
		},
		{
			name:     "all elements exclusive",
			a:        []DownsampleResolution{{Label: "a"}},
			b:        []DownsampleResolution{{Label: "c"}, {Label: "d"}, {Label: "e"}},
			expected: []DownsampleResolution{{Label: "a"}},
		},
		{
			name:     "same",
			a:        []DownsampleResolution{{Label: "a"}, {Label: "b"}, {Label: "c"}, {Label: "d"}},
			b:        []DownsampleResolution{{Label: "a"}, {Label: "b"}, {Label: "c"}, {Label: "d"}},
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
