// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package day

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	tcs := []struct {
		in  string
		out string
	}{
		{
			in:  "5m",
			out: "5m",
		}, {
			in:  "2.5m",
			out: "2m30s",
		}, {
			in:  "1.1d1s",
			out: "1d2h24m1s",
		}, {
			in:  "24d",
			out: "24d",
		}, {
			in:  "1.5d",
			out: "1d12h",
		}, {
			in:  "4d1h5m",
			out: "4d1h5m",
		}, {
			in:  "1000h",
			out: "41d16h",
		}, {
			in:  "1000h1m",
			out: "41d16h1m",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.in, func(t *testing.T) {
			var d Duration
			require.NoError(t, d.UnmarshalText([]byte(tc.in)))
			require.Equal(t, tc.out, d.String())
		})
	}
}
