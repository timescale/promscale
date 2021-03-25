// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgutf8str

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var tcs = []struct {
	name           string
	input          string
	expectedOutput string
}{
	{
		name:           "null char at start",
		input:          fmt.Sprintf("%cabcdefgh", NullChar),
		expectedOutput: fmt.Sprintf("%cabcdefgh", NullCharSanitize),
	},
	{
		name:           "null char at center",
		input:          fmt.Sprintf("abcd%cefgh", NullChar),
		expectedOutput: fmt.Sprintf("abcd%cefgh", NullCharSanitize),
	},
	{
		name:           "null char at center, twice",
		input:          fmt.Sprintf("ab%ccdef%cgh", NullChar, NullChar),
		expectedOutput: fmt.Sprintf("ab%ccdef%cgh", NullCharSanitize, NullCharSanitize),
	},
	{
		name:           "null char at end",
		input:          fmt.Sprintf("abcdefgh%c", NullChar),
		expectedOutput: fmt.Sprintf("abcdefgh%c", NullCharSanitize),
	},
	{
		name:           "null char only",
		input:          fmt.Sprintf("%c", NullChar),
		expectedOutput: fmt.Sprintf("%c", NullCharSanitize),
	},
	{
		name:           "null char twice only",
		input:          fmt.Sprintf("%c%c", NullChar, NullChar),
		expectedOutput: fmt.Sprintf("%c%c", NullCharSanitize, NullCharSanitize),
	},
}

func TestSanitizeNullCharAndReverting(t *testing.T) {
	for _, tc := range tcs {
		// Test sanitize.
		sanitizedStr := sanitizeNullChars(tc.input)
		require.Equal(t, tc.expectedOutput, sanitizedStr, tc.name)
		// Test revert.
		original := revertSanitization(sanitizedStr)
		require.Equal(t, tc.input, original, tc.input+": revert")
	}
}

func BenchmarkMapWithContains(b *testing.B) {
	benchmarks := []struct {
		name                string
		testSanitizeString  string
		testSanitizedString string
	}{
		{"No nulls", "this_is_a_normal_string", "this_is_a_normal_string"},
		{"With nulls", fmt.Sprintf("this_%c_null_%c_string", NullChar, NullChar), fmt.Sprintf("this_%c_null_%c_string", NullCharSanitize, NullCharSanitize)},
	}
	for _, bm := range benchmarks {
		s := bm.testSanitizeString
		b.Run(bm.name+"_sanitize", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if strings.ContainsRune(s, NullChar) {
					strings.Map(replaceFunc, s)
				}
			}
		})
		s = bm.testSanitizedString
		b.Run(bm.name+"_revert", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if strings.ContainsRune(s, NullCharSanitize) {
					strings.Map(revertFunc, s)
				}
			}
		})
	}
}

func BenchmarkMapWithoutContains(b *testing.B) {
	benchmarks := []struct {
		name                string
		testSanitizeString  string
		testSanitizedString string
	}{
		{"No nulls", "this_is_a_normal_string", "this_is_a_normal_string"},
		{"With nulls", fmt.Sprintf("this_%c_null_%c_string", NullChar, NullChar), fmt.Sprintf("this_%c_null_%c_string", NullCharSanitize, NullCharSanitize)},
	}
	for _, bm := range benchmarks {
		s := bm.testSanitizeString
		b.Run(bm.name+"_sanitize", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s = strings.Map(replaceFunc, s)
			}
		})
		s = bm.testSanitizedString
		b.Run(bm.name+"_revert", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s = strings.Map(revertFunc, s)
			}
		})
	}
}
