package pgsafetype

import (
	"fmt"
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

// strings.ReplaceALL: BenchmarkSanitizationOnNullChars-8   	  287623	      4669 ns/op
// strings.Map with rune: BenchmarkSanitizationOnNullChars-8   	  308642	      3868 ns/op
func BenchmarkSanitizationOnNullChars(b *testing.B) {
	for _, bc := range tcs {
		for n := 0; n < b.N; n++ {
			// Sanitize.
			sanitizeNullChars(bc.input)
		}
	}
}

// To compare normal string vs null char string, look the results of BenchmarkStringsComp_normal-8 vs BenchmarkStringsComp_null_string-8 respectively.
// BenchmarkStringsComp_normal-8   	 1849795	       551 ns/op
func BenchmarkStringsComp_normal(b *testing.B) {
	for n := 0; n < b.N; n++ {
		// Sanitize normal string.
		sanitizeNullChars("this_is_a_normal_string")
	}
}

// BenchmarkStringsComp_null_string-8   	 1069735	       999 ns/op
func BenchmarkStringsComp_null_string(b *testing.B) {
	s := fmt.Sprintf("this_%c_null_%c_string", NullChar, NullChar)
	for n := 0; n < b.N; n++ {
		// Sanitize null chars string.
		sanitizeNullChars(s)
	}
}

// using []pgtype.Text in pgsafetype.TextArray.Set(): BenchmarkTypeArray-8   	   85900	     13161 ns/op
// using []string in pgsafetype.TextArray.Set(): 	  BenchmarkTypeArray-8   	   84028	     16224 ns/op
func BenchmarkTypeArray(b *testing.B) {
	stringArr := []string{"one", "two", "three", "four", "five", "six", "seven", "eight"}
	for n := 0; n < b.N; n++ {
		t := &TextArray{}
		err := t.Set(stringArr)
		require.NoError(b, err)
	}
}
