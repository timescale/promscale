package limits

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPercent(t *testing.T) {
	t.Parallel()

	inputs := []struct {
		val     string
		percent int
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
		var tm TargetMemory
		gotErr := tm.Set(tc.val)
		require.Equal(t, tc.err, gotErr != nil, "input %v got error %v", tc.val, gotErr)
		if gotErr != nil {
			continue
		}
		require.Equal(t, tc.percent, tm.percentage)
		require.True(t, tm.Bytes() > 20000)
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
		var tm TargetMemory
		gotErr := tm.Set(tc.val)
		require.Equal(t, tc.err, gotErr != nil, "input %v got error %v", tc.val, gotErr)
		if gotErr != nil {
			continue
		}
		require.Equal(t, tc.bytes, tm.Bytes(), "input %v", tc.val)
		require.Equal(t, 0, tm.percentage)
	}
}

func TestString(t *testing.T) {
	t.Parallel()

	var tm TargetMemory
	gotErr := tm.Set("80% ")
	require.NoError(t, gotErr)
	require.Equal(t, "80%", tm.String())
	gotErr = tm.Set(" 3000 ")
	require.NoError(t, gotErr)
	require.Equal(t, "3000", tm.String())
}
