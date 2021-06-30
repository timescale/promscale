// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package throughput

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThroughputWatcher(t *testing.T) {
	// Test do not report throughput.
	InitWatcher(0)
	require.True(t, throughputWatcher == nil)

	// Test report throughput.
	InitWatcher(time.Second)
	require.True(t, throughputWatcher.every == time.Second)
}
