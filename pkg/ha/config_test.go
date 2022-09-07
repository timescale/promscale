// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultFlags(t *testing.T) {
	var cfg *Config
	fs := flag.NewFlagSet("test", flag.ContinueOnError)

	cfg = ParseFlags(fs)

	err := fs.Parse([]string{})
	require.Nil(t, err)

	require.Equal(t, DefaultConfig.Enabled, cfg.Enabled)
	require.Equal(t, DefaultConfig.ReplicaLabelName, cfg.ReplicaLabelName)
	require.Equal(t, DefaultConfig.ClusterLabelName, cfg.ClusterLabelName)
}
