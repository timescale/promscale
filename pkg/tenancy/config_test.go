// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package tenancy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTenantAllowed(t *testing.T) {
	incomingTenant := "tenant-a"
	emptyTenant := ""
	tcs := []struct {
		name   string
		c      AuthConfig
		forbid bool
	}{
		{
			name: "allow all tenants",
			c:    &AllowAllTenantsConfig{nonTenants: true},
		},
		{
			name: "allow valid tenants only",
			c: &selectiveConfig{
				validTenantsMap: map[string]struct{}{
					"tenant-a": {},
					"tenant-b": {},
				},
			},
		},
		{
			name: "forbid tenants",
			c: &selectiveConfig{
				validTenantsMap: map[string]struct{}{
					"tenant-b": {},
				},
			},
			forbid: true,
		},
	}
	for _, tc := range tcs {
		c := tc.c
		if tc.forbid {
			require.False(t, c.IsTenantAllowed(incomingTenant), tc.name)
		} else {
			require.True(t, c.IsTenantAllowed(incomingTenant), tc.name)
		}
		if _, ok := c.(*AllowAllTenantsConfig); ok {
			require.True(t, c.IsTenantAllowed(emptyTenant))
		} else {
			require.False(t, c.IsTenantAllowed(emptyTenant), tc.name) // Empty tenants must never be allowed.
		}
	}
}

func TestGetTenantSafetyMatcher(t *testing.T) {
	// Test selective config with non-MT ops.
	conf := NewSelectiveTenancyConfig([]string{"tenant-a", "tenant-b"}, false)
	matcher, err := conf.getTenantSafetyMatcher()
	require.NoError(t, err)
	require.Equal(t, `__tenant__=~"tenant-a|tenant-b"`, matcher.String())

	// Test selective config with MT ops.
	conf = NewSelectiveTenancyConfig([]string{"tenant-a", "tenant-b"}, true)
	matcher, err = conf.getTenantSafetyMatcher()
	require.NoError(t, err)
	require.Equal(t, `__tenant__=~"tenant-a|tenant-b|^$"`, matcher.String())

	// Test allow-all config with non-MT ops.
	conf = NewAllowAllTenantsConfig(false)
	matcher, err = conf.getTenantSafetyMatcher()
	require.NoError(t, err)
	require.Equal(t, `__tenant__!=""`, matcher.String())

	// Test selective config with MT ops.
	conf = NewAllowAllTenantsConfig(true)
	matcher, err = conf.getTenantSafetyMatcher()
	require.NoError(t, err)
	if matcher != nil {
		require.Fail(t, "matcher was expected to be nil")
	}
}
