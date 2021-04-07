package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTenantAllowed(t *testing.T) {
	incomingTenant := "tenant-a"
	emptyTenant := ""
	tcs := []struct {
		name   string
		c      Config
		forbid bool
	}{
		{
			name: "allow all tenants",
			c:    &OpenConfig{},
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
		if _, ok := c.(*OpenConfig); ok {
			require.True(t, c.IsTenantAllowed(emptyTenant))
		} else {
			require.False(t, c.IsTenantAllowed(emptyTenant), tc.name) // Empty tenants must never be allowed.
		}
	}
}
