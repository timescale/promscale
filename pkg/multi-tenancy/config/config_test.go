package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	tcs := []struct {
		name       string
		c          *Config
		shouldFail bool
	}{
		{
			name: "plain config",
			c: &Config{
				AuthType: Allow,
			},
		},
		{
			name: "plain with valid tenants",
			c: &Config{
				AuthType:     Allow,
				ValidTenants: []string{"tenant-a", "tenant-b"},
			},
		},
		{
			name: "plain with bearer_token",
			c: &Config{
				AuthType:    Allow,
				BearerToken: "token",
			},
			shouldFail: true,
		},
		{
			name: "invalid type",
			c: &Config{
				AuthType: 10,
			},
			shouldFail: true,
		},
		{
			name: "invalid type with tenants",
			c: &Config{
				AuthType:     10,
				ValidTenants: []string{"tenant-a", "tenant-b"},
			},
			shouldFail: true,
		},
		{
			name: "bearer config",
			c: &Config{
				AuthType: BearerToken,
			},
			shouldFail: true,
		},
		{
			name: "bearer with valid tenants",
			c: &Config{
				AuthType:     BearerToken,
				BearerToken:  "token",
				ValidTenants: []string{"tenant-a", "tenant-b"},
			},
		},
		{
			name: "bearer type",
			c: &Config{
				AuthType:    BearerToken,
				BearerToken: "token",
			},
		},
	}
	for _, tc := range tcs {
		c := tc.c
		err := c.Validate()
		if tc.shouldFail {
			require.Error(t, err, tc.name)
		} else {
			require.NoError(t, err, tc.name)
		}
	}
}

func TestTenantAllowed(t *testing.T) {
	incomingTenant := "tenant-a"
	tcs := []struct {
		name   string
		c      *Config
		forbid bool
	}{
		{
			name: "plain config",
			c: &Config{
				AuthType: Allow,
			},
		},
		{
			name: "plain with valid tenants",
			c: &Config{
				AuthType:     Allow,
				ValidTenants: []string{"tenant-a", "tenant-b"},
			},
		},
		{
			name: "plain with valid tenants but forbid",
			c: &Config{
				AuthType:     Allow,
				ValidTenants: []string{"tenant-b"},
			},
			forbid: true,
		},
		{
			name: "bearer config",
			c: &Config{
				AuthType:    BearerToken,
				BearerToken: "token",
			},
		},
		{
			name: "bearer with valid tenants",
			c: &Config{
				AuthType:     BearerToken,
				BearerToken:  "token",
				ValidTenants: []string{"tenant-a", "tenant-b"},
			},
		},
		{
			name: "bearer with valid tenants but forbid",
			c: &Config{
				AuthType:     BearerToken,
				BearerToken:  "token",
				ValidTenants: []string{"tenant-b"},
			},
			forbid: true,
		},
	}
	for _, tc := range tcs {
		c := tc.c
		err := c.Validate()
		require.NoError(t, err, tc.name)
		if tc.forbid {
			require.False(t, c.IsTenantAllowed(incomingTenant), tc.name)
		} else {
			require.True(t, c.IsTenantAllowed(incomingTenant), tc.name)
		}
	}
}
