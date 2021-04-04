// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package config

import "fmt"

const (
	// Allow allows all requests corresponding to be ingested/queried. it does not check for
	// authorization for the incoming tenant requests.
	Allow = iota
	// BearerToken allows only those requests to be ingested/querier that match the bearer_token.
	BearerToken
	TenantLabelKey = "__tenant__"
)

// Config defines the configuration for multi-tenancy.
type Config struct {
	AuthType     uint8
	BearerToken  string
	ValidTenants []string
	tenantsCache map[string]struct{}
}

// Validate validates the configuration of multi-tenancy config.
func (cfg *Config) Validate() error {
	switch cfg.AuthType {
	case Allow:
		if cfg.BearerToken != "" {
			return fmt.Errorf("bearer_token is not require for a allow multi-tenancy type")
		}
	case BearerToken:
		if cfg.BearerToken == "" {
			return fmt.Errorf("bearer_token is required for a bearer_token multi-tenancy type")
		}
	default:
		return fmt.Errorf("invalid multi-tenancy type: %d", cfg.AuthType)
	}
	cfg.tenantsCache = make(map[string]struct{})
	for _, tname := range cfg.ValidTenants {
		cfg.tenantsCache[tname] = struct{}{}
	}
	return nil
}

// isTokenValid returns true if the token given matches with the token provided in the start.
func (cfg *Config) IsTokenValid(token string) bool {
	return cfg.BearerToken == token
}

// IsTenantAllowed returns true if the given tenantName is allowed to be ingested.
func (cfg *Config) IsTenantAllowed(tenantName string) bool {
	if tenantName == "" {
		// Do not allow empty tenant names.
		return false
	}
	if len(cfg.tenantsCache) == 0 {
		return true
	}
	if _, ok := cfg.tenantsCache[tenantName]; ok {
		return true
	}
	return false
}
