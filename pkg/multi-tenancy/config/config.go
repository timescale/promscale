// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package config

// TenantLabelKey is a label key reserved for multi-tenancy.
const TenantLabelKey = "__tenant__"

// Config defines configuration for multi-tenancy.
type Config interface {
	// Tenants returns the list of valid tenants.
	Tenants() []string
	// IsTenantAllowed returns true if the given tenantName is allowed to be ingested.
	IsTenantAllowed(string) bool
}

// selectiveConfig defines the configuration for multi-tenancy where only valid tenants are allowed.
type selectiveConfig struct {
	validTenantsMap map[string]struct{}
}

// New creates a new config for multi-tenancy where only valid tenants are allowed.
func NewSelectiveTenancyConfig(validTenants []string) Config {
	cfg := &selectiveConfig{validTenantsMap: make(map[string]struct{})}
	for _, tname := range validTenants {
		cfg.validTenantsMap[tname] = struct{}{}
	}
	return cfg
}

func (cfg *selectiveConfig) Tenants() []string {
	tenants := make([]string, len(cfg.validTenantsMap))
	i := 0
	for k := range cfg.validTenantsMap {
		tenants[i] = k
	}
	return tenants
}

// IsTenantAllowed returns true if the given tenantName is allowed to be ingested.
func (cfg *selectiveConfig) IsTenantAllowed(tenantName string) bool {
	if tenantName == "" {
		// Do not allow empty tenant names.
		return false
	}
	if len(cfg.validTenantsMap) == 0 {
		return true
	}
	if _, ok := cfg.validTenantsMap[tenantName]; ok {
		return true
	}
	return false
}

type OpenConfig struct{}

// NewOpenTenancyConfig creates a new config for multi-tenancy where all tenants are allowed.
func NewOpenTenancyConfig() Config {
	return &OpenConfig{}
}

func (cfg *OpenConfig) Tenants() []string {
	return []string{}
}

// IsTenantAllowed returns true if the given tenantName is allowed to be ingested.
func (cfg *OpenConfig) IsTenantAllowed(_ string) bool {
	return true
}
