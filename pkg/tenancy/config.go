// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package tenancy

import (
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

// TenantLabelKey is a label key reserved for tenancy.
const TenantLabelKey = "__tenant__"

// AuthConfig defines configuration type for tenancy.
type AuthConfig interface {
	// allowNonTenants returns true if tenancy is asked to accept write-requests from non-multi-tenants.
	allowNonTenants() bool
	// getTenantSafetyMatcher returns a safety matcher that ensures queries only have data of tenants that are authorized.
	getTenantSafetyMatcher() (*labels.Matcher, error)
	// IsTenantAllowed returns true if the given tenantName is allowed to be ingested.
	IsTenantAllowed(string) bool
	// ValidTenants returns a list of tenants that are authorized in the current session of Promscale.
	// If -metrics.multi-tenancy.allow-non-tenants is applied, then []tenantName is empty and allTenantsValid is true.
	ValidTenants() (tenantName []string, allTenantsValid bool)
}

// selectiveConfig defines the configuration for tenancy where only valid tenants are allowed.
type selectiveConfig struct {
	nonTenants      bool
	validTenantsMap map[string]struct{}
}

// NewSelectiveTenancyConfig creates a new config for tenancy where only valid tenants are allowed.
func NewSelectiveTenancyConfig(validTenants []string, allowNonTenants bool) AuthConfig {
	cfg := &selectiveConfig{
		validTenantsMap: make(map[string]struct{}),
		nonTenants:      allowNonTenants,
	}
	for _, tname := range validTenants {
		cfg.validTenantsMap[tname] = struct{}{}
	}
	return cfg
}

func (cfg *selectiveConfig) tenants() []string {
	tenants := make([]string, len(cfg.validTenantsMap))
	i := 0
	for k := range cfg.validTenantsMap {
		tenants[i] = k
		i++
	}
	sort.Strings(tenants) // We sort here to be deterministic, as we verify tenant matchers in tests.
	return tenants
}

func (cfg *selectiveConfig) allowNonTenants() bool {
	return cfg.nonTenants
}

// IsTenantAllowed returns true if the given tenantName is allowed to be ingested.
func (cfg *selectiveConfig) IsTenantAllowed(tenantName string) bool {
	if tenantName == "" {
		return cfg.allowNonTenants()
	}
	if _, ok := cfg.validTenantsMap[tenantName]; ok {
		return true
	}
	return false
}

func (cfg *selectiveConfig) ValidTenants() (tenantName []string, allTenantsValid bool) {
	for name := range cfg.validTenantsMap {
		tenantName = append(tenantName, name)
	}
	return tenantName, false
}

func (cfg *selectiveConfig) getTenantSafetyMatcher() (*labels.Matcher, error) {
	matcher, err := getMTSafeLabelMatcher(cfg.tenants())
	if err != nil {
		return nil, fmt.Errorf("init safety label-matche: %w", err)
	}
	if cfg.allowNonTenants() {
		modifiedMatcher, err := labels.NewMatcher(matcher.Type, matcher.Name, fmt.Sprintf("%s%s^$", matcher.Value, regexOR)) // Allow an empty tenant as well, indicated by '^$'.
		if err != nil {
			return nil, fmt.Errorf("modified matcher: %w", err)
		}
		matcher = modifiedMatcher
	}
	return matcher, nil
}

type AllowAllTenantsConfig struct {
	nonTenants bool
}

// NewAllowAllTenantsConfig creates a new config for tenancy where all tenants are allowed.
func NewAllowAllTenantsConfig(allowNonTenants bool) AuthConfig {
	return &AllowAllTenantsConfig{nonTenants: allowNonTenants}
}

//nolint | kept inorder to implement the interface.
func (cfg *AllowAllTenantsConfig) tenants() []string {
	return nil
}

func (cfg *AllowAllTenantsConfig) allowNonTenants() bool {
	return cfg.nonTenants
}

// IsTenantAllowed returns true if the given tenantName is allowed to be ingested.
func (cfg *AllowAllTenantsConfig) IsTenantAllowed(tenantName string) bool {
	if !cfg.nonTenants && tenantName == "" {
		// Do not allow empty tenant names.
		return false
	}
	return true
}

func (cfg *AllowAllTenantsConfig) ValidTenants() (tenantName []string, allTenantsValid bool) {
	return []string{}, true
}

func (cfg *AllowAllTenantsConfig) getTenantSafetyMatcher() (*labels.Matcher, error) {
	if cfg.allowNonTenants() {
		return nil, nil
	}
	matcher, err := labels.NewMatcher(labels.MatchNotEqual, TenantLabelKey, "") // Allow all tenants but no non-tenants.
	if err != nil {
		return nil, fmt.Errorf("init safety label-matcher: %w", err)
	}
	return matcher, nil
}

// getMTSafeLabelMatcher creates a new safety label matcher, from the given list of valid tenants.
func getMTSafeLabelMatcher(validTenants []string) (*labels.Matcher, error) {
	mtSafetyLabelVal := strings.Join(validTenants, regexOR)
	mtSafetyLabelMatcher, err := labels.NewMatcher(labels.MatchRegexp, TenantLabelKey, mtSafetyLabelVal)
	if err != nil {
		return nil, fmt.Errorf("init safety label-matcher: %w", err)
	}
	return mtSafetyLabelMatcher, nil
}
