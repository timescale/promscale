// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package read

import (
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
)

const regexOR = "|"

type authorizerConfig struct {
	config *config.Config
	// mtSafetyLabelPair is a label-pair that is applied to incoming multi-tenant read requests for security reasons.
	// This matcher helps prevent a query from querying a tenant for the query has not been authorized.
	mtSafetyLabelMatcher *labels.Matcher
}

// Authorizer tells if a read request is allowed to query via Promscale.
type Authorizer interface {
	// IsValid validates the given token against the provided token during start.
	IsValid(token string) bool
	// ApplySafetyMatcher applies a safety matcher to incoming query matchers. This safety matcher is responsible
	// from prevent unauthorized query reads from tenants that the incoming query is not supposed to read.
	ApplySafetyMatcher(ms []*labels.Matcher) []*labels.Matcher
}

// initSafetyLabelMatcher creates a new safety label matcher, from the given list of valid tenants.
func initSafetyLabelMatcher(validTenants []string) (*labels.Matcher, error) {
	if len(validTenants) == 0 {
		return nil, nil
	}
	mtSafetyLabelVal := strings.Join(validTenants, regexOR)
	mtSafetyLabelMatcher, err := labels.NewMatcher(labels.MatchRegexp, config.TenantLabelKey, mtSafetyLabelVal)
	if err != nil {
		return nil, fmt.Errorf("init safety label-matcher: %w", err)
	}
	return mtSafetyLabelMatcher, nil
}
