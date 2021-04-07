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

type readAuthorizer struct {
	config.Config
	// mtSafetyLabelPair is a label-pair that is applied to incoming multi-tenant read requests for security reasons.
	// This matcher helps prevent a query from querying a tenant for the query has not been authorized.
	mtSafetyLabelMatcher *labels.Matcher
}

// NewPlainReadAuthorizer is a authorizer that does not care about the token checks. It just makes sure that the
// read operations are performed against a valid tenants, which Promscale has been asked for.
func NewAuthorizer(cfg config.Config) (Authorizer, error) {
	if _, ok := cfg.(*config.OpenConfig); ok {
		return &readAuthorizer{
			Config:               cfg,
			mtSafetyLabelMatcher: nil,
		}, nil
	}
	matcher, err := getSafetyLabelMatcher(cfg.Tenants())
	if err != nil {
		return nil, fmt.Errorf("create plain read-authorizer: %w", err)
	}
	return &readAuthorizer{
		Config:               cfg,
		mtSafetyLabelMatcher: matcher,
	}, nil
}

func (a *readAuthorizer) ApplySafetyMatcher(ms []*labels.Matcher) []*labels.Matcher {
	if a.mtSafetyLabelMatcher == nil {
		return ms
	}
	ms = append(ms, a.mtSafetyLabelMatcher)
	return ms
}

// getSafetyLabelMatcher creates a new safety label matcher, from the given list of valid tenants.
func getSafetyLabelMatcher(validTenants []string) (*labels.Matcher, error) {
	mtSafetyLabelVal := strings.Join(validTenants, regexOR)
	mtSafetyLabelMatcher, err := labels.NewMatcher(labels.MatchRegexp, config.TenantLabelKey, mtSafetyLabelVal)
	if err != nil {
		return nil, fmt.Errorf("init safety label-matcher: %w", err)
	}
	return mtSafetyLabelMatcher, nil
}
