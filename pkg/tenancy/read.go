// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package tenancy

import (
	"fmt"

	"github.com/prometheus/prometheus/pkg/labels"
)

type readAuthorizer struct {
	AuthConfig
	// mtSafetyLabelPair is a label-pair that is applied to incoming multi-tenant read requests for security reasons.
	// This matcher helps prevent a query from querying a tenant for the query has not been authorized.
	mtSafetyLabelMatcher *labels.Matcher
}

// NewReadAuthorizer is a authorizer for performing read operations on valid tenants.
func NewReadAuthorizer(cfg AuthConfig) (ReadAuthorizer, error) {
	matcher, err := cfg.getTenantSafetyMatcher()
	if err != nil {
		return nil, fmt.Errorf("get safety tenant matcher: %w", err)
	}
	return &readAuthorizer{
		AuthConfig:           cfg,
		mtSafetyLabelMatcher: matcher,
	}, nil
}

func (a *readAuthorizer) AppendTenantMatcher(ms []*labels.Matcher) []*labels.Matcher {
	if a.mtSafetyLabelMatcher == nil {
		return ms
	}
	ms = append(ms, a.mtSafetyLabelMatcher)
	return ms
}
