// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package read

import (
	"fmt"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
)

type bearerTokenReadAuthorizer struct {
	authorizerConfig
}

func NewBearerTokenReadAuthorizer(cfg *config.Config) (Authorizer, error) {
	matcher, err := initSafetyLabelMatcher(cfg.ValidTenants)
	if err != nil {
		return nil, fmt.Errorf("create bearer_token read-authorizer: %w", err)
	}
	return &bearerTokenReadAuthorizer{
		authorizerConfig{
			config:               cfg,
			mtSafetyLabelMatcher: matcher,
		},
	}, nil
}

func (a *bearerTokenReadAuthorizer) IsValid(token string) bool {
	return token == a.config.BearerToken
}

func (a *bearerTokenReadAuthorizer) ApplySafetyMatcher(ms []*labels.Matcher) []*labels.Matcher {
	if a.mtSafetyLabelMatcher == nil {
		return ms
	}
	ms = append(ms, a.mtSafetyLabelMatcher)
	return ms
}
