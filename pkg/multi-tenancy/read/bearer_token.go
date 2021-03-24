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
	if token == a.config.BearerToken {
		return true
	}
	return false
}

func (a *bearerTokenReadAuthorizer) ApplySafetyMatcher(ms []*labels.Matcher) []*labels.Matcher {
	ms = append(ms, a.mtSafetyLabelMatcher)
	return ms
}
