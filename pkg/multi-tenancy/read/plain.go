// todo: header in all new files.

package read

import (
	"fmt"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
)

type plainReadAuthorizer struct {
	authorizerConfig
}

// NewPlainReadAuthorizer is a authorizer that does not care about the token checks. It just makes sure that the
// read operations are performed against a valid tenants, which Promscale has been asked for.
func NewPlainReadAuthorizer(cfg *config.Config) (Authorizer, error) {
	matcher, err := initSafetyLabelMatcher(cfg.ValidTenants)
	if err != nil {
		return nil, fmt.Errorf("create plain read-authorizer: %w", err)
	}
	return &plainReadAuthorizer{
		authorizerConfig{
			config:               cfg,
			mtSafetyLabelMatcher: matcher,
		},
	}, nil
}

// IsValid takes in the first param as token. Since in plainAuthorizer we do not verify the token checks, so
// IsValid should always returns true.
func (a *plainReadAuthorizer) IsValid(_ string) bool {
	return true
}

func (a *plainReadAuthorizer) ApplySafetyMatcher(ms []*labels.Matcher) []*labels.Matcher {
	if a.mtSafetyLabelMatcher == nil {
		return ms
	}
	ms = append(ms, a.mtSafetyLabelMatcher)
	return ms
}
