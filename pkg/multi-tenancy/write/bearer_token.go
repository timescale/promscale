package write

import (
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
	"github.com/timescale/promscale/pkg/prompb"
)

// bearerTokenWriteAuthorizer is a write authorizer that authorizes if the incoming write request is valid to be
// written or not. This is done by verifying if the incoming write request has a token that matches the provided
// bearer_token during start.
type bearerTokenWriteAuthorizer struct {
	authorizerConfig
}

// NewBearerTokenWriteAuthorizer returns a new plainWriteAuthorizer.
func NewBearerTokenWriteAuthorizer(config *config.Config) Authorizer {
	validTenants := make(map[string]struct{})
	for _, tname := range config.ValidTenants {
		validTenants[tname] = struct{}{}
	}
	return &bearerTokenWriteAuthorizer{
		authorizerConfig{
			config:           config,
			validTenantCache: validTenants,
		},
	}
}

func (a *bearerTokenWriteAuthorizer) IsAuthorized(token, tenantName string) bool {
	if token != a.config.BearerToken {
		return false
	}
	if len(a.validTenantCache) == 0 {
		return true
	}
	if _, ok := a.validTenantCache[tenantName]; ok {
		return true
	}
	return false
}

func (a *bearerTokenWriteAuthorizer) VerifyAndApplyTenantLabel(tenantName string, labels []prompb.Label) ([]prompb.Label, bool) {
	for _, label := range labels {
		if label.Name == config.TenantLabelKey {
			return labels, false
		}
	}
	labels = append(labels, prompb.Label{Name: config.TenantLabelKey, Value: tenantName})
	return labels, true
}
