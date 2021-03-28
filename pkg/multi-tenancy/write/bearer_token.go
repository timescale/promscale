package write

import (
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
	"github.com/timescale/promscale/pkg/prompb"
)

// bearerTokenWriteAuthorizer is a write authorizer that authorizes if the incoming write request is valid to be
// written or not. This is done by verifying if the incoming write request has a token that matches the provided
// bearer_token during start.
type bearerTokenWriteAuthorizer struct {
	*config.Config
}

// NewBearerTokenWriteAuthorizer returns a new plainWriteAuthorizer.
func NewBearerTokenWriteAuthorizer(config *config.Config) Authorizer {
	return &bearerTokenWriteAuthorizer{config}
}

func (a *bearerTokenWriteAuthorizer) IsAuthorized(token, tenantName string) bool {
	if !a.IsTokenValid(token) {
		return false
	}
	return a.IsTenantAllowed(tenantName)
}

func (a *bearerTokenWriteAuthorizer) VerifyAndApplyTenantLabel(tenantName string, labels []prompb.Label) ([]prompb.Label, bool) {
	for _, label := range labels {
		if label.Name == config.TenantLabelKey {
			return labels, false
		}
	}
	if tenantName == "" {
		// Non-multi-tenant write request.
		return labels, true
	}
	labels = append(labels, prompb.Label{Name: config.TenantLabelKey, Value: tenantName})
	return labels, true
}
