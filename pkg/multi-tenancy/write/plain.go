package write

import (
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
	"github.com/timescale/promscale/pkg/prompb"
)

// plainWriteAuthorizer is a write authorizer that authorizes if the incoming write request is valid to be written or not.
// If does to
type plainWriteAuthorizer struct {
	authorizerConfig
}

// NewPlainWriteAuthorizer returns a new plainWriteAuthorizer.
func NewPlainWriteAuthorizer(config *config.Config) Authorizer {
	validTenants := make(map[string]struct{})
	for _, tname := range config.ValidTenants {
		validTenants[tname] = struct{}{}
	}
	return &plainWriteAuthorizer{
		authorizerConfig{
			config:           config,
			validTenantCache: validTenants,
		},
	}
}

func (a *plainWriteAuthorizer) IsAuthorized(_, tenantName string) bool {
	if len(a.validTenantCache) == 0 {
		return true
	}
	if _, ok := a.validTenantCache[tenantName]; ok {
		return true
	}
	return false
}

func (a *plainWriteAuthorizer) VerifyAndApplyTenantLabel(tenantName string, labels []prompb.Label) ([]prompb.Label, bool) {
	for _, label := range labels {
		if label.Name == config.TenantLabelKey {
			return labels, false
		}
	}
	labels = append(labels, prompb.Label{Name: config.TenantLabelKey, Value: tenantName})
	return labels, true
}
