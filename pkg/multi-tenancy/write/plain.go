// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package write

import (
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
	"github.com/timescale/promscale/pkg/prompb"
)

// plainWriteAuthorizer is a write authorizer that authorizes if the incoming write request is valid to be written or not.
// If does to
type plainWriteAuthorizer struct {
	*config.Config
}

// NewPlainWriteAuthorizer returns a new plainWriteAuthorizer.
func NewPlainWriteAuthorizer(config *config.Config) Authorizer {
	validTenants := make(map[string]struct{})
	for _, tname := range config.ValidTenants {
		validTenants[tname] = struct{}{}
	}
	return &plainWriteAuthorizer{config}
}

func (a *plainWriteAuthorizer) IsAuthorized(_, tenantName string) bool {
	return a.IsTenantAllowed(tenantName)
}

func (a *plainWriteAuthorizer) VerifyAndApplyTenantLabel(tenantName string, labels []prompb.Label) ([]prompb.Label, bool) {
	for _, label := range labels {
		if label.Name == config.TenantLabelKey {
			return labels, false
		}
	}
	if tenantName == "" {
		// Non-multi-tenant write request.
		// We accept non-multi-tenant write requests in multi-tenancy mode.
		return labels, true
	}
	labels = append(labels, prompb.Label{Name: config.TenantLabelKey, Value: tenantName})
	return labels, true
}
