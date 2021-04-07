// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package write

import (
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
	"github.com/timescale/promscale/pkg/prompb"
)

// writeAuthorizer is a write authorizer that authorizes if the incoming write request is valid to be written or not.
type writeAuthorizer struct {
	config.Config
}

// NewAuthorizer returns a new plainWriteAuthorizer.
func NewAuthorizer(config config.Config) Authorizer {
	return &writeAuthorizer{config}
}

func (a *writeAuthorizer) IsAuthorized(tenantName string) bool {
	return a.IsTenantAllowed(tenantName)
}

func (a *writeAuthorizer) VerifyAndApplyTenantLabel(tenantName string, labels []prompb.Label) ([]prompb.Label, bool) {
	for _, label := range labels {
		if label.Name == config.TenantLabelKey && label.Value != tenantName {
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
