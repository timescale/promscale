// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package tenancy

import (
	"fmt"

	"github.com/timescale/promscale/pkg/prompb"
)

// writeAuthorizer is a write authorizer that authorizes if the incoming write request is valid to be written or not.
type writeAuthorizer struct {
	AuthConfig
}

var errTenantMismatch = fmt.Errorf("__tenant__ value and tenant-name from headers are different")

// NewWriteAuthorizer returns a new plainWriteAuthorizer.
func NewWriteAuthorizer(config AuthConfig) WriteAuthorizer {
	return &writeAuthorizer{config}
}

func (a *writeAuthorizer) isAuthorized(tenantName string) error {
	if a.IsTenantAllowed(tenantName) {
		return nil
	}
	return fmt.Errorf("authorization error for tenant %s: %w", tenantName, ErrUnauthorizedTenant)
}

func (a *writeAuthorizer) VerifyAndApplyTenantLabel(tenantNameFromHeader string, labels []prompb.Label) ([]prompb.Label, error) {
	if tenantNameFromHeader != "" {
		if err := a.isAuthorized(tenantNameFromHeader); err != nil {
			return labels, err
		}
		return a.getTenantLabelMatchingHeader(tenantNameFromHeader, labels)
	}
	tenantNameFromLabels := a.getTenantNameFromLabel(labels)
	return labels, a.isAuthorized(tenantNameFromLabels)
}

func (a *writeAuthorizer) getTenantLabelMatchingHeader(tenantNameFromHeader string, labels []prompb.Label) ([]prompb.Label, error) {
	for _, label := range labels {
		if label.Name == TenantLabelKey {
			if label.Value == tenantNameFromHeader {
				return labels, nil
			}
			if label.Value == "" {
				// Tenant label exists but no tenant value. This is invalid.
				return labels, fmt.Errorf("%s exists with an empty value", TenantLabelKey)
			}
			return labels, errTenantMismatch
		}
	}
	labels = append(labels, prompb.Label{Name: TenantLabelKey, Value: tenantNameFromHeader})
	return labels, nil
}

func (a *writeAuthorizer) getTenantNameFromLabel(labels []prompb.Label) string {
	for _, label := range labels {
		if label.Name == TenantLabelKey {
			return label.Value
		}
	}
	return ""
}
