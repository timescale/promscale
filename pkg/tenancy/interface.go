// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package tenancy

import (
	"fmt"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/prompb"
)

const regexOR = "|"

var ErrUnauthorizedTenant = fmt.Errorf("unauthorized or invalid tenant")

// ReadAuthorizer tells if a read request is allowed to query via Promscale.
type ReadAuthorizer interface {
	// AppendTenantMatcher applies a safety matcher to incoming query matchers. This safety matcher is responsible
	// from prevent unauthorized query reads from tenants that the incoming query is not supposed to read.
	AppendTenantMatcher(ms []*labels.Matcher) []*labels.Matcher
}

// WriteAuthorizer tells if a write request is authorized to be written.
type WriteAuthorizer interface {
	// isAuthorized verifies if the tenant to be inserted is authorized. It returns a ErrUnauthorizedTenant if the tenant is not authorized.
	isAuthorized(tenantName string) error
	// VerifyAndApplyTenantLabel verifies and applies the __tenant__ label in the incoming labels.
	VerifyAndApplyTenantLabel(tenantName string, labels []prompb.Label) ([]prompb.Label, error)
}
