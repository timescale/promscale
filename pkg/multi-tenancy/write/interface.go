// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package write

import (
	"fmt"
	"github.com/timescale/promscale/pkg/prompb"
)

var ErrInvalidLabels = fmt.Errorf("invalid labels-set: labels-set should not contain __tenant__ label")

// Authorizer tells if a write request is authorized to be written.
type Authorizer interface {
	// IsAuthorized verifies if the token and tenant to be inserted are both authorized.
	// Note: Even though in start of Promscale, the bearer_token in incoming write-request is verified by the authHandler,
	// we do the token verification again in order to avoid any edge cases and maintain a clean design.
	IsAuthorized(tenantName string) bool
	// VerifyAndApplyTenantLabel verifies that the __tenant__ label in the samples aligns with the TENANT specified by the header in the request.
	// After confirming that, it appends a __tenant__ label pair into the incoming labels, so that they can be ingested as
	// a new series in the database.
	VerifyAndApplyTenantLabel(tenantName string, labels []prompb.Label) ([]prompb.Label, bool)
}
