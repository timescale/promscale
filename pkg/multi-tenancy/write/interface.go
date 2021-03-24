package write

import (
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
	"github.com/timescale/promscale/pkg/prompb"
)

type authorizerConfig struct {
	config           *config.Config
	validTenantCache map[string]struct{}
}

// Authorizer tells if a write request is authorized to be written.
type Authorizer interface {
	IsAuthorized(token, tenantName string) bool
	// VerifyAndApplyTenantLabel verifies if the incoming write-request does not contains a __tenant__ label pair.
	// After confirming that, it appends a __tenant__ label pair into the incoming labels, so that they can be ingested as
	// a new series in the database.
	VerifyAndApplyTenantLabel(tenantName string, labels []prompb.Label) ([]prompb.Label, bool)
}
