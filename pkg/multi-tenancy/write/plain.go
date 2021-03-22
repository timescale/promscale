package write

import (
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
)

// plainWriteAuthorizer is a write authorizer that authorizes if the incoming write request is valid to be written or not.
// If does to
type plainWriteAuthorizer struct {
	config *config.Config
}

// NewPlainWriteAuthorizer returns a new plainWriteAuthorizer.
func NewPlainWriteAuthorizer(config *config.Config) *plainWriteAuthorizer {
	return &plainWriteAuthorizer{config: config}
}


