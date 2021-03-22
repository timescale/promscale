package config

import "fmt"

const (
	// Allow allows all requests corresponding to be ingested/queried. it does not check for
	// authorization for the incoming tenant requests.
	Allow = iota
	// BearerToken allows only those requests to be ingested/querier that match the bearer_token.
	BearerToken
)

// Config defines the configuration for multi-tenancy.
type Config struct {
	AuthType uint8
	BearerToken string
	ValidTenants []string
}

// Validate validates the configuration of multi-tenancy config.
func (cfg *Config) Validate() error {
	switch cfg.AuthType {
	case Allow:
		if cfg.BearerToken != "" {
			return fmt.Errorf("bearer_token is not require for a allow multi-tenancy type")
		}
	case BearerToken:
		if cfg.BearerToken == "" {
			return fmt.Errorf("bearer_token is required for a bearer_token multi-tenancy type")
		}
	default:
		panic(fmt.Sprintf("invalid multi-tenancy type: %d", cfg.AuthType))
	}
	return nil
}

// isTokenValid returns true if the token given matches with the token provided in the start.
func (cfg *Config) isTokenValid(token string) bool {
	if cfg.BearerToken == token {
		return true
	}
	return false
}

// IsTenantAllowed returns true if the given tenantName is allowed to be ingested.
func (cfg *Config) isTenantAllowed(tenantName string) bool {
	if len(cfg.ValidTenants) == 0 {
		return true
	}
	for _, tenant := range cfg.ValidTenants {
		if tenant == tenantName {
			return true
		}
	}
	return false
}
