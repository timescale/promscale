package utils

import "github.com/prometheus/common/config"

// Auth defines the authentication for prom-migrator.
type Auth struct {
	Username    string
	Password    string
	BearerToken string
}

// Convert converts the auth credentials to HTTP client compatible format.
func (a *Auth) ToHTTPClientConfig() config.HTTPClientConfig {
	conf := config.HTTPClientConfig{}
	if a.Password != "" {
		conf.BasicAuth = &config.BasicAuth{
			Username: a.Username,
			Password: config.Secret(a.Password),
		}
	}
	if a.BearerToken != "" {
		// Since Password and BearerToken are mutually exclusive, we assign both on input flag condition
		// and leave upto the HTTPClientConfig.Validate() for validation.
		conf.BearerToken = config.Secret(a.BearerToken)
	}
	return conf
}
