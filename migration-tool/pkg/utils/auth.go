// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package utils

import "github.com/prometheus/common/config"

// Auth defines the authentication for prom-migrator.
type Auth struct {
	Username        string
	Password        string
	PasswordFile    string
	BearerToken     string
	BearerTokenFile string
	config.TLSConfig
}

// Convert converts the auth credentials to HTTP client compatible format.
func (a *Auth) ToHTTPClientConfig() config.HTTPClientConfig {
	conf := config.HTTPClientConfig{}
	if a.Password != "" {
		conf.BasicAuth = &config.BasicAuth{
			Username: a.Username,
		}
		if a.PasswordFile != "" {
			conf.BasicAuth.PasswordFile = a.PasswordFile
		} else {
			conf.BasicAuth.Password = config.Secret(a.Password)
		}
	}
	// Note: Even though password and bearer_token are mutually exclusive, we apply both. The part of verification (whether both were used)
	// is left on .Validate() function which is expected to be called in the main. This avoids us from missing corner cases.
	if a.BearerTokenFile != "" {
		conf.BearerTokenFile = a.BearerTokenFile
	} else if a.BearerToken != "" {
		// Since Password and BearerToken are mutually exclusive, we assign both on input flag condition
		// and leave upto the HTTPClientConfig.Validate() for validation.
		conf.BearerToken = config.Secret(a.BearerToken)
	}
	if appliedAny(a.CAFile, a.CertFile, a.KeyFile, a.ServerName) || a.InsecureSkipVerify {
		conf.TLSConfig = a.TLSConfig
	}
	return conf
}

// appliedAny returns true if any of the fields are non-empty.
func appliedAny(sets ...string) bool {
	for _, s := range sets {
		if s != "" {
			return true
		}
	}
	return false
}
