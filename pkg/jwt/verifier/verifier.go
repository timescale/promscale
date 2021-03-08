package verifier

import (
	"fmt"
	"github.com/dgrijalva/jwt-go/v4"
	jwt_claims "github.com/timescale/promscale/pkg/jwt/claims"
	"io/ioutil"
)

type Config struct {
	PublicKey     string
	PublicKeyPath string
	Payload
}

type Payload struct {
	Issuer   string
	Audience string
}

var (
	ErrNomatchIssuer   = fmt.Errorf("issuer does not match")
	ErrNomatchAudience = fmt.Errorf("audience does not with the claims audience list")
)

// Validate validates the config. It must be called before calling any other function.
func (c *Config) Validate() error {
	if c.Issuer == "" {
		return fmt.Errorf("issuer cannot be empty string")
	}
	if c.Audience == "" {
		return fmt.Errorf("audience cannot be empty string")
	}
	if c.PublicKey == "" && c.PublicKeyPath == "" {
		return fmt.Errorf("both public key and public key path cannot be empty string")
	} else if c.PublicKey != "" && c.PublicKeyPath != "" {
		return fmt.Errorf("public key and public key path are mutually exclusive. Please provide any one of them")
	}
	return nil
}

// FillKeyFromFiles fills the key from the supplied file path.
func (c *Config) FillKeyFromFiles() error {
	if c.PublicKeyPath == "" {
		return fmt.Errorf("public key path is empty")
	}
	data, err := ioutil.ReadFile(c.PublicKeyPath)
	if err != nil {
		return fmt.Errorf("fill key from file: %w", err)
	}
	c.PublicKey = string(data)
	return nil
}

func VerifyToken(config *Config, tokenString, audience string) (bool, error) {
	publicKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(config.PublicKey))
	if err != nil {
		return false, fmt.Errorf("parse rsa public key from PEM in verify-token: %w", err)
	}
	claims := jwt_claims.Claims{}
	token, err := jwt.ParseWithClaims(tokenString, &claims, func(token *jwt.Token) (interface{}, error) {
		if err := token.Claims.Valid(jwt.NewValidationHelper(jwt.WithoutAudienceValidation())); err != nil {
			return nil, fmt.Errorf("claims validation: %w", err)
		}
		return publicKey, nil
	}, jwt.WithAudience(audience))

	if err != nil {
		return false, fmt.Errorf("verify-token parse with claims: %w", err)
	}

	if token == nil {
		return false, fmt.Errorf("token cannot be nil")
	}
	if !token.Valid {
		return false, nil
	}

	if claims.Issuer != config.Issuer {
		return false, ErrNomatchIssuer
	}

	var audienceMatch bool
	for _, aud := range claims.Audience {
		if aud == config.Audience {
			audienceMatch = true
			break
		}
	}
	if !audienceMatch {
		return false, ErrNomatchAudience
	}
	return true, nil
}
