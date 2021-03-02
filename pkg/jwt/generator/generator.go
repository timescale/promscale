package generator

import (
	"fmt"
	"time"

	"github.com/dgrijalva/jwt-go/v4"
)

type Config struct {
	PrivateKey string
	Payload
}

type Payload struct {
	Issuer string
	Audience string
	IssuedAt time.Time
	ExpireAt time.Time
	TenantList []string
}

type Claims struct {
	TenantList []string
	jwt.StandardClaims
}

var signingMethod = jwt.GetSigningMethod("RS256")

func (c *Config) Validate() error {
	if c.Issuer == "" {
		return fmt.Errorf("issuer cannot be empty string")
	}
	if c.Audience == "" {
		return fmt.Errorf("audience cannot be empty string")
	}
	if c.PrivateKey == "" {
		return fmt.Errorf("private key cannot be empty string")
	}
	return nil
}

// GenerateToken generates the JWT token based on the config. It sets the IssuedAt time to time.Now() in case it is unset.
func GenerateToken(config *Config) (tokenStr string, err error) {
	token := jwt.New(signingMethod)
	token.Claims = Claims{
		TenantList: config.TenantList,
		StandardClaims: jwt.StandardClaims{
			Issuer:   config.Issuer,
			Audience: []string{config.Audience},
		},
	}
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(config.PrivateKey))
	if err != nil {
		return "", fmt.Errorf("generate-token parse private key from PEM: %w", err)
	}
	tokenStr, err = token.SignedString(privateKey)
	if err != nil {
		return "", fmt.Errorf("generate-token sign: %w", err)
	}
	return
}
