package generator

import (
	"crypto/rsa"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/dgrijalva/jwt-go/v4"
	jwt_claims "github.com/timescale/promscale/pkg/jwt/claims"
)

type Config struct {
	PrivateKey     string
	PrivateKeyPath string
	Password       string
	Payload
}

type Payload struct {
	Issuer   string
	Audience string
	IssuedAt time.Time
	ExpireAt time.Time
}

var signingMethod = jwt.GetSigningMethod("RS256")

// Validate validates the config. It must be called before calling any other function.
func (c *Config) Validate() error {
	if c.Issuer == "" {
		return fmt.Errorf("issuer cannot be empty string")
	}
	if c.Audience == "" {
		return fmt.Errorf("audience cannot be empty string")
	}
	if c.PrivateKey == "" && c.PrivateKeyPath == "" {
		return fmt.Errorf("both private key and private key path cannot be empty string")
	} else if c.PrivateKey != "" && c.PrivateKeyPath != "" {
		return fmt.Errorf("private key and private key path are mutually exclusive. Please provide any one of them")
	}
	return nil
}

// FillKeyFromFiles fills the key from the supplied file path.
func (c *Config) FillKeyFromFiles() error {
	if c.PrivateKeyPath == "" {
		return fmt.Errorf("private key path is empty")
	}
	data, err := ioutil.ReadFile(c.PrivateKeyPath)
	if err != nil {
		return fmt.Errorf("fill key from file: %w", err)
	}
	c.PrivateKey = string(data)
	return nil
}

// GenerateToken generates the JWT token based on the config. It sets the IssuedAt time to time.Now() in case it is unset.
func GenerateToken(config *Config) (tokenStr string, err error) {
	token := jwt.New(signingMethod)
	token.Claims = jwt_claims.Claims{
		StandardClaims: jwt.StandardClaims{
			Issuer:   config.Issuer,
			Audience: []string{config.Audience},
		},
	}
	var privateKey *rsa.PrivateKey
	if config.Password == "" {
		privateKey, err = jwt.ParseRSAPrivateKeyFromPEM([]byte(config.PrivateKey))
		if err != nil {
			return "", fmt.Errorf("generate-token parse private key from PEM: %w", err)
		}
	} else {
		privateKey, err = jwt.ParseRSAPrivateKeyFromPEMWithPassword([]byte(config.PrivateKey), config.Password)
		if err != nil {
			return "", fmt.Errorf("generate-tooken parse private key from PEM with password: %w", err)
		}
	}
	tokenStr, err = token.SignedString(privateKey)
	if err != nil {
		return "", fmt.Errorf("generate-token sign: %w", err)
	}
	return
}
