package claims

import "github.com/dgrijalva/jwt-go/v4"

type Claims struct {
	Scope string `json:"scope"`
	Tenant string `json:"https://allowed-tenants"`
	jwt.StandardClaims
}
