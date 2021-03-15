package auth0_world

import (
	"context"
	"log"

	"golang.org/x/oauth2"

	oidc "github.com/coreos/go-oidc"
)

type Authenticator struct {
	Provider *oidc.Provider
	Config   oauth2.Config
	Ctx      context.Context
}

func NewAuthenticator() (*Authenticator, error) {
	ctx := context.Background()

	provider, err := oidc.NewProvider(ctx, "https://dev-cfooy6iu.us.auth0.com/")
	if err != nil {
		log.Printf("failed to get provider: %v", err)
		return nil, err
	}

	conf := oauth2.Config{
		ClientID:     "EnxfEwcMPYB3KiGdw5I3b6RWDnZl7tik",
		ClientSecret: "s6ETfd85Ka2qNnKh-Mtlmdm0ck4RlszL7HsgF-y71BVhQuDIqjxFDovAZ30lduDX",
		RedirectURL:  "http://localhost:3000/callback",
		Endpoint: 	  provider.Endpoint(),
		Scopes:       []string{oidc.ScopeOpenID, "profile"},
	}

	return &Authenticator{
		Provider: provider,
		Config:   conf,
		Ctx:      ctx,
	}, nil
}
