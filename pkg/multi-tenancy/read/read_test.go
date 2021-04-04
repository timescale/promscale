package read

import (
	"strings"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
)

func TestMultiTenancyPlainRead(t *testing.T) {
	var (
		matchers = []*labels.Matcher{
			{Type: labels.MatchEqual, Name: "__name__", Value: "metric"},
			{Type: labels.MatchEqual, Name: "job", Value: "promscale-multi-tenancy"},
			{Type: labels.MatchEqual, Name: "instance", Value: "localhost:9201"},
		}
		token = ""
	)

	// With valid tenants.
	conf := &config.Config{
		AuthType:     config.Allow,
		ValidTenants: []string{"tenant-a", "tenant-b"},
	}
	err := conf.Validate()
	require.NoError(t, err)
	authr, err := NewPlainReadAuthorizer(conf)
	require.NoError(t, err)
	require.True(t, authr.IsValid(token))
	newMatchers := authr.ApplySafetyMatcher(matchers)
	require.True(t, containsSafetyMatcher(newMatchers))

	// Without valid tenants.
	conf = &config.Config{
		AuthType: config.Allow,
	}
	err = conf.Validate()
	require.NoError(t, err)
	authr, err = NewPlainReadAuthorizer(conf)
	require.NoError(t, err)
	require.True(t, authr.IsValid(token))
	newMatchers = authr.ApplySafetyMatcher(matchers)
	require.False(t, containsSafetyMatcher(newMatchers))
}

func TestMultiTenancyTokenRead(t *testing.T) {
	var (
		matchers = []*labels.Matcher{
			{Type: labels.MatchEqual, Name: "__name__", Value: "metric"},
			{Type: labels.MatchEqual, Name: "job", Value: "promscale-multi-tenancy"},
			{Type: labels.MatchEqual, Name: "instance", Value: "localhost:9201"},
		}
		token        = "token"
		invalidToken = "invalidToken"
	)

	// With valid tenants.
	conf := &config.Config{
		AuthType:     config.BearerToken,
		BearerToken:  token,
		ValidTenants: []string{"tenant-a", "tenant-b"},
	}
	err := conf.Validate()
	require.NoError(t, err)
	authr, err := NewBearerTokenReadAuthorizer(conf)
	require.NoError(t, err)
	require.True(t, authr.IsValid(token))
	newMatchers := authr.ApplySafetyMatcher(matchers)
	require.True(t, containsSafetyMatcher(newMatchers))

	// Invalid token.
	conf = &config.Config{
		AuthType:     config.BearerToken,
		BearerToken:  invalidToken,
		ValidTenants: []string{"tenant-a", "tenant-b"},
	}
	err = conf.Validate()
	require.NoError(t, err)
	authr, err = NewBearerTokenReadAuthorizer(conf)
	require.NoError(t, err)
	require.False(t, authr.IsValid(token))
}

func containsSafetyMatcher(ms []*labels.Matcher) bool {
	for _, m := range ms {
		if m.Name == config.TenantLabelKey {
			if strings.Contains(m.Value, regexOR) {
				// This works only when more than 1 valid tenants are configured,
				// which satisfies our test.
				return true
			}
		}
	}
	return false
}
