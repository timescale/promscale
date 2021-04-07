package read

import (
	"strings"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
)

func TestMultiTenancyRead(t *testing.T) {
	var (
		matchers = []*labels.Matcher{
			{Type: labels.MatchEqual, Name: "__name__", Value: "metric"},
			{Type: labels.MatchEqual, Name: "job", Value: "promscale-multi-tenancy"},
			{Type: labels.MatchEqual, Name: "instance", Value: "localhost:9201"},
		}
	)

	// With valid tenants.
	conf := config.NewSelectiveTenancyConfig([]string{"tenant-a", "tenant-b"})
	authr, err := NewAuthorizer(conf)
	require.NoError(t, err)
	newMatchers := authr.ApplySafetyMatcher(matchers)
	require.True(t, containsSafetyMatcher(newMatchers))

	// Without valid tenants.
	conf = config.NewOpenTenancyConfig()
	authr, err = NewAuthorizer(conf)
	require.NoError(t, err)
	newMatchers = authr.ApplySafetyMatcher(matchers)
	require.False(t, containsSafetyMatcher(newMatchers))
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
