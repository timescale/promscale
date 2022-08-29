// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package tenancy

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestMultiTenancyRead(t *testing.T) {
	var (
		matchers = []*labels.Matcher{
			{Type: labels.MatchEqual, Name: "__name__", Value: "metric"},
			{Type: labels.MatchEqual, Name: "job", Value: "promscale-tenancy"},
			{Type: labels.MatchEqual, Name: "instance", Value: "localhost:9201"},
		}
	)

	// With valid tenants.
	conf := NewSelectiveTenancyConfig([]string{"tenant-a", "tenant-b"}, false, true)
	authr, err := NewReadAuthorizer(conf)
	require.NoError(t, err)
	newMatchers := authr.AppendTenantMatcher(matchers)
	safetyMatcher, present := getSafetyMatcher(newMatchers)
	require.True(t, present)
	require.Equal(t, "tenant-a|tenant-b", safetyMatcher)

	// Without valid tenants.
	conf = NewAllowAllTenantsConfig(false)
	authr, err = NewReadAuthorizer(conf)
	require.NoError(t, err)
	newMatchers = authr.AppendTenantMatcher(matchers)
	safetyMatcher, present = getSafetyMatcher(newMatchers)
	require.True(t, present)
	require.Equal(t, "", safetyMatcher)

	// Non-tenants.
	// With valid tenants.
	conf = NewSelectiveTenancyConfig([]string{"tenant-a", "tenant-b"}, true, true)
	authr, err = NewReadAuthorizer(conf)
	require.NoError(t, err)
	newMatchers = authr.AppendTenantMatcher(matchers)
	safetyMatcher, present = getSafetyMatcher(newMatchers)
	require.True(t, present)
	require.Equal(t, "tenant-a|tenant-b|^$", safetyMatcher)

	// Without valid tenants.
	conf = NewAllowAllTenantsConfig(true)
	authr, err = NewReadAuthorizer(conf)
	require.NoError(t, err)
	newMatchers = authr.AppendTenantMatcher(matchers)
	_, present = getSafetyMatcher(newMatchers)
	require.False(t, present)
}

func getSafetyMatcher(ms []*labels.Matcher) (string, bool) {
	for _, m := range ms {
		if m.Name == TenantLabelKey {
			return m.Value, true
		}
	}
	return "", false
}
