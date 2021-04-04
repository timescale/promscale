package write

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestMultiTenancyPlainWrite(t *testing.T) {
	var (
		lblsArr    = getlbls()
		token      = ""
		tenantName = "tenant-a" // Assume from TENANT header.
	)

	// With valid tenants.
	conf := &config.Config{
		AuthType:     config.Allow,
		ValidTenants: []string{"tenant-a", "tenant-b"},
	}
	err := conf.Validate()
	require.NoError(t, err)

	authr := NewPlainWriteAuthorizer(conf)
	require.True(t, authr.IsAuthorized(token, tenantName))
	for _, lbls := range lblsArr {
		lb, ok := authr.VerifyAndApplyTenantLabel(tenantName, lbls)
		require.True(t, ok)
		require.True(t, containsAppliedTenantLabel(lb))
	}

	// Should not verify.
	conf = &config.Config{
		AuthType:     config.Allow,
		ValidTenants: []string{"tenant-b"},
	}
	err = conf.Validate()
	require.NoError(t, err)

	authr = NewPlainWriteAuthorizer(conf)
	require.False(t, authr.IsAuthorized(token, tenantName))
}

func TestMultiTenancyTokenWrite(t *testing.T) {
	var (
		lblsArr      = getlbls()
		token        = "token"
		invalidToken = "invalidToken"
		tenantName   = "tenant-a" // Assume from TENANT header.
	)

	// With valid tenants.
	conf := &config.Config{
		AuthType:     config.BearerToken,
		BearerToken:  token,
		ValidTenants: []string{"tenant-a", "tenant-b"},
	}
	err := conf.Validate()
	require.NoError(t, err)

	authr := NewBearerTokenWriteAuthorizer(conf)
	require.True(t, authr.IsAuthorized(token, tenantName))
	for _, lbls := range lblsArr {
		lb, ok := authr.VerifyAndApplyTenantLabel(tenantName, lbls)
		require.True(t, ok)
		require.True(t, containsAppliedTenantLabel(lb))
	}

	// Should not verify.
	conf = &config.Config{
		AuthType:     config.BearerToken,
		BearerToken:  token,
		ValidTenants: []string{"tenant-a", "tenant-b"},
	}
	err = conf.Validate()
	require.NoError(t, err)

	authr = NewBearerTokenWriteAuthorizer(conf)
	require.False(t, authr.IsAuthorized(invalidToken, tenantName))
}

func containsAppliedTenantLabel(lbls []prompb.Label) bool {
	for _, lbl := range lbls {
		if lbl.Name == config.TenantLabelKey {
			return true
		}
	}
	return false
}

func getlbls() [][]prompb.Label {
	return [][]prompb.Label{
		{
			{Name: model.MetricNameLabelName, Value: "firstMetric"},
			{Name: "foo", Value: "bar"},
			{Name: "common", Value: "tag"},
			{Name: "empty", Value: ""},
		},
		{
			{Name: model.MetricNameLabelName, Value: "secondMetric"},
			{Name: "foo", Value: "baz"},
			{Name: "common", Value: "tag"},
		},
	}
}
