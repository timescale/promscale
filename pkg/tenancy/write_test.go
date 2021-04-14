// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package tenancy

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestVerifyAndApplyTenantLabel(t *testing.T) {
	// ----- Test with non-MT being false -----.
	conf := NewSelectiveTenancyConfig([]string{"tenant-a", "tenant-b"}, false)
	authr := NewWriteAuthorizer(conf)
	tenantName := "tenant-a"

	// Test with tenant name from header.
	lblsWithoutTenants := getlbls()
	newLbls, err := authr.VerifyAndApplyTenantLabel(tenantName, lblsWithoutTenants[0])
	require.NoError(t, err)

	expectedLbls := [][]prompb.Label{
		{
			{Name: model.MetricNameLabelName, Value: "firstMetric"},
			{Name: "foo", Value: "bar"},
			{Name: "common", Value: "tag"},
			{Name: "empty", Value: ""},
			{Name: TenantLabelKey, Value: "tenant-a"},
		},
	}
	require.Equal(t, expectedLbls[0], newLbls)

	// Test with tenant name from labels.
	lblsWithTenants := getlblsWithTenants()
	_, err = authr.VerifyAndApplyTenantLabel("", lblsWithTenants[0])
	require.NoError(t, err)

	// Test with tenant name from labels but with empty value, but having the __tenant__ key.
	_, err = authr.VerifyAndApplyTenantLabel("", lblsWithTenants[2])
	require.Error(t, err)

	// Test with no tenant names (non-MT write).
	_, err = authr.VerifyAndApplyTenantLabel("", lblsWithoutTenants[0])
	require.Error(t, err)

	// ----- Test with allow non-MT being true -----.
	conf = NewSelectiveTenancyConfig([]string{"tenant-a", "tenant-b"}, true)
	authr = NewWriteAuthorizer(conf)

	// Test with tenant name from header.
	newLbls, err = authr.VerifyAndApplyTenantLabel(tenantName, lblsWithoutTenants[0])
	require.NoError(t, err)
	require.Equal(t, expectedLbls[0], newLbls)

	// Test with tenant name from labels.
	_, err = authr.VerifyAndApplyTenantLabel("", lblsWithTenants[0])
	require.NoError(t, err)

	// Test with no tenant names (non-MT write).
	expectedLbls = [][]prompb.Label{
		{
			{Name: model.MetricNameLabelName, Value: "firstMetric"},
			{Name: "foo", Value: "bar"},
			{Name: "common", Value: "tag"},
			{Name: "empty", Value: ""},
		},
	}
	newLbls, err = authr.VerifyAndApplyTenantLabel("", lblsWithoutTenants[0])
	require.NoError(t, err)
	require.Equal(t, expectedLbls[0], newLbls)
}

func TestLabelsWithoutTenants(t *testing.T) {
	var (
		lblsArr    = getlbls()
		tenantName = "tenant-a" // Assume from TENANT header.
	)

	// With valid tenants.
	conf := NewSelectiveTenancyConfig([]string{"tenant-a", "tenant-b"}, false)
	authr := NewWriteAuthorizer(conf)
	require.NoError(t, authr.isAuthorized(tenantName))

	for _, lbls := range lblsArr {
		lb, err := authr.VerifyAndApplyTenantLabel(tenantName, lbls)
		require.NoError(t, err)
		require.True(t, containsAppliedTenantLabel(lb))
	}

	// Should not verify.
	conf = NewSelectiveTenancyConfig([]string{"tenant-b"}, false)
	authr = NewWriteAuthorizer(conf)
	err := authr.isAuthorized(tenantName)
	if expected := fmt.Sprintf("authorization error for tenant tenant-a: %s", ErrUnauthorizedTenant.Error()); err.Error() != expected {
		require.Fail(t, "error does not match", err)
	}

	// Empty tenant write.
	conf = NewAllowAllTenantsConfig(false)
	authr = NewWriteAuthorizer(conf)
	require.NoError(t, authr.isAuthorized(tenantName))

	for _, lbls := range lblsArr {
		lb, err := authr.VerifyAndApplyTenantLabel(tenantName, lbls)
		require.NoError(t, err)
		require.True(t, containsAppliedTenantLabel(lb))
	}

	conf = NewAllowAllTenantsConfig(true)
	authr = NewWriteAuthorizer(conf)
	require.NoError(t, authr.isAuthorized(""))

	for _, lbls := range lblsArr {
		lb, err := authr.VerifyAndApplyTenantLabel("", lbls)
		require.NoError(t, err)
		require.True(t, !containsAppliedTenantLabel(lb))
	}
}

func TestLabelsWithTenants(t *testing.T) {
	var (
		lblsArrWithTenants = getlblsWithTenants()
		tenantName         = "tenant-a" // Assume from TENANT header.
		tenantRandom       = "tenant-random"
		conf               = NewSelectiveTenancyConfig([]string{"tenant-a"}, false)
		authr              = NewWriteAuthorizer(conf)
	)
	require.NoError(t, authr.isAuthorized(tenantName))
	// Tenant label value and tenant header value same.
	lb, err := authr.VerifyAndApplyTenantLabel(tenantName, lblsArrWithTenants[0])
	require.NoError(t, err)
	require.True(t, containsAppliedTenantLabel(lb))

	// Tenant label value and tenant header are different.
	_, err = authr.VerifyAndApplyTenantLabel(tenantRandom, lblsArrWithTenants[0])
	if err.Error() != "authorization error for tenant tenant-random: unauthorized or invalid tenant" {
		require.Fail(t, "error does not match", err)
	}

	// Empty tenant write.
	conf = NewAllowAllTenantsConfig(false)
	authr = NewWriteAuthorizer(conf)
	require.NoError(t, authr.isAuthorized(tenantName))

	for i, lbls := range lblsArrWithTenants {
		if i < 2 {
			lb, err := authr.VerifyAndApplyTenantLabel(tenantName, lbls)
			require.NoError(t, err)
			require.True(t, containsAppliedTenantLabel(lb))
		}
	}

	conf = NewAllowAllTenantsConfig(true)
	authr = NewWriteAuthorizer(conf)
	require.NoError(t, authr.isAuthorized(""))

	for i, lbls := range lblsArrWithTenants {
		if i < 2 {
			lb, err := authr.VerifyAndApplyTenantLabel(tenantName, lbls)
			require.NoError(t, err)
			require.True(t, containsAppliedTenantLabel(lb))
		}
	}
}

func containsAppliedTenantLabel(lbls []prompb.Label) bool {
	for _, lbl := range lbls {
		if lbl.Name == TenantLabelKey {
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

func getlblsWithTenants() [][]prompb.Label {
	return [][]prompb.Label{
		{
			{Name: TenantLabelKey, Value: "tenant-a"},
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
		{
			{Name: TenantLabelKey, Value: ""}, // Invalid label value.
			{Name: model.MetricNameLabelName, Value: "thirdMetric"},
			{Name: "foo", Value: "baz"},
			{Name: "common", Value: "tag"},
		},
	}
}
