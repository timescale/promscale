package tenancy

import (
	"flag"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/peterbourgon/ff/v3"
	"github.com/stretchr/testify/require"
)

func TestParseFlags(t *testing.T) {
	config := fullyParse(t, []string{"-metrics.multi-tenancy", fmt.Sprintf("-metrics.multi-tenancy.valid-tenants=%s", AllowAllTenants)})
	require.Equal(t, Config{EnableMultiTenancy: true, ValidTenantsStr: AllowAllTenants, SkipTenantValidation: true, UseExperimentalLabelQueries: true}, config)

	config = fullyParse(t, []string{"-metrics.multi-tenancy", "-metrics.multi-tenancy.valid-tenants=tenant-a,tenant-b,tenant-c"})
	require.Equal(t, Config{EnableMultiTenancy: true, ValidTenantsStr: "tenant-a,tenant-b,tenant-c", ValidTenantsList: []string{"tenant-a", "tenant-b", "tenant-c"}, UseExperimentalLabelQueries: true}, config)

	config = fullyParse(t, []string{fmt.Sprintf("-metrics.multi-tenancy.valid-tenants=%s", AllowAllTenants)})
	require.Equal(t, Config{ValidTenantsStr: AllowAllTenants, SkipTenantValidation: false, UseExperimentalLabelQueries: true}, config)

	config = fullyParse(t, []string{fmt.Sprintf("-metrics.multi-tenancy.valid-tenants=%s", AllowAllTenants), "-metrics.multi-tenancy.experimental.label-queries=false"})
	require.Equal(t, Config{ValidTenantsStr: AllowAllTenants, SkipTenantValidation: false, UseExperimentalLabelQueries: false}, config)
}

func fullyParse(t *testing.T, args []string) Config {
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	config := &Config{}
	ParseFlags(fs, config)
	require.NoError(t, ff.Parse(fs, args))
	require.NoError(t, Validate(config))
	return *config
}

func TestRemoveEmptyTenants(t *testing.T) {
	tcs := []struct {
		name          string
		tenants       []string
		outputTenants []string
		fail          bool
	}{
		{
			name:          "normal",
			tenants:       []string{"tenant-a", "tenant-b"},
			outputTenants: []string{"tenant-a", "tenant-b"},
		},
		{
			name:          "empty at end",
			tenants:       []string{"tenant-a", "tenant-b", ""}, // Happens when tenant-a, tenant-b, (Note the trailing comma)
			outputTenants: []string{"tenant-a", "tenant-b"},
		},
		{
			name:          "fail",
			tenants:       []string{"tenant-a", "tenant-b", AllowAllTenants},
			outputTenants: []string{"tenant-a", "tenant-b"},
			fail:          true,
		},
	}
	for _, tc := range tcs {
		output, err := removeEmptyTenants(tc.tenants)
		require.Equal(t, tc.outputTenants, output, tc.name)
		if tc.fail {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}
