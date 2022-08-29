package tenancy

import (
	"flag"
	"fmt"
	"strings"
)

const AllowAllTenants = "allow-all"

type Config struct {
	SkipTenantValidation        bool
	EnableMultiTenancy          bool
	AllowNonMTWrites            bool
	UseExperimentalLabelQueries bool
	ValidTenantsStr             string
	ValidTenantsList            []string
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) {
	fs.BoolVar(&cfg.EnableMultiTenancy, "metrics.multi-tenancy", false, "Use multi-tenancy mode in Promscale.")
	fs.BoolVar(&cfg.AllowNonMTWrites, "metrics.multi-tenancy.allow-non-tenants", false, "Allow Promscale to ingest/query all tenants as well as non-tenants. "+
		"By setting this to true, Promscale will ingest data from non multi-tenant Prometheus instances as well. "+
		"If this is false, only multi-tenants (tenants listed in 'multi-tenancy-valid-tenants') are allowed for ingesting and querying data.")
	fs.StringVar(&cfg.ValidTenantsStr, "metrics.multi-tenancy.valid-tenants", AllowAllTenants, "Sets valid tenants that are allowed to be ingested/queried from Promscale. "+
		fmt.Sprintf("This can be set as: '%s' (default) or a comma separated tenant names. '%s' makes Promscale ingest or query any tenant from itself. ", AllowAllTenants, AllowAllTenants)+
		"A comma separated list will indicate only those tenants that are authorized for operations from Promscale.")
	fs.BoolVar(&cfg.UseExperimentalLabelQueries, "metrics.multi-tenancy.experimental.label-queries", true, "[EXPERIMENTAL] Use label queries "+
		"that returns labels of authorized tenants only. This may affect system performance while running PromQL queries. "+
		"By default this is enabled in -metrics.multi-tenancy mode.")
}

func Validate(cfg *Config) error {
	if !cfg.EnableMultiTenancy {
		return nil
	}
	if cfg.ValidTenantsStr == AllowAllTenants {
		cfg.SkipTenantValidation = true
		return nil
	} else if cfg.ValidTenantsStr == "" {
		return fmt.Errorf("'multi-tenancy-valid-tenants' cannot be empty")
	}
	l, err := removeEmptyTenants(strings.Split(cfg.ValidTenantsStr, ","))
	if err != nil {
		return err
	}
	cfg.ValidTenantsList = l
	return nil
}

// removeEmptyTenants protects against corner cases, when the user enters comma separated tenants
// such that there is a trailing comma towards the end.
func removeEmptyTenants(t []string) (tenants []string, err error) {
	for i := 0; i < len(t); i++ {
		if len(t[i]) == 0 {
			continue
		}
		if t[i] == AllowAllTenants {
			return tenants, fmt.Errorf("'%s' should not be present with valid tenant names", AllowAllTenants)
		}
		tenants = append(tenants, t[i])
	}
	return
}
