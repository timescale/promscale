# Promscale Mixin

*This is a work in progress. We aim for it to become a good role model for alerts and dashboards eventually, but it is not quite there yet.*

The Promscale Mixin is a set of configurable, reusable, and extensible alerts and dashboards based on the metrics exported by the Promscale. The mixin creates recording and alerting rules for Prometheus and suitable dashboard descriptions for Grafana.

We recommend monitoring your Promscale deployments and include the mixins in this package
as part of your alerting environment.

## Using pre-built mixin

### How to use

Promscale alerts are defined [here](alerts/alerts.yaml). Copy the context into a file
say `promscale_alerts.yaml`.

### Configuring Prometheus

In the Prometheus configuration file, add `promscale_alerts.yaml` under `rule_files` like

```yaml
rule_files:
  - promscale_alerts.yaml
```

## Building mixin

To build it, you need to have mixtool and jsonnetfmt installed. If you have a working Go development environment, it's easiest to run the following:

```console
$ go install github.com/monitoring-mixins/mixtool/cmd/mixtool@latest
$ go install github.com/google/go-jsonnet/cmd/jsonnetfmt@latest
```

You can then build the Prometheus rules files alerts.yaml and rules.yaml and a directory dashboard_out with the JSON dashboard files for Grafana:

```
$ make build
```

For more advanced uses of mixins, see https://github.com/monitoring-mixins/docs.
