# Promscale Mixin

This file contains instructions on how to use Promscale mixins.

We recommend monitoring your Promscale deployments and include the mixins in this package
as part of your alerting environment.

## How to use
Promscale alerts are defined [here](alerts/alerts.yaml). Copy the context into a file
say `promscale_alerts.yaml`.

## Configuring Prometheus
In the Prometheus configuration file, add `promscale_alerts.yaml` under `rule_files` like
```yaml
rule_files:
  - promscale_alerts.yaml
```
