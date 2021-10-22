# Alerting

To configure alerts we recommend using [Prometheus Alert Manager](https://prometheus.io/docs/alerting/latest/alertmanager/). Promscale does not provide its own alerting engine currently (future roadmap item).

Alerting rules are used to trigger alerts based on the violation of any condition(s). These alerts are fired to external services like slack, mails, etc by the alert manager. Alerting rules are written in a YAML file and paths of these files are mentioned in the Prometheus configuration respectively. It is important to note that the evaluation of these conditional rules are performed at the Prometheus side. The newly formed series for alerting are stored in both Prometheus and Promscale.

```
groups:
  - name: <alert-group-name>
    rules:
    - alert: <alert-name>
      expr: <promql_expression>
      for: <time-interval for how long this to happen to happen to fire an alert>
      labels:
        <key>:<value>
      annotations:
        summary: <text>
        description: <description on alert>
```

Load the above defined alert rule file to your prometheus configuration for evaluation.

```
rule_files:
  - "<alert-rules-file>"
```

Configure the alert manager in prometheus configuration.

```
alerting:
  alertmanagers:
  - static_configs:
    - targets:
      - localhost:9093
```

More details on alerting rules can be found [here](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/).
