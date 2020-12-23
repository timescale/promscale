# Alerting & Recording Rules

### Alerting Rules

Alerting rules are used to trigger alerts based on the violation of any condition(s). These alerts are fired to external services like slack, mails, etc by the [alert manager](https://prometheus.io/docs/alerting/latest/alertmanager/). Alerting rules are written in a YAML file and paths of these files are mentioned in the Prometheus configuration respectively. It is important to note that the evaluation of these conditional rules are performed at the Prometheus side. The newly formed series for alerting are stored in both Prometheus and Promscale.

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


### Recording Rules

The recording rules are for pre-calculating frequently used or computationally expensive queries. PromQL expressions defined in recording rules are evaluated at the Prometheus side. These evaluated rules form a new series that goes by the name of the record field's value and ingested in both Prometheus and Promscale.

Adding recording rules:

```
groups:
  - name: my-rules
    rules:
    - record: <RECORD_NAME>
      expr: <PROMQL_EXPR>
      
```

Load the above defined recording rule file to your prometheus configuration for evaluation.

```
rule_files:
  - "<recording-rules-file>"
```

More details on recording rules can be found [here](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/).

**Note**: We recommended setting `read_recent` to `true` in the [Prometheus remote_read configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_read) when using recording rules. This tells Prometheus to fetch data from Promscale when evaluating PromQL queries (including recording rules). If `read_recent` is disabled, **only** the data stored in Prometheus's local tsdb will be used when evaluating alerting/recording rules and thus be dependent on the retention period of Prometheus (not Promscale).