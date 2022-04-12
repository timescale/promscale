{
  prometheusAlerts+:: {
    groups+: std.parseYaml(importstr 'alerts.yaml').groups,
  },
}
