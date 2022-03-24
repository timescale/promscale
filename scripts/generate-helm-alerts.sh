#!/bin/bash

set -euo pipefail

alerts="docs/mixin/alerts/alerts.yaml"
file="deploy/helm-chart/templates/prometheus-rule.yaml"

cat << EOF > "$file"
{{- /* This file is generated using a script located at scripts/generate-helm-alerts.sh */}}
{{ if .Values.serviceMonitor.enabled }}
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: {{ include "promscale.fullname" . }}-rules
  namespace: {{ template "promscale.namespace" . }}
  labels:
    app: {{ template "promscale.fullname" . }}
    chart: {{ template "promscale.chart" . }}
    release: {{ .Release.Name }}
    heritage: {{ .Release.Service }}
    app.kubernetes.io/name: "promscale-connector"
    app.kubernetes.io/version: {{ .Chart.AppVersion }}
    app.kubernetes.io/part-of: "promscale-connector"
    app.kubernetes.io/component: "connector"
spec:
{{\`
$(cat "$alerts" | sed -e 's/^/  /')
\`}}
{{- end }}
EOF
