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
{{ include "promscale-helm.labels" . | indent 4 }}
spec:
{{\`
$(cat "$alerts" | sed -e 's/^/  /')
\`}}
{{- end }}
EOF
