{{- define "telemetry-fabric.name" -}}
telemetry-fabric
{{- end -}}

{{- define "telemetry-fabric.controlPlaneName" -}}
{{ include "telemetry-fabric.name" . }}-control-plane
{{- end -}}

{{- define "telemetry-fabric.crawlerName" -}}
{{ include "telemetry-fabric.name" . }}-chain-crawler
{{- end -}}

{{- define "telemetry-fabric.streamProcessorName" -}}
{{ include "telemetry-fabric.name" . }}-stream-processor
{{- end -}}

{{- define "telemetry-fabric.redpandaName" -}}
{{ include "telemetry-fabric.name" . }}-redpanda
{{- end -}}

{{- define "telemetry-fabric.clickhouseName" -}}
{{ include "telemetry-fabric.name" . }}-clickhouse
{{- end -}}

{{- define "telemetry-fabric.prometheusName" -}}
{{ include "telemetry-fabric.name" . }}-prometheus
{{- end -}}

{{- define "telemetry-fabric.grafanaName" -}}
{{ include "telemetry-fabric.name" . }}-grafana
{{- end -}}

{{- define "telemetry-fabric.agentConfigMapName" -}}
{{ include "telemetry-fabric.name" . }}-agent-config
{{- end -}}

{{- define "telemetry-fabric.labels" -}}
app.kubernetes.io/name: {{ include "telemetry-fabric.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "telemetry-fabric.controlPlaneLabels" -}}
app.kubernetes.io/name: {{ include "telemetry-fabric.controlPlaneName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "telemetry-fabric.componentLabels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "telemetry-fabric.serviceAccountName" -}}
{{- if .Values.agent.serviceAccount.name -}}
{{ .Values.agent.serviceAccount.name }}
{{- else -}}
{{ include "telemetry-fabric.name" . }}
{{- end -}}
{{- end -}}

{{- define "telemetry-fabric.controlPlaneServiceAccountName" -}}
{{- if .Values.controlPlane.serviceAccount.name -}}
{{ .Values.controlPlane.serviceAccount.name }}
{{- else -}}
{{ include "telemetry-fabric.controlPlaneName" . }}
{{- end -}}
{{- end -}}

{{- define "telemetry-fabric.crawlerServiceAccountName" -}}
{{- if .Values.crawler.serviceAccount.name -}}
{{ .Values.crawler.serviceAccount.name }}
{{- else -}}
{{ include "telemetry-fabric.crawlerName" . }}
{{- end -}}
{{- end -}}

{{- define "telemetry-fabric.streamProcessorServiceAccountName" -}}
{{- if .Values.streamProcessor.serviceAccount.name -}}
{{ .Values.streamProcessor.serviceAccount.name }}
{{- else -}}
{{ include "telemetry-fabric.streamProcessorName" . }}
{{- end -}}
{{- end -}}
