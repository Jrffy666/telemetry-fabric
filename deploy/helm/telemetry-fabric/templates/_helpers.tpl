{{- define "telemetry-fabric.name" -}}
telemetry-fabric
{{- end -}}

{{- define "telemetry-fabric.labels" -}}
app.kubernetes.io/name: {{ include "telemetry-fabric.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
