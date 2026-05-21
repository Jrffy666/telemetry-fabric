{{- define "telemetry-fabric.name" -}}
telemetry-fabric
{{- end -}}

{{- define "telemetry-fabric.controlPlaneName" -}}
{{ include "telemetry-fabric.name" . }}-control-plane
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
