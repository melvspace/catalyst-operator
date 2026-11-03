{{- define "catalyst-operator.name" -}}
{{- printf "%s-catalyst-operator" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "catalyst-operator.labels" -}}
app.kubernetes.io/name: catalyst-operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
{{- define "catalyst-operator.selector" -}}
app.kubernetes.io/name: catalyst-operator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
