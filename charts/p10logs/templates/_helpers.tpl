{{/* vim: set filetype=mustache: */}}
{{- define "p10logs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "p10logs.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "p10logs.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "p10logs.labels" -}}
helm.sh/chart: {{ include "p10logs.chart" . }}
app.kubernetes.io/name: {{ include "p10logs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: p10logs
{{- end -}}

{{- define "p10logs.agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "p10logs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: agent
{{- end -}}

{{- define "p10logs.hub.selectorLabels" -}}
app.kubernetes.io/name: {{ include "p10logs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: hub
{{- end -}}

{{- define "p10logs.agent.fullname" -}}
{{ include "p10logs.fullname" . }}-agent
{{- end -}}

{{- define "p10logs.hub.fullname" -}}
{{ include "p10logs.fullname" . }}-hub
{{- end -}}

{{- define "p10logs.agent.serviceAccountName" -}}
{{- if .Values.agent.serviceAccount.create -}}
{{ default (include "p10logs.agent.fullname" .) .Values.agent.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.agent.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{- define "p10logs.hub.serviceAccountName" -}}
{{- if .Values.hub.serviceAccount.create -}}
{{ default (include "p10logs.hub.fullname" .) .Values.hub.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.hub.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{- define "p10logs.image" -}}
{{- $reg := .global.imageRegistry -}}
{{- $tag := default .appVersion .image.tag -}}
{{- if $reg -}}
{{- printf "%s/%s:%s" $reg .image.repository $tag -}}
{{- else -}}
{{- printf "%s:%s" .image.repository $tag -}}
{{- end -}}
{{- end -}}

{{/* Resolved hub URL for the agent */}}
{{- define "p10logs.agent.hubUrl" -}}
{{- if .Values.agent.hub.url -}}
{{ .Values.agent.hub.url }}
{{- else -}}
http://{{ include "p10logs.hub.fullname" . }}.{{ .Release.Namespace }}.svc:{{ .Values.hub.service.port }}
{{- end -}}
{{- end -}}

{{/* Name of the secret holding the ingest token (shared by agent + hub in standalone mode) */}}
{{- define "p10logs.ingestSecretName" -}}
{{- if .Values.hub.auth.existingSecret -}}
{{ .Values.hub.auth.existingSecret }}
{{- else if .Values.agent.hub.existingSecret -}}
{{ .Values.agent.hub.existingSecret }}
{{- else -}}
{{ include "p10logs.fullname" . }}-ingest
{{- end -}}
{{- end -}}

{{/* Stable auto-generated token: reuse existing secret value on upgrade */}}
{{- define "p10logs.ingestToken" -}}
{{- $existing := (lookup "v1" "Secret" .Release.Namespace (include "p10logs.ingestSecretName" .)) -}}
{{- if .Values.hub.auth.ingestToken -}}
{{ .Values.hub.auth.ingestToken }}
{{- else if .Values.agent.hub.token -}}
{{ .Values.agent.hub.token }}
{{- else if and $existing $existing.data (index $existing.data "token") -}}
{{ index $existing.data "token" | b64dec }}
{{- else -}}
{{ randAlphaNum 48 }}
{{- end -}}
{{- end -}}

{{- define "p10logs.uiPassword" -}}
{{- $name := printf "%s-ui" (include "p10logs.fullname" .) -}}
{{- $existing := (lookup "v1" "Secret" .Release.Namespace $name) -}}
{{- if .Values.hub.auth.ui.basic.password -}}
{{ .Values.hub.auth.ui.basic.password }}
{{- else if and $existing $existing.data (index $existing.data "password") -}}
{{ index $existing.data "password" | b64dec }}
{{- else -}}
{{ randAlphaNum 24 }}
{{- end -}}
{{- end -}}
