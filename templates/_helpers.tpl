{{/* Base name */}}
{{- define "pub-crawler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified app name */}}
{{- define "pub-crawler.fullname" -}}
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

{{/* Common labels */}}
{{- define "pub-crawler.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "pub-crawler.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Selector labels */}}
{{- define "pub-crawler.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pub-crawler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Image reference */}}
{{- define "pub-crawler.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{/* DATABASE_URL env entry (from the Secret) */}}
{{- define "pub-crawler.dbEnv" -}}
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "pub-crawler.fullname" . }}
      key: DATABASE_URL
{{- end -}}

{{/* REDIS_URL env entry (from the Secret) */}}
{{- define "pub-crawler.redisEnv" -}}
- name: REDIS_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "pub-crawler.fullname" . }}
      key: REDIS_URL
{{- end -}}

{{/* Private-key volume (Secret -> single file) */}}
{{- define "pub-crawler.keyVolume" -}}
- name: private-key
  secret:
    secretName: {{ include "pub-crawler.fullname" . }}
    items:
      - key: private.pem
        path: {{ base .Values.privateKey.mountPath }}
{{- end -}}

{{/* Private-key volume mount (mounts the dir containing the key) */}}
{{- define "pub-crawler.keyVolumeMount" -}}
- name: private-key
  mountPath: {{ dir .Values.privateKey.mountPath }}
  readOnly: true
{{- end -}}
