{{- define "matchbook.labels" -}}
app.kubernetes.io/part-of: matchbook
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "matchbook.image" -}}
{{ .root.Values.image.registry }}/{{ .name }}:{{ .root.Values.image.tag }}
{{- end -}}

{{- define "matchbook.kafka" -}}
{{- if .Values.infra.enabled -}}{{ .Release.Name }}-kafka:9092{{- else -}}{{ .Values.external.kafkaBootstrap }}{{- end -}}
{{- end -}}

{{- define "matchbook.redis" -}}
{{- if .Values.infra.enabled -}}{{ .Release.Name }}-redis{{- else -}}{{ .Values.external.redisHost }}{{- end -}}
{{- end -}}

{{- define "matchbook.postgres" -}}
{{- if .Values.infra.enabled -}}{{ .Release.Name }}-postgres{{- else -}}{{ .Values.external.postgresHost }}{{- end -}}
{{- end -}}

{{- define "matchbook.commonEnv" -}}
- name: MATCHBOOK_SYMBOLS_FILE
  value: /config/symbols.json
{{- if .Values.otlpEndpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .Values.otlpEndpoint | quote }}
- name: OTEL_TRACES_ENABLED
  value: "true"
{{- end }}
{{- end -}}
