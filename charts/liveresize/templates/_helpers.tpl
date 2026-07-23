{{- define "liveresize.fullname" -}}{{ .Release.Name }}{{- end -}}
{{- define "liveresize.labels" -}}
app.kubernetes.io/name: liveresize
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
