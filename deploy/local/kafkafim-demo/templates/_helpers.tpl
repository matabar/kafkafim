{{- define "kafkafim-demo.labels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/part-of: kafkafim-demo
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end -}}

{{/*
A Secret with `username` and a random `password`. The password is generated on install
and kept on upgrade (read back with lookup), so pods that already use it stay valid.
`helm template` has no cluster to look up, so it renders a fresh password every time.
*/}}
{{- define "kafkafim-demo.secret" -}}
{{- $existing := lookup "v1" "Secret" .root.Release.Namespace .name -}}
apiVersion: v1
kind: Secret
metadata:
  name: {{ .name }}
  labels:
    {{- include "kafkafim-demo.labels" (dict "root" .root "name" .name) | nindent 4 }}
type: Opaque
data:
  username: {{ .username | b64enc | quote }}
  {{- if and $existing $existing.data $existing.data.password }}
  password: {{ $existing.data.password | quote }}
  {{- else }}
  password: {{ randAlphaNum 32 | b64enc | quote }}
  {{- end }}
{{- end -}}
