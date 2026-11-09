{{/*
Chart name, overridable.
*/}}
{{- define "kafkafim.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Release-wide name prefix. A release named "kafkafim" (the documented convention:
one release per Kubernetes cluster) yields "kafkafim", so a group's resources are
"kafkafim-<group>".
*/}}
{{- define "kafkafim.fullname" -}}
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

{{/*
Name of a group's resources. Not truncated: kafkafim.validate fails instead, since
truncation could make two groups collide.
Usage: include "kafkafim.groupName" (dict "root" $ "group" $g)
*/}}
{{- define "kafkafim.groupName" -}}
{{- printf "%s-%s" (include "kafkafim.fullname" .root) .group -}}
{{- end -}}

{{- define "kafkafim.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Release-wide selector labels (every kafkafim pod of this release).
*/}}
{{- define "kafkafim.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kafkafim.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "kafkafim.labels" -}}
helm.sh/chart: {{ include "kafkafim.chart" . }}
{{ include "kafkafim.selectorLabels" . }}
{{- with include "kafkafim.versionLabel" . }}
app.kubernetes.io/version: {{ . | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: kafkafim
{{- end -}}

{{/*
Per-group selector labels.
Usage: include "kafkafim.groupSelectorLabels" (dict "root" $ "group" $g)
*/}}
{{- define "kafkafim.groupSelectorLabels" -}}
{{ include "kafkafim.selectorLabels" .root }}
app.kubernetes.io/component: {{ .group }}
{{- end -}}

{{- define "kafkafim.groupLabels" -}}
{{ include "kafkafim.labels" .root }}
app.kubernetes.io/component: {{ .group }}
{{- end -}}

{{- define "kafkafim.serviceAccountName" -}}
{{- include "kafkafim.fullname" . -}}
{{- end -}}

{{- define "kafkafim.imageTag" -}}
{{- .Values.image.tag | default .Chart.AppVersion | toString | trim -}}
{{- end -}}

{{/*
app.kubernetes.io/version: the image tag (or appVersion), never the digest, made a
valid label value: any character outside [A-Za-z0-9_.-] (such as semver's "+")
becomes "_", at most 63 characters, starting and ending with an alphanumeric
character. Empty, and the label left out, when there is no image.tag and no
appVersion (a digest-pinned image) or the tag has no alphanumeric character.
*/}}
{{- define "kafkafim.versionLabel" -}}
{{- $v := regexReplaceAll "[^A-Za-z0-9_.-]" (include "kafkafim.imageTag" .) "_" | trunc 63 -}}
{{- regexReplaceAll "^[^A-Za-z0-9]+|[^A-Za-z0-9]+$" $v "" -}}
{{- end -}}

{{/*
Full image reference. image.registry is first-class so the air-gapped registry is a
one-line override.
  no digest:            <registry>/<repository>:<image.tag or appVersion>
  digest, no image.tag: <registry>/<repository>@<digest>
  digest and image.tag: <registry>/<repository>:<image.tag>@<digest>
With a digest the runtime pulls by digest; a tag next to it is only informational.
*/}}
{{- define "kafkafim.image" -}}
{{- $repo := .Values.image.repository -}}
{{- if .Values.image.registry -}}
{{- $repo = printf "%s/%s" (trimSuffix "/" .Values.image.registry) $repo -}}
{{- end -}}
{{- $digest := .Values.image.digest | default "" | toString | trim -}}
{{- $tag := .Values.image.tag | default "" | toString | trim -}}
{{- if and $digest $tag -}}
{{- printf "%s:%s@%s" $repo $tag $digest -}}
{{- else if $digest -}}
{{- printf "%s@%s" $repo $digest -}}
{{- else -}}
{{- printf "%s:%s" $repo (include "kafkafim.imageTag" .) -}}
{{- end -}}
{{- end -}}

{{/*
Normalised secret mount path (no trailing slash, "." and ".." resolved).
*/}}
{{- define "kafkafim.secretMountPath" -}}
{{- clean . -}}
{{- end -}}

{{/*
Where otel.secretHeaders is mounted (JSON list): configtree turns each file into
management.otlp.metrics.export.headers.<header> and
management.opentelemetry.tracing.export.otlp.headers.<header> (Spring Boot 4 names).
Everything under /etc/kafkafim/secrets/management is reserved for these mounts.
Keep in sync with OtlpExportHeadersIntegrationTest.SECRET_HEADER_DIRS.
*/}}
{{- define "kafkafim.otelSecretHeaderMountPaths" -}}
{{- list "/etc/kafkafim/secrets/management/otlp/metrics/export/headers" "/etc/kafkafim/secrets/management/opentelemetry/tracing/export/otlp/headers" | toJson -}}
{{- end -}}

{{/*
A group's replicas (default 2) and HPA minReplicas (default 2), as strings.
Usage: include "kafkafim.replicas" $group
*/}}
{{- define "kafkafim.replicas" -}}
{{- if hasKey . "replicas" -}}{{ int .replicas }}{{- else -}}2{{- end -}}
{{- end -}}

{{- define "kafkafim.minReplicas" -}}
{{- $a := .autoscaling | default dict -}}
{{- if hasKey $a "minReplicas" -}}{{ int $a.minReplicas }}{{- else -}}2{{- end -}}
{{- end -}}

{{- define "kafkafim.autoscalingEnabled" -}}
{{- $a := .autoscaling | default dict -}}
{{- if $a.enabled -}}true{{- end -}}
{{- end -}}

{{/*
Template-time validation (docs/HELM.md "Template-time validation"). Included at the
top of every template so a bad values file fails whichever template renders first.
values.schema.json covers the shape; these are the cross-field rules a schema
cannot express.
*/}}
{{- define "kafkafim.validate" -}}
{{- if not (or (include "kafkafim.imageTag" .) (.Values.image.digest | default "" | toString | trim)) -}}
{{- fail "image.tag and image.digest are empty and the chart has no appVersion; set image.tag or image.digest" -}}
{{- end -}}
{{- if contains "@" (.Values.image.tag | default "" | toString) -}}
{{- fail (printf "image.tag %q contains \"@\"; put the tag in image.tag and the digest (sha256:<64 hex>) in image.digest" (.Values.image.tag | toString)) -}}
{{- end -}}
{{- $pipelines := .Values.config.kafkafim.pipelines | default dict -}}
{{- $owner := dict -}}
{{- $fullname := include "kafkafim.fullname" . -}}
{{- range $g, $group := .Values.groups -}}
  {{- $name := printf "%s-%s" $fullname $g -}}
  {{- if gt (len $name) 63 -}}
    {{- fail (printf "groups.%s: resource name %q is longer than 63 characters; shorten the group name or fullnameOverride" $g $name) -}}
  {{- end -}}
  {{- range $p := $group.pipelines -}}
    {{- if not (hasKey $pipelines $p) -}}
      {{- fail (printf "groups.%s.pipelines names %q, which is not defined in config.kafkafim.pipelines" $g $p) -}}
    {{- end -}}
    {{- if hasKey $owner $p -}}
      {{- fail (printf "pipeline %q is assigned to groups %q and %q; every pipeline belongs to exactly one group" $p (get $owner $p) $g) -}}
    {{- end -}}
    {{- $_ := set $owner $p $g -}}
  {{- end -}}
  {{- if include "kafkafim.autoscalingEnabled" $group -}}
    {{- $min := int (include "kafkafim.minReplicas" $group) -}}
    {{- if and (lt $min 2) (not $group.allowSingleReplica) -}}
      {{- fail (printf "groups.%s.autoscaling.minReplicas is %d; at least 2 replicas keep every pipeline consumed on two pods (set groups.%s.allowSingleReplica: true to allow fewer)" $g $min $g) -}}
    {{- end -}}
    {{- if not (hasKey $group.autoscaling "maxReplicas") -}}
      {{- fail (printf "groups.%s.autoscaling.maxReplicas is required when autoscaling is enabled (keep it <= the partition count)" $g) -}}
    {{- end -}}
    {{- if lt (int $group.autoscaling.maxReplicas) $min -}}
      {{- fail (printf "groups.%s.autoscaling.maxReplicas (%d) is lower than minReplicas (%d)" $g (int $group.autoscaling.maxReplicas) $min) -}}
    {{- end -}}
    {{- $res := $group.resources | default $.Values.resources | default dict -}}
    {{- $req := $res.requests | default dict -}}
    {{- if not $req.cpu -}}
      {{- fail (printf "groups.%s.autoscaling is enabled but the group has no resources.requests.cpu; the CPU HPA needs a CPU request" $g) -}}
    {{- end -}}
  {{- else -}}
    {{- $replicas := int (include "kafkafim.replicas" $group) -}}
    {{- if and (lt $replicas 2) (not $group.allowSingleReplica) -}}
      {{- fail (printf "groups.%s.replicas is %d; at least 2 replicas keep every pipeline consumed on two pods (set groups.%s.allowSingleReplica: true to allow fewer)" $g $replicas $g) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- range $p, $_ := $pipelines -}}
  {{- if not (hasKey $owner $p) -}}
    {{- fail (printf "pipeline %q is defined in config.kafkafim.pipelines but not listed in any group" $p) -}}
  {{- end -}}
{{- end -}}
{{- $seen := dict -}}
{{- range $i, $s := .Values.secrets -}}
  {{- $path := include "kafkafim.secretMountPath" $s.mountPath -}}
  {{- if not (hasPrefix "/etc/kafkafim/secrets/" $path) -}}
    {{- fail (printf "secrets[%d] (%s): mountPath %q must be below /etc/kafkafim/secrets/ (the configtree import root)" $i $s.name $s.mountPath) -}}
  {{- end -}}
  {{- if or (eq $path "/etc/kafkafim/secrets/management") (hasPrefix "/etc/kafkafim/secrets/management/" $path) -}}
    {{- fail (printf "secrets[%d] (%s): mountPath %q is below /etc/kafkafim/secrets/management, which is reserved for otel.secretHeaders" $i $s.name $s.mountPath) -}}
  {{- end -}}
  {{- if hasKey $seen $path -}}
    {{- fail (printf "secrets[%d] (%s): mountPath %q is already used by secrets[%s]" $i $s.name $s.mountPath (get $seen $path)) -}}
  {{- end -}}
  {{- $_ := set $seen $path (toString $i) -}}
{{- end -}}
{{- range $a, $ia := $seen -}}
  {{- range $b, $ib := $seen -}}
    {{- if hasPrefix (printf "%s/" $a) $b -}}
      {{- fail (printf "secrets[%s] mountPath %q is nested inside secrets[%s] mountPath %q; secret mounts must not overlap" $ib $b $ia $a) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
{{- range $k, $_ := .Values.podLabels -}}
  {{- if has $k (list "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/component") -}}
    {{- fail (printf "podLabels must not set %s; it is a selector label the chart sets" $k) -}}
  {{- end -}}
{{- end -}}
{{- $plainHeaders := dict -}}
{{- range $h, $_ := .Values.otel.headers -}}
  {{- $_ := set $plainHeaders (lower $h) $h -}}
{{- end -}}
{{- $secretHeaders := dict -}}
{{- range $h, $_ := .Values.otel.secretHeaders -}}
  {{- if hasKey $plainHeaders (lower $h) -}}
    {{- fail (printf "otel header %q is set in both otel.headers and otel.secretHeaders (header names are case-insensitive)" $h) -}}
  {{- end -}}
  {{- if hasKey $secretHeaders (lower $h) -}}
    {{- fail (printf "otel.secretHeaders sets %q and %q; header names are case-insensitive" (get $secretHeaders (lower $h)) $h) -}}
  {{- end -}}
  {{- $_ := set $secretHeaders (lower $h) $h -}}
{{- end -}}
{{- $reserved := list "SPRING_PROFILES_ACTIVE" "SPRING_CONFIG_ADDITIONAL_LOCATION" "SPRING_CONFIG_IMPORT" "OTEL_EXPORTER_OTLP_ENDPOINT" "JAVA_TOOL_OPTIONS" -}}
{{- /* Boot maps these env vars into a property source that wins over configtree, so
       they would silently override otel.headers / otel.secretHeaders. */}}
{{- if or .Values.otel.headers .Values.otel.secretHeaders -}}
  {{- $reserved = concat $reserved (list "OTEL_EXPORTER_OTLP_HEADERS" "OTEL_EXPORTER_OTLP_METRICS_HEADERS" "OTEL_EXPORTER_OTLP_TRACES_HEADERS") -}}
{{- end -}}
{{- range $e := .Values.extraEnv -}}
  {{- if has $e.name $reserved -}}
    {{- fail (printf "extraEnv must not set %s; the chart already sets it from values" $e.name) -}}
  {{- end -}}
{{- end -}}
{{- end -}}
