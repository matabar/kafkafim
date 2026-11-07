# Helm chart design (`charts/kafkafim`)

One chart, **one release per Kubernetes cluster**. A release carries every group
that cluster runs. The chart lives in this repository and is published as an OCI
artifact (see [DELIVERY.md](DELIVERY.md)); the per-cluster `values-<cluster>.yaml`
files live in the team's private infrastructure repository, since they name real
clusters, topics and Secret names.

## What the chart does and does not do

- Renders, **per group**: `Deployment`, `ConfigMap` (`application-kafkafim.yaml`),
  `PodDisruptionBudget`, `Service` (actuator), optional `HorizontalPodAutoscaler`.
- Renders once: `ServiceAccount` (no token automount), optional `NetworkPolicy`.
- Creates **no** `Secret`. Secrets are referenced by name and mounted.
- Uses only core and `apps/autoscaling/policy/networking` APIs. No CRDs, so no
  `ServiceMonitor` by default (opt-in flag for clusters that have the Prometheus
  Operator); scrape annotations are on by default.
- Has no subcharts and pulls no other images. Air-gap safe by construction.

## Values (shape)

```yaml
image:
  registry: ghcr.io/matabar          # overridden to the internal registry in the air gap
  repository: kafkafim
  tag: ""                            # defaults to .Chart.AppVersion
  pullPolicy: IfNotPresent
  pullSecrets: []

config:                              # rendered verbatim into application-kafkafim.yaml
  kafkafim:
    kafka:         { clusters: { ... } }          # shared by all groups
    elasticsearch: { clusters: { ... } }          # shared by all groups
    pipelines:     { orders: { ... }, audit: { ... }, iam: { ... } }

secrets:                             # existing Secrets → files → Spring configtree
  - name: kafka-prod-a-trust
    mountPath: /etc/kafkafim/secrets/kafka/prod-a
  - name: kafka-prod-a-orders-reader
    mountPath: /etc/kafkafim/secrets/kafka/prod-a.orders-reader
  - name: es-search-eu-orders-writer
    mountPath: /etc/kafkafim/secrets/elasticsearch/search-eu.orders-writer

groups:
  orders:
    pipelines: [orders]
    replicas: 6
    resources: { requests: { cpu: "1", memory: 1Gi }, limits: { memory: 2Gi } }
    autoscaling: { enabled: true, minReplicas: 2, maxReplicas: 12, targetCPUUtilizationPercentage: 70 }
    nodeSelector: {}
    tolerations: []
  small:
    pipelines: [audit, iam]
    replicas: 2

otel:
  endpoint: http://apm-server.observability:8200   # OTLP/HTTP; a Collector later is just a new URL
  headers: {}                                       # e.g. Authorization from a Secret via envFrom

jvm:
  options: "-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"

podSecurityContext: { runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532, seccompProfile: { type: RuntimeDefault } }
containerSecurityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: [ALL] } }
terminationGracePeriodSeconds: 90    # > Spring lifecycle timeout (60s)
pdb: { minAvailable: 1 }
topologySpread: { enabled: true, topologyKey: topology.kubernetes.io/zone }
networkPolicy: { enabled: false }
serviceMonitor: { enabled: false }   # needs the Prometheus Operator CRD
podAnnotations: { prometheus.io/scrape: "true", prometheus.io/path: /actuator/prometheus, prometheus.io/port: "8080" }
```

## Rendering rules

For each `groups.<g>`:

- `ConfigMap kafkafim-<g>` holds `application-kafkafim.yaml` =
  `config.kafkafim.kafka` + `config.kafkafim.elasticsearch` + the subset of
  `config.kafkafim.pipelines` named in `groups.<g>.pipelines`. A group never sees
  another group's pipelines, so a reload in one group cannot touch another.
- `Deployment kafkafim-<g>` mounts the ConfigMap at `/etc/kafkafim/config/` and every
  `secrets[]` entry read-only at its `mountPath`, and sets
  `SPRING_PROFILES_ACTIVE=kafkafim` (required: `application-kafkafim.yaml` is a
  profile-specific file, and the active profile also disables the default `local`
  profile that carries the development sample),
  `SPRING_CONFIG_ADDITIONAL_LOCATION=/etc/kafkafim/config/` (deliberately **not**
  `optional:`, overriding the image default, so a missing mount fails the pod at
  startup rather than starting it without pipelines),
  `SPRING_CONFIG_IMPORT=configtree:/etc/kafkafim/secrets/`,
  `OTEL_EXPORTER_OTLP_ENDPOINT`, `JAVA_TOOL_OPTIONS`.
- **No checksum annotation** on the pod template for the ConfigMap: pipeline edits
  hot-reload. Image, env, resources or mount changes roll the pods.
- Probes: startup `/actuator/health/liveness` (failureThreshold 30 × 2s), liveness
  `/actuator/health/liveness`, readiness `/actuator/health/readiness`.
- Strategy `RollingUpdate{maxSurge: 1, maxUnavailable: 0}`; preferred pod
  anti-affinity on hostname; topology spread across zones when enabled.
- `HorizontalPodAutoscaler` (autoscaling/v2, CPU) when `autoscaling.enabled`;
  `replicas` is omitted from the Deployment in that case so Helm and the HPA do not fight.
- Names: a group's resources are `<fullname>-<group>`; with the conventional release
  name `kafkafim` that is `kafkafim-<group>`. The ServiceAccount (and the opt-in
  `NetworkPolicy` / `ServiceMonitor`) are named `<fullname>` and select every group.
- Group defaults: top-level `resources`, `nodeSelector`, `tolerations`, `affinity` and
  `pdb` apply to every group that does not set its own (a group's value replaces the
  default, it is not merged); `podAnnotations` are merged, the group's winning.
  `replicas` defaults to 2. In a `pdb`, `maxUnavailable` wins over `minAvailable`
  when both are set (so overriding the default `minAvailable: 1` needs no `null`);
  a single-replica group should use `maxUnavailable: 1` so it does not block drains.
- The ConfigMap is mounted as a directory, never with `subPath`, so kubelet updates
  the file in place and hot reload sees it. A `/tmp` `emptyDir` backs the read-only
  root filesystem.
- `NetworkPolicy` (opt-in) is ingress-only: TCP 8080 from `networkPolicy.allowFrom`
  (default: pods in the release namespace). Egress is not restricted. An empty
  `allowFrom` list means any source, as in Kubernetes.
- `otel.headers` (non-secret) render as `OTEL_EXPORTER_OTLP_HEADERS=k=v,…`; secret
  headers come from a Secret through `extraEnv[].valueFrom` or `extraEnvFrom`.

## Template-time validation (`fail` in templates, plus `values.schema.json`)

- Every pipeline named in a group exists in `config.kafkafim.pipelines`.
- Every pipeline in `config.kafkafim.pipelines` belongs to exactly one group
  (unassigned or duplicated pipelines are the most likely human error).
- Every `secrets[].mountPath` is under `/etc/kafkafim/secrets/` and unique. Paths are
  normalised first (trailing `/`, `.` and `..` resolved), so `…/prod-a` and
  `…/prod-a/` are the same mount and `…/secrets/../config` is outside the root.
  Mounts must not nest (`…/kafka` and `…/kafka/prod-a`): a mount inside a read-only
  Secret volume fails container creation and would garble configtree names.
- `groups.<g>.replicas >= 2` unless `allowSingleReplica: true` is set on the group.
  Under autoscaling the same floor applies to `autoscaling.minReplicas` (default 2);
  `autoscaling.maxReplicas` is then required, must be `>= minReplicas`, and the
  group must have a `resources.requests.cpu` (the CPU HPA has nothing to scale on
  otherwise).
- `image.tag` or `.Chart.AppVersion` is non-empty.
- `<fullname>-<group>` fits in 63 characters (Service names are DNS labels; the
  chart fails rather than truncate, which could make two groups collide).
- `podLabels` does not set a selector label (`app.kubernetes.io/name`, `instance`,
  `component`).
- `extraEnv` does not redefine a variable the chart sets (the `SPRING_*` triple,
  `OTEL_EXPORTER_OTLP_ENDPOINT`, `JAVA_TOOL_OPTIONS`, and
  `OTEL_EXPORTER_OTLP_HEADERS` when `otel.headers` is set).

`values.schema.json` checks the shape only (types, required keys, no unknown keys,
group names are DNS labels); the cross-field rules above are `fail` calls in
`templates/_helpers.tpl`, each with its own unit test.

`config.kafkafim` accepts exactly `kafka`, `elasticsearch` and `pipelines`; the
schema rejects any other key (for example `allow-inline-secrets`, which is for local
development only and must never reach a production ConfigMap) instead of silently
dropping it. Helm reads values numbers as float64, so integers above 2^53 in
`config` lose precision: quote them. Application-wide settings that are not
per-group belong in the image's `application.yaml` or in `extraEnv`.

Semantic validation of the kafkafim config itself (cluster and identity references,
route rules, patterns) is the application's job. The infrastructure repository that
holds the per-cluster values can run the image with the `validate` profile against
each rendered ConfigMap in its own CI; this repository's CI only lints and templates
the chart.

## Secrets and configtree naming

A `Secret` mounted at `/etc/kafkafim/secrets/kafka/prod-a.orders-reader/` with keys
`username` and `password` becomes the properties
`kafka.prod-a.orders-reader.username` and `kafka.prod-a.orders-reader.password`
through `configtree:/etc/kafkafim/secrets/`. Identity definitions reference them as
`${kafka.prod-a.orders-reader.username}`. Convention:

```
/etc/kafkafim/secrets/kafka/<cluster>/                truststore.p12, truststore-password
/etc/kafkafim/secrets/kafka/<cluster>.<identity>/     username, password | keystore.p12, keystore-password
/etc/kafkafim/secrets/elasticsearch/<cluster>/        ca.crt
/etc/kafkafim/secrets/elasticsearch/<cluster>.<identity>/  username, password | api-key | keystore.p12, keystore-password
```

The top-level `kafka.*` and `elasticsearch.*` property namespaces are reserved for
configtree secrets; the application's own properties all live under `kafkafim.*`.

## Chart testing

- `charts/kafkafim/ci/*-values.yaml`: minimal, two-groups, hpa, network-policy.
- `helm lint`, `helm template` + `kubeconform -strict`, `helm unittest` asserting the
  pipeline-subset rendering, every `fail` condition, and that each `Deployment` sets
  `SPRING_PROFILES_ACTIVE=kafkafim` and the non-optional config location.
- Chart version and appVersion are set at release time from the git tag; in the repo
  they are `0.0.0-dev`.

## Operating notes

- A per-cluster values file is the single place to add a pipeline: add it under
  `config.kafkafim.pipelines`, list it in a group, add its Secret mounts. `helm
  upgrade` updates the ConfigMap, and the affected group hot-reloads.
- Moving a pipeline between groups changes nothing in Kafka (same `group-id`).
- Secret rotation: update the `Secret`; kubelet refreshes the mounted files; the
  application reloads the identities that changed and restarts only their pipelines.
