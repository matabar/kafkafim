# ADR-0023: One Helm chart, one release per Kubernetes cluster, groups rendered as Deployments

Status: accepted · Date: 2026-11-07

## Decision
- A single chart in `charts/kafkafim`, published as an OCI artifact with the app's
  version. Per-cluster values files live in the team's private infrastructure repo.
- One release per Kubernetes cluster; the release's `groups` map renders one
  `Deployment` + `ConfigMap` + `PodDisruptionBudget` + `Service` (+ optional CPU
  `HorizontalPodAutoscaler`) per group. Each group's ConfigMap contains only that
  group's pipelines plus the shared cluster definitions.
- The chart creates no `Secret`; it mounts existing ones under
  `/etc/kafkafim/secrets/` for Spring's `configtree`. Property names follow the mount
  path: `${kafka.<cluster>.<identity>.password}`.
- No checksum annotation for the config ConfigMap (hot reload). Core APIs only; no
  CRDs; `ServiceMonitor` and `NetworkPolicy` are opt-in.
- Template-time `fail` on: pipeline named in a group but undefined, pipeline defined
  but unassigned or assigned twice, duplicate or misplaced secret mounts, single
  replica without explicit opt-in.
- `image.registry` is a first-class value so the air-gapped registry is a one-line
  override.

## Alternatives
One release per group (many releases to keep in sync, shared cluster config
duplicated); an umbrella chart with subcharts (adds indirection with nothing to
share); Kustomize overlays (fine, but the team already standardises on Helm).

## Consequences
Adding a pipeline is one values edit: define it, list it in a group, mount its
Secrets. The chart is tested with `helm unittest` and `kubeconform` in CI.
