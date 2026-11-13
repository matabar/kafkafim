# ADR-0042: The local Kubernetes demo is one umbrella chart with Kafka, Elasticsearch and Grafana LGTM

Status: accepted · Date: 2026-11-13 · Refines [ADR-0015](0015-local-dev-compose-and-k8s-demo.md)

## Context
The local Kubernetes demo was four steps hidden in `demo.sh`: create two Secrets,
`kubectl apply` a manifest with Kafka and Elasticsearch, create the topics with
`kubectl exec`, then install `charts/kafkafim`. It had no observability stack, so OTLP
export was switched off and neither dashboard could be shown.

In production Kafka, Elasticsearch, Grafana and the APM Server that receives OTLP are all
external; the chart deploys only kafkafim, and dashboards and alerts ship with each
release and are uploaded by hand. Locally, the whole thing must come up with a single
`helm install` (#180), with `grafana/otel-lgtm` as the observability stack, as in Compose.

## Decision
- The demo is a demo-only umbrella chart, `deploy/local/kafkafim-demo`. Its templates are
  Kafka, Elasticsearch, Grafana LGTM and the two credential Secrets; kafkafim is the
  production chart as a subchart, configured under the `kafkafim:` key.
- `charts/kafkafim` is unchanged and still deploys only kafkafim (ADR-0015). The demo
  chart is not packaged or released.
- The subchart is a symlink (`charts/kafkafim` → the repository's `charts/kafkafim`).
  `Chart.yaml` declares the dependency, and the symlink satisfies it, so it is not
  fetched with `helm dependency build`: no extra step before the install and no packaged
  copy that can go stale.
- The dashboards and their Grafana provider are read with `.Files` through symlinks into
  `deploy/grafana`, and rendered into ConfigMaps mounted as in `compose.yaml`. There is
  one copy of each dashboard in the repository.
- kafkafim's OTLP export points at the LGTM Service through the chart's one endpoint
  value (`kafkafim.otel.endpoint`), so every signal the app exports over OTLP arrives
  there without a demo change.
- The credential Secrets are templates: a random password on install, read back with
  `lookup` on upgrade.
- The topics are created inside the broker pod by a second container whose readiness
  gates the pod's. The `kafka` Service has no endpoint until the topics exist, so
  kafkafim's preflight sees an unreachable broker (transient, retried) and never a missing
  topic (`failed_preflight`). No Helm hook and no ordering between resources is needed.

## Alternatives
- Demo templates in `charts/kafkafim` behind a value that production never sets: one chart,
  but backend and Grafana manifests would ship in the production chart and its schema.
- A `file://` dependency with `helm dependency build`: a second command, and a `.tgz`
  that must be rebuilt after every chart change.
- A packaging step that copies the dashboards into the chart: a second copy, or a
  generated directory to keep out of git.
- Upstream Kafka/Elasticsearch/Grafana charts or operators as dependencies: needs network
  access to chart repositories and far more memory than a laptop demo should.

## Consequences
- The demo needs a checkout with working symlinks (the default on macOS and Linux; on
  Windows `git config core.symlinks true`). The chart's template fails with a clear
  message when the dashboards are missing.
- Resource names are fixed (`kafka`, `elasticsearch`, `lgtm`): one demo per namespace.
- CI lints, renders and validates the demo chart in the *Helm chart* job.
