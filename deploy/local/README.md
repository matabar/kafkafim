# Local Kubernetes demo

One `helm install` brings up everything on a local Kubernetes cluster: a Kafka broker,
Elasticsearch, Grafana LGTM (dashboards, metrics, traces, logs) and kafkafim, installed
by the real chart ([charts/kafkafim](../../charts/kafkafim)). Secrets take the
production path: Kubernetes `Secret`s mounted as files and read through
`configtree:/etc/kafkafim/secrets/` ([HELM.md](../../docs/HELM.md#secrets-and-configtree-naming)).
On Kubernetes kafkafim refuses the `local` profile and inline secrets, so this is the
way to see a chart install work before production. For day-to-day development use
`./mvnw spring-boot:run` with Compose instead ([README](../../README.md#develop)).

Local only. In production Kafka, Elasticsearch, Grafana and the APM Server that
receives OTLP are external, and `charts/kafkafim` deploys only kafkafim (ADR-0015). The
demo is a separate, demo-only chart, [kafkafim-demo](kafkafim-demo), which is never
packaged or released
([ADR-0042](../../docs/decisions/0042-local-kubernetes-demo-is-one-umbrella-chart.md)).

## What it deploys

One release, `kafkafim`, in namespace `kafkafim-demo`:

| Piece | From | Notes |
|---|---|---|
| `kafka` | [templates/kafka.yaml](kafkafim-demo/templates/kafka.yaml) | `apache/kafka:4.1.0`, one KRaft node, no persistence. SASL/PLAIN on `kafka:9092` for kafkafim; plaintext on the pod's loopback for the demo's own `kubectl exec` commands. Topic auto-creation off. |
| Topics `demo.events`, `demo.events.kafkafim-dlq` | the `topics` container of the `kafka` pod | kafkafim never creates topics (ADR-0021). The pod becomes ready, and the `kafka` Service gets its endpoint, only once they exist, so kafkafim never sees the broker without them. |
| `elasticsearch` | [templates/elasticsearch.yaml](kafkafim-demo/templates/elasticsearch.yaml) | `elasticsearch:8.19.0`, single node, security on (basic auth), TLS off, no persistence. |
| `lgtm` | [templates/lgtm.yaml](kafkafim-demo/templates/lgtm.yaml) | `grafana/otel-lgtm:0.11.0` (the Compose version): OpenTelemetry Collector, Prometheus, Tempo, Loki and Grafana in one container, no persistence. Both dashboards of [deploy/grafana](../grafana/README.md) are loaded into the *kafkafim* folder. |
| Secrets `kafka-demo-kafkafim`, `es-demo-kafkafim` | [templates/secrets.yaml](kafkafim-demo/templates/secrets.yaml) | `username` + a random `password`, generated on install and kept on upgrade. The broker and Elasticsearch read them as env vars; kafkafim mounts them as configtree files. |
| kafkafim (`deploy/kafkafim-demo`) | `charts/kafkafim` as a subchart, values under `kafkafim:` in [values.yaml](kafkafim-demo/values.yaml) | Group `demo` (one replica) running the Compose demo's `demo` pipeline; image `kafkafim:dev` built from this checkout. OTLP export goes to `http://lgtm:4318` through the chart's one endpoint value, `otel.endpoint`. |

Nothing is copied into the demo chart. `kafkafim-demo/charts/kafkafim` is a symlink to
the production chart, so there is no `helm dependency build` step. `kafkafim-demo/files/`
holds symlinks to `deploy/grafana/dashboards` and `deploy/grafana/provisioning`; the
chart renders them into ConfigMaps mounted where `compose.yaml` mounts the same
directories. On Windows, check out with `git config core.symlinks true`.

## Requirements

Docker, `kubectl`, `helm`, and either [kind](https://kind.sigs.k8s.io/) or Docker
Desktop's Kubernetes (kubeadm mode, which shares Docker's images). JDK 27 to build the
jar, as for the rest of the repository. With Nix:
`nix shell nixpkgs#kind nixpkgs#kubectl nixpkgs#kubernetes-helm`.

Memory: the four pods request about 3.3 GiB and are limited to about 4.4 GiB (Kafka
768 Mi + 256 Mi for its topics container, Elasticsearch 1 Gi, LGTM 1.5 Gi, kafkafim
768 Mi). Give Docker at least 6 GiB.

## Run it

```bash
deploy/local/demo.sh up       # jar + kafkafim:dev image, kind cluster, then the one helm install
```

```bash
deploy/local/demo.sh smoke    # one record into demo.events; waits until it is in demo-eu-<date>
```

```bash
deploy/local/demo.sh down     # deletes the kind cluster
```

`demo.sh up` only prepares what Helm cannot (the image and the cluster). The install
itself is one command, which also works on its own against any cluster that can run the
`kafkafim:dev` image:

```bash
helm upgrade --install kafkafim deploy/local/kafkafim-demo -n kafkafim-demo --create-namespace --wait --timeout 10m
```

`CLUSTER=docker-desktop` uses Docker Desktop's cluster instead (`down` then removes
the release and the namespace). `SKIP_BUILD=1` reuses an existing `kafkafim:dev`
image. `up` is safe to re-run: after a code change, run it again and restart the pods
(`kubectl --context kind-kafkafim-demo -n kafkafim-demo rollout restart deploy/kafkafim-demo`),
since the tag stays `dev`. The cluster pulls the Kafka, Elasticsearch and LGTM images
itself on the first install.

## Look around

The commands below use the kind context; with `CLUSTER=docker-desktop` replace
`--context kind-kafkafim-demo` with `--context docker-desktop`.

Grafana, with anonymous admin access:

```bash
kubectl --context kind-kafkafim-demo -n kafkafim-demo port-forward svc/lgtm 3000:3000
```

Then open <http://localhost:3000>:

- *Dashboards → kafkafim*: **kafkafim overview** and **kafkafim Elasticsearch**. Run
  `demo.sh smoke` a few times first; metrics arrive every 15s, and rate panels need two
  samples.
- *Explore → Tempo*: search for `service.name = kafkafim` to see a record's trace.
- *Explore → Loki*: kafkafim's logs, once the application exports logs over OTLP (#182).
  They use the same endpoint, so the demo needs no change.

kafkafim itself:

```bash
kubectl --context kind-kafkafim-demo -n kafkafim-demo logs deploy/kafkafim-demo -f
```

```bash
kubectl --context kind-kafkafim-demo -n kafkafim-demo port-forward svc/kafkafim-demo 8080:8080
```

With the port-forward running, `curl -s localhost:8080/actuator/health` and
`curl -s localhost:8080/actuator/prometheus | grep kafkafim_pipeline_state` show the
pipeline. Hot reload works as in production: edit the pipeline under `kafkafim.config`
in `deploy/local/kafkafim-demo/values.yaml` (for example a route's `index`), run the
`helm upgrade --install` above again, and the pod reloads within about a minute without
a restart (HELM.md, *Operating notes*).

## Verified

On 2026-11-13, with kind 0.33.0 (node image `kindest/node:v1.37.0`), Helm 4.3.1 and
Docker Desktop for Mac (7.7 GiB):

- `demo.sh up` on a new cluster: the one `helm upgrade --install` finished in about a
  minute with images already pulled; all four pods ready with no restarts
  (`kafka` 2/2, the others 1/1). The kind node used 3.8 GiB with everything running.
- kafkafim started while the broker pod was still creating the topics, stayed in
  `starting`, and went to `running` on its own.
- `demo.sh smoke`, ten times: each record indexed in `demo-eu-<date>`.
- Grafana 11.6.0 through the port-forward: *kafkafim / Overview* and *kafkafim /
  Elasticsearch* are in the *kafkafim* folder. Overview: all 21 panels have data
  (`running` = 1 for pipeline `demo`, end-to-end latency, records, documents by route,
  one DLQ record from a deliberately bad timestamp). Elasticsearch: 9 of 11 panels have
  data (breaker `closed`, bulk requests with status 200, bulk latency); *Retries by
  scope* and *Item errors by type* are empty because the demo had no retry or item error.
- Tempo: a `pipeline.batch` trace of service `kafkafim` with its `es.bulk` child span
  (`http.response.status_code` 200, `kafkafim.pipeline` `demo`).
- Loki has no kafkafim logs yet: the application does not export logs over OTLP before
  #182.
- Running the same `helm upgrade --install` again kept both generated passwords, and
  `smoke` passed after it.
- `demo.sh down`. `helm lint` passes and the rendered chart passes `kubeconform -strict`.

`CLUSTER=docker-desktop` has not been run.
