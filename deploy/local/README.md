# Local Kubernetes demo

Runs kafkafim on a local Kubernetes cluster with the real chart
([charts/kafkafim](../../charts/kafkafim)), against an in-cluster Kafka broker and
Elasticsearch. Secrets take the production path: Kubernetes `Secret`s mounted as files
and read through `configtree:/etc/kafkafim/secrets/` ([HELM.md](../../docs/HELM.md#secrets-and-configtree-naming)).
On Kubernetes kafkafim refuses the `local` profile and inline secrets, so this is the
way to see a chart install work before production. For day-to-day development use
`./mvnw spring-boot:run` with Compose instead ([README](../../README.md#develop)).

Local only (ADR-0015): the chart never deploys Kafka or Elasticsearch.

## What it deploys

All in namespace `kafkafim-demo`:

| Piece | From | Notes |
|---|---|---|
| `kafka` | [backends.yaml](backends.yaml) | `apache/kafka:4.1.0`, one KRaft node, no persistence. SASL/PLAIN on `kafka:9092` for kafkafim; plaintext on the pod's loopback for the demo's own `kubectl exec` commands. Topic auto-creation off. |
| `elasticsearch` | [backends.yaml](backends.yaml) | `elasticsearch:8.19.0`, single node, security on (basic auth), TLS off, no persistence. |
| Secrets `kafka-demo-kafkafim`, `es-demo-kafkafim` | `demo.sh up` | `username` + a random `password`, created once. The broker and Elasticsearch read them as env vars; kafkafim mounts them as configtree files. |
| Topics `demo.events`, `demo.events.kafkafim-dlq` | `demo.sh up` | Created with `kafka-topics.sh`: kafkafim never creates topics (ADR-0021). |
| Release `kafkafim` | [values-demo.yaml](values-demo.yaml) | Group `demo` (one replica) running the Compose demo's `demo` pipeline; image `kafkafim:dev` built from this checkout. OTLP export off (no APM Server here). |

## Requirements

Docker, `kubectl`, `helm`, and either [kind](https://kind.sigs.k8s.io/) or Docker
Desktop's Kubernetes (kubeadm mode, which shares Docker's images). JDK 27 to build the
jar, as for the rest of the repository. About 3 GiB of memory for the three pods. With
Nix: `nix shell nixpkgs#kind nixpkgs#kubectl nixpkgs#kubernetes-helm`.

## Run it

```bash
deploy/local/demo.sh up       # jar + kafkafim:dev image, kind cluster, Kafka, ES, chart
```

```bash
deploy/local/demo.sh smoke    # one record into demo.events; waits until it is in demo-eu-<date>
```

```bash
deploy/local/demo.sh down     # deletes the kind cluster
```

`CLUSTER=docker-desktop` uses Docker Desktop's cluster instead (`down` then removes
the release and the namespace). `SKIP_BUILD=1` reuses an existing `kafkafim:dev`
image. `up` is safe to re-run: after a code change, run it again and restart the pods
(`kubectl --context kind-kafkafim-demo -n kafkafim-demo rollout restart deploy/kafkafim-demo`),
since the tag stays `dev`.

## Look around

```bash
kubectl --context kind-kafkafim-demo -n kafkafim-demo logs deploy/kafkafim-demo -f
```

```bash
kubectl --context kind-kafkafim-demo -n kafkafim-demo port-forward svc/kafkafim-demo 8080:8080
```

With the port-forward running, `curl -s localhost:8080/actuator/health` and
`curl -s localhost:8080/actuator/prometheus | grep kafkafim_pipeline_state` show the
pipeline. Hot reload works as in production: edit `deploy/local/values-demo.yaml`
(for example a route's `index`), run the same `helm upgrade --install` as `demo.sh up`
does, and the pod reloads within about a minute without a restart (HELM.md, *Operating
notes*).

## Verified

On 2026-11-11, with kind 0.33.0 (Kubernetes from its default node image) on Docker
Desktop for Mac: `demo.sh up` (all pods ready), `demo.sh smoke` (record indexed in
`demo-eu-<date>`), `demo.sh down`. The rendered chart and `backends.yaml` pass
`kubeconform -strict`. `CLUSTER=docker-desktop` has not been run.
