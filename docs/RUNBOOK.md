# kafkafim operator runbook

For the on-call engineer: how to deploy, observe and recover kafkafim in production.
It describes what the code does today; where behaviour is missing, it says so and
lists the gap under [Known gaps](#known-gaps). Background is in
[DESIGN.md](DESIGN.md), the chart in [HELM.md](HELM.md), the release and air-gap
procedure in [DELIVERY.md](DELIVERY.md).

Conventions used below:

- One Helm release per Kubernetes cluster, conventionally named `kafkafim`. Every
  group `<g>` renders `Deployment`, `ConfigMap`, `Service` and `PodDisruptionBudget`
  named `kafkafim-<g>`; pods carry `app.kubernetes.io/instance=kafkafim` and
  `app.kubernetes.io/component=<g>` ([HELM.md](HELM.md#rendering-rules)).
- The actuator is on port 8080 and reachable only in-cluster
  ([DESIGN §11](DESIGN.md#11-security)). From a workstation, port-forward to **one pod**:

  ```bash
  kubectl -n <ns> port-forward pod/<pod> 8080:8080
  ```

  Every pod has its own pipelines, state and reload result; a port-forward to
  `deploy/kafkafim-<g>` picks the first matching pod, so prefer naming the pod.
- Logs are ECS JSON on stdout. The log lines quoted below are the `message` field.
- Metric names are the Prometheus names from [DESIGN §10](DESIGN.md#10-observability).

## 1. Mental model in one minute

- A **group** is a `Deployment`; every replica runs every pipeline of the group, and
  Kafka's consumer-group rebalancing spreads partitions over the replicas
  ([DESIGN §4.1](DESIGN.md#41-deployment-model-groups)).
- A **pipeline** consumes one consumer group, decodes JSON, routes each document to
  one or more Elasticsearch targets, and commits an offset only once every document
  of the record was acknowledged by Elasticsearch or written to the DLQ
  ([DESIGN §4.3](DESIGN.md#43-offsets-and-delivery-guarantee)). Redelivery is
  harmless because `_id`s are deterministic.
- **Kafka is the only buffer.** When Elasticsearch is slow or down, kafkafim stops
  fetching and lag grows; it never drops data and never dead-letters because of an
  outage ([DESIGN §7](DESIGN.md#7-backpressure-and-failure-handling),
  [ADR-0010](decisions/0010-kafka-as-the-only-buffer-backpressure.md)).
- The **DLQ** is a topic on the source Kafka cluster that receives every document
  Elasticsearch rejects for a data reason (and records that cannot be decoded or
  routed). If the DLQ itself cannot be written, the pipeline blocks rather than lose
  data ([DESIGN §9](DESIGN.md#9-dlq)).
- kafkafim **creates nothing** in Kafka or Elasticsearch
  ([ADR-0021](decisions/0021-no-resource-provisioning.md)). A missing topic,
  ACL, user or role is fixed in the provisioning systems, not here.

## 2. Deploy, upgrade, roll back

### 2.1 Import a release into the air gap

Follow [DELIVERY.md § Importing into the air-gapped environment](DELIVERY.md#importing-into-the-air-gapped-environment):
download and `cosign verify` on the internet side, carry the bundle across,
`sha256sum -c SHA256SUMS`, `cosign load` the image into the internal registry,
`helm push` the chart, and record the image digest in each per-cluster values file
(`image.registry`, `image.digest`).

### 2.2 Upgrade

The infrastructure repository's CI applies each cluster's release with plain Helm
(no Argo CD or Flux):

```bash
helm upgrade --install kafkafim oci://registry.internal/charts/kafkafim --version <ver> -n <ns> -f values-<cluster>.yaml
```

What happens depends on what changed ([HELM.md](HELM.md#rendering-rules),
[DESIGN §5.4](DESIGN.md#54-helm-rendering)):

| Change | Effect |
|---|---|
| Only `config.kafkafim.*` (pipelines, clusters, identities) | The group's `ConfigMap` changes; pods are **not** rolled. Each pod hot-reloads within about a minute (kubelet refresh) plus 10s poll and 2s debounce. See [§4](#4-hot-reload-and-secret-rotation). |
| A `Secret` named in `secrets[]` (outside Helm) | Same as above: kubelet refreshes the files, pods reload the identities that changed. |
| Image, `extraEnv`, resources, mounts, `secrets[]` list, `otel.*`, `jvm.options`, probes | The `Deployment`'s pod template changes; pods roll one at a time (`maxSurge: 1`, `maxUnavailable: 0`). |
| Both kinds at once | Old pods may hot-reload the new configuration before the rollout replaces them; new pods start with it. |

Watch a rollout per group:

```bash
kubectl -n <ns> rollout status deployment/kafkafim-<g> --timeout=10m
```

A new pod becomes ready only when at least one of its pipelines is `running` or
`paused` (or the group has no pipelines), so a rollout of a broken configuration
stops at the first new pod and the old pods keep working (unless the same upgrade
also changed `config.kafkafim.*`: the old pods then hot-reload the new configuration
too, and may reject it or end up with `failed_preflight` pipelines)
([DESIGN §4.6](DESIGN.md#46-no-provisioning-fail-fast-verification)). Readiness
also requires that at least one started pipeline's Kafka consumers are connected.
Elasticsearch health never affects readiness.

Before you call an upgrade done:

1. `kubectl -n <ns> get pods -l app.kubernetes.io/instance=kafkafim`: all `Running`,
   all ready, restart counts not climbing.
2. Every pipeline is `running` on every pod (see [§3.1](#31-pipeline-state)); none
   is `failed_preflight` or stuck in `starting`.
3. For a config-only change: `kafkafim_config_generation` went up by one on every
   pod of the group and `kafkafim_config_reload_total{result="failure"}` did not
   move ([§4.2](#42-reading-the-result)).
4. The pipelines' consumer-group lag, in the Kafka team's consumer-lag monitoring, is
   flat or falling, `kafkafim_e2e_latency_seconds` p99 did not jump, and
   `rate(kafkafim_dlq_records_total[5m])` did not jump.

### 2.3 Roll back

```bash
helm history kafkafim -n <ns>
```

```bash
helm rollback kafkafim <revision> -n <ns>
```

A rollback is an upgrade to the old manifests and behaves as in the table above: a
config-only rollback hot-reloads, an image rollback rolls the pods. In the air gap
the previous image must still be in the internal registry under the digest the old
revision names; do not prune it until the new release is proven. Equivalently,
revert the values change in the infrastructure repository and let its CI run
`helm upgrade --install` again; prefer that, so git stays the source of truth.

Redelivery after a rollback is harmless (idempotent `_id`s). The one exception:
pipelines with `document.id-missing: auto` (ES-generated ids) index duplicates for
any record re-read after a restart.

### 2.4 Restart pods

```bash
kubectl -n <ns> rollout restart deployment/kafkafim-<g>
```

Needed for: a pipeline paused by a record-handler failure, an OTLP header Secret
rotation, TLS certificate or keystore files rotated under the same path and password
([§4.3](#43-rotating-a-secret); a new keystore or truststore password reloads without
a restart).
A pipeline in `failed_preflight` does not need a restart: after fixing the cause, a
no-op `POST /actuator/reload` retries it ([§5.5](#55-failed_preflight-pipelines)).
Shutdown is graceful: each pipeline stops fetching, gives
in-flight records up to 30s to complete, commits and leaves its group; records still
incomplete are re-delivered to the next owner. The pod has 90s
(`terminationGracePeriodSeconds`) for this ([DESIGN §7](DESIGN.md#7-backpressure-and-failure-handling),
Pod SIGTERM row).

## 3. Observe

### 3.1 Pipeline state

Each pipeline on each pod is in one `PipelineState`
(`supervisor/PipelineState`):

| State | Meaning | Normal? |
|---|---|---|
| `starting` | Preflight running, or retrying after a **transient** failure (broker or ES unreachable, 5xx): 1s doubling to 30s, forever. | Briefly at start and after a restart. Minutes in `starting` means something is unreachable. |
| `running` | Source started and fetching (it may still be throttled by the in-flight byte budget, which is ordinary backpressure). | Yes. |
| `paused` | Source started, fetching stopped for a supervisor reason: `es-breaker:<cluster>`, `es-auth:<cluster>/<identity>` or `dlq-blocked`. Resumes by itself when the reason clears. | No; see [§5](#5-incidents). |
| `failed_preflight` | A preflight or start check found a **configuration** problem. Stays stopped until the next accepted reload (any `applied` or `unchanged` one, including a no-op `POST /actuator/reload`) retries it, or the pod restarts. | No. |
| `failed` | Was running, then the record handler threw or a Kafka consumer stopped on its own. kafkafim stopped the pipeline (drained, committed, left the group); it stays stopped until the next accepted reload retries it, or the pod restarts ([§5.13](#513-failed-pipelines)). | No. |
| `stopped` | Not started yet, or stopped by a reload or shutdown. | Only transiently. |

**Alerts**: [`KafkafimPipelinePaused`](../deploy/grafana/alerts/kafkafim-alerts.yaml) (a pipeline `paused` for 5m; the
reason is in the log lines below, and §5 has a section per reason).

Where to read it:

- **Metric** `kafkafim_pipeline_state{pipeline,state}`: 1 for the current state, 0
  for the others, per pod. This is the source of truth for alerts:

  ```promql
  max by (pipeline) (kafkafim_pipeline_state{state=~"failed_preflight|failed"}) == 1
  ```

  The shipped `KafkafimPipelineFailed` keeps `job` and `instance` instead, since a
  `failed` pipeline is retried pod by pod ([§5.13](#513-failed-pipelines)).

  From a port-forwarded pod:

  ```bash
  curl -s localhost:8080/actuator/prometheus | grep '^kafkafim_pipeline_state' | grep ' 1.0$'
  ```

- **Logs**: every transition is logged at INFO as `Pipeline '<name>': <from> -> <to>`;
  a pause as WARN `Pipeline '<name>': pausing (<reason>)` and its end as INFO
  `Pipeline '<name>': <reason> cleared`. The **reason** of a pause is only in these log
  lines (the metric says `paused`, not why).
- **Health**: `/actuator/health/readiness` includes the `pipelines` component. With
  the packaged `show-details: when-authorized` and no authentication, the response
  shows component statuses only, not the per-pipeline states, so use the metric.

### 3.2 Health endpoints

| Endpoint | Up when | Used by |
|---|---|---|
| `/actuator/health/liveness` | The process is alive (`livenessState`) and `supervisor`: the pipeline supervisor runs (or is draining for shutdown). Pipeline, Kafka and Elasticsearch problems never fail liveness: a restart would not fix them. | Startup probe (30 × 2s) and liveness probe. |
| `/actuator/health/readiness` | `readinessState` and `pipelines`: at least one pipeline `running` or `paused`, or zero pipelines configured; and `kafka`: at least one started pipeline's consumers hold broker connections (or none is started). Never gated on Elasticsearch. | Readiness probe; the `Service` (actuator only, no business traffic). |

An unready pod still consumes: readiness only gates the actuator `Service` and the
rollout. A pod whose every pipeline is `failed_preflight` or `failed` stays unready,
without being restarted, until a reload retries them ([§5.5](#55-failed_preflight-pipelines),
[§5.13](#513-failed-pipelines)).

### 3.3 Metrics to look at first

Pipeline metrics carry `pipeline` and `kafka_cluster`; `kafkafim_pipeline_state` has only `pipeline` and `state`, `kafkafim_breaker_state` only `es_cluster`, the reload meters
neither (`kafkafim_config_reload_total` has `result`) ([DESIGN §10](DESIGN.md#10-observability)).

| Question | Metric |
|---|---|
| Is every pipeline running? | `kafkafim_pipeline_state{state=...}` |
| Did the last config change apply? | `kafkafim_config_generation`, `kafkafim_config_reload_total{result="failure"}` (no `pipeline` label) |
| Are we keeping up? | `kafkafim_e2e_latency_seconds` (p99 against the target), `rate(kafkafim_records_consumed_total[5m])` against the producers' rate; consumer lag is in the Kafka team's consumer-lag monitoring, per consumer group (`source.group-id`, default `kafkafim.<pipeline>`); kafkafim exports none ([ADR-0034](decisions/0034-consumer-lag-is-monitored-outside-kafkafim.md)) |
| Is data flowing? | `rate(kafkafim_records_consumed_total[5m])`, `rate(kafkafim_docs_total{outcome="indexed"}[5m])` |
| Is Elasticsearch healthy? | `kafkafim_breaker_state{es_cluster}` (0 closed, 1 half-open, 2 open), `kafkafim_bulk_requests_total{es_cluster,status}`, `kafkafim_bulk_latency_seconds`, `kafkafim_retries_total{scope}` |
| Credentials rejected? | `kafkafim_auth_failures_total{es_cluster,identity}` |
| What is going to the DLQ? | `kafkafim_dlq_records_total{error_type}`, `kafkafim_item_errors_total{es_cluster,error_type,retryable}` |
| What are drop routes discarding? | `rate(kafkafim_docs_total{outcome="dropped"}[5m])` by `route` ([§5.14](#514-documents-missing-from-elasticsearch-dropped-or-lost)) |
| Backpressure / memory? | `kafkafim_in_flight_bytes` against the pipeline's `source.max-in-flight-bytes`; `jvm_memory_used_bytes{area="heap"}` |
| Does this pod own partitions? Is Kafka connected? | `kafkafim_assigned_partitions`, `kafkafim_kafka_connected` (1/0) |

Dashboards and alert rules for these metrics ship in `deploy/grafana/`
([DESIGN §10](DESIGN.md#shipped-with-the-repo)): see
[deploy/grafana/README.md](../deploy/grafana/README.md) for importing the dashboards
and loading the rules. Every alert's `runbook_url` points to a section of §5 (or
§3.1), and each of those sections names the alerts that lead to it.

## 4. Hot reload and secret rotation

### 4.1 How a change gets in

Each pod watches its config directory (`/etc/kafkafim/config/`) and every secret
directory under `/etc/kafkafim/secrets/` (file watch plus a 10s content-hash poll,
2s debounce). Kubelet refreshes mounted ConfigMaps and Secrets with up to about a
minute of delay, so expect a change to apply within about 75s of `helm upgrade` or a
`Secret` update. To reload one pod at once (after kubelet has refreshed its files):

```bash
curl -s -X POST localhost:8080/actuator/reload
```

`POST /actuator/reload` does the same reload as the watcher, waits for it (including
draining the pipelines it restarts, up to about 50s each, in parallel) and works with
the watcher disabled (`kafkafim.reload.enabled=false`). It does not make kubelet
refresh any faster: if the response is `unchanged` right after an upgrade, the pod
has not seen the new files yet. Reloads are serialised per pod; a second call waits
for the first.

What a reload restarts ([DESIGN §5.6](DESIGN.md#56-validation-and-hot-reload),
[ADR-0028](decisions/0028-hot-reload-rereads-config-data-and-shares-building-blocks-per-cluster.md)):

- a pipeline whose own definition changed;
- every pipeline using a cluster whose definition changed (other than its identities);
- only the pipelines referencing an identity that changed, including a rotated
  secret value or a new `default-identity` they inherit;
- added pipelines start, removed ones drain and stop; everything else is untouched,
  except that every unchanged pipeline in `failed_preflight` is started once more
  ([ADR-0032](decisions/0032-accepted-reload-retries-failed-preflight-pipelines.md)).

A restart drains first (stop fetching, finish in-flight work, commit, leave the
group), so nothing that completed is re-delivered; the group still rebalances.

### 4.2 Reading the result

The endpoint answers JSON (names and violations only, never values):

```json
{"result":"applied","generation":7,"added":["audit"],"removed":[],
 "restarted":[{"pipeline":"orders","reasons":["..."]}],"retried":["billing"],
 "unchanged":["iam"],"errors":[]}
```

| HTTP / `result` | Meaning | What to do |
|---|---|---|
| 200 `applied` | Valid and changed something; `generation` went up by one. `kafkafim_config_reload_total{result="success"}` +1; INFO lines `Configuration reload (<trigger>): applying generation <n>: added ..., removed ..., restarted ..., unchanged ...` and `... generation <n> applied`, with `; retrying failed pipeline(s) [...]` appended when `retried` is not empty. | Check the restarted, added and retried pipelines reach `running`. |
| 200 `unchanged` | Valid, identical to what runs; nothing restarted, `generation` stays. Pipelines in `failed_preflight` or `failed` are still retried and listed under `retried`; INFO `Configuration reload (<trigger>): no change, generation <n> stays`. Counts as `success`. | If you expected a change: kubelet has not refreshed the mount yet, or the change was outside `kafkafim.*`. Check the retried pipelines reach `running`. |
| 400 `rejected` | The new tree is invalid or unreadable; **the running configuration stays**, nothing was touched. `errors` lists every violation as `<property path>: <problem>`; `kafkafim_config_reload_total{result="failure"}` +1; one ERROR line `Configuration reload (<trigger>) rejected; generation <n> keeps running. ...`. | Fix the values and upgrade again. See the warning below. |
| 503 | Body `"result":"rejected"` with the reason in `errors`. Usually the supervisor is not running (startup not finished, or shutting down); an internal state error while applying is also mapped here. | Retry once the pod is up; if it persists on a running pod, treat it like a 500. |
| 500 | Applying a valid tree failed part-way (ERROR `... failed while applying generation <n>; pipelines may be partly reconfigured`); `kafkafim_config_reload_total{result="failure"}` +1. | Restart the group's pods ([§2.4](#24-restart-pods)) so they start cleanly from the files. |

> **A rejected reload is a time bomb.** The pods keep running the old generation, but
> the `ConfigMap` now holds the invalid one: any pod that restarts, is rescheduled or
> is added by the HPA fails at startup with the same violations. Revert or fix the
> values promptly ([§2.3](#23-roll-back)). The watcher does not retry a rejected tree
> until the files change again.

Not reloaded (needs a pod restart): `kafkafim.reload.*` (a WARN says so), OTLP export
headers and endpoint, and anything outside `kafkafim.*`. With the chart these only
arrive through `otel.*`, `extraEnv` or the image, which roll the pods anyway (except
rotating the contents of an `otel.secretHeaders` Secret, which needs a restart; the
chart's `config.kafkafim` accepts only `kafka`, `elasticsearch` and `pipelines`).
Every accepted reload (`applied` or `unchanged`, from the watcher or the endpoint)
starts each unchanged `failed_preflight` or `failed` pipeline once more and lists it
under `retried` instead of `unchanged`; a rejected reload retries nothing. A retry is
one ordinary start in the background, so the response does not say whether it worked:
watch `kafkafim_pipeline_state` and the pipeline's log lines. A pipeline that fails
again waits in a failed state for the next reload; nothing retries on its own
([ADR-0032](decisions/0032-accepted-reload-retries-failed-preflight-pipelines.md),
[ADR-0036](decisions/0036-pipeline-that-fails-while-running-is-stopped-as-failed.md)).

### 4.3 Rotating a secret

Secrets reach the application as files through Spring's `configtree`
(`/etc/kafkafim/secrets/kafka/<cluster>.<identity>/password` becomes
`${kafka.<cluster>.<identity>.password}`, [DESIGN §5.5](DESIGN.md#55-secrets),
[HELM.md](HELM.md#secrets-and-configtree-naming)).

1. Make the new credential valid in Kafka or Elasticsearch **first**, keeping the old
   one valid: pipelines use the old value until kubelet has refreshed the file.
2. Update the Kubernetes `Secret` (however it is provisioned: plain, Sealed, External
   Secrets, Vault agent).
3. Within about a minute each pod reloads, restarts only the pipelines referencing that
   identity, and logs the applied generation. Check `kafkafim_config_generation` rose on
   every pod of the affected groups and the pipelines are `running`.
4. Revoke the old credential.

**TLS files are the exception.** Truststores, keystores and `ssl.ca-certificate` are
referenced by **path**; only their passwords are properties. When a certificate or
keystore file changes but its path and password stay the same (for example
cert-manager renewing a keystore), the reload sees an identical configuration and
answers `unchanged`; running Kafka and Elasticsearch clients keep the old material,
read when they were built. After rotating TLS files, restart the affected groups'
pods ([§2.4](#24-restart-pods)). A rotation that also changes the file's password
(a `configtree` property) reloads normally: the identity or cluster definition
changed, so the restarted pipelines build new clients from the new files.

A secret file that disappears (key removed from the `Secret`) leaves its
`${...}` placeholder unresolved and the reload is **rejected**; the pods keep the old
value. The APM Server token (`otel.secretHeaders`) is read only at startup: after
rotating it, restart the pods.

## 5. Incidents

Each entry: **symptom → check → action.** "Lag grows" means the pipeline's consumer
group lag (`source.group-id`; the default group id is `kafkafim.<pipeline>`) in the
Kafka team's consumer-lag monitoring, which also alerts on it; kafkafim exports no lag
([ADR-0034](decisions/0034-consumer-lag-is-monitored-outside-kafkafim.md)). Kafka
tooling shows it too (`kafka-consumer-groups.sh --describe --group kafkafim.<pipeline>`).

### 5.1 Elasticsearch outage, circuit breaker open

**Alerts**: [`KafkafimBreakerOpen`](../deploy/grafana/alerts/kafkafim-alerts.yaml).

- **Symptom**: pipelines routing to one ES cluster go `paused`; lag grows. Logs: WARN `Elasticsearch cluster '<es>' circuit breaker: State transition
  from CLOSED to OPEN`, WARN `Pipeline '<p>': pausing (es-breaker:<es>)`, WARN
  `Bulk sink ...: request failed (status <s>, attempt <n>), retrying in <ms>ms: ...`.
  For an HTTP answer the text after the colon is Elasticsearch's
  `type=<error.type>, root_cause=[...]` (`non-JSON, <n> bytes` for a proxy page,
  `empty body` for none), never its `reason`, which can quote document values
  (ADR-0030); a connection error or timeout shows `transport error: <exception>`. To
  see the response body itself, cut to 1024 characters, at DEBUG, add to the chart's
  `extraEnv` (this rolls every group's pods):

  ```yaml
  extraEnv:
    - name: SPRING_APPLICATION_JSON
      value: '{"logging":{"level":{"io.github.matabar.kafkafim.sink.es.BulkSink":"DEBUG"}}}'
  ```

  A `LOGGING_LEVEL_..._BULKSINK` variable does not work: Spring lowercases
  environment-variable keys, and logger names are case-sensitive. Remove the entry when
  done: the body can hold document field values.
  `kafkafim_breaker_state{es_cluster="<es>"} == 2`,
  `kafkafim_bulk_requests_total{status=~"5..|429|io_error"}` rising.
- **Check**: the ES cluster itself (its own monitoring; kafkafim never calls cluster
  health). Is it down, overloaded (429s, `es_rejected_execution_exception`), or is
  the network path broken? Pipelines writing to other ES clusters are unaffected.
- **Action**: none in kafkafim. Bulk requests are retried with the same documents
  forever (100ms doubling to 30s, ±50%). The breaker (per ES cluster, shared by
  identities and pipelines) opens at ≥50% failures over the last 20 requests (at
  least 10), stays open 10s, then lets 2 probes through; once it closes, the pause
  clears and the backlog drains. A **request-level** failure (connection error,
  timeout, 5xx or 429 for the whole bulk) never dead-letters anything. **Item-level**
  failures inside a successful bulk response are different: an item 429
  (`es_rejected_execution_exception`), item 5xx or item `index_not_found` is retried
  in later bulks only up to `dlq.max-attempts` (default 10) or `dlq.max-elapsed`
  (default 10m), then goes to the DLQ as `es_retries_exhausted`. A sustained partial
  overload therefore does produce DLQ records; replay them once ES is healthy
  ([§5.6](#56-dlq-filling-or-dlq-blocked)). If the outage will be long, nothing needs to be scaled down: memory is
  bounded by `max-in-flight-bytes`. A reload that restarts an affected pipeline drops
  its breaker pause; the new run re-pauses on its first failures.
- **Afterwards**: expect a catch-up burst; watch lag fall and `kafkafim_bulk_latency_seconds`.

### 5.2 Elasticsearch authentication failure (pipeline paused)

**Alerts**: [`KafkafimEsAuthFailures`](../deploy/grafana/alerts/kafkafim-alerts.yaml).

- **Symptom**: a pipeline is `paused`; WARN `Pipeline '<p>': pausing
  (es-auth:<es>/<identity>)` (`<none>` when the route has no identity); bulk sink
  WARNs with `status 401` or `403` and `authentication failure: type=security_exception,
  ..., reason=...`; the reason names the user, roles, action and index.
  `kafkafim_auth_failures_total{es_cluster,identity}` increasing. Not a breaker
  failure; nothing goes to the DLQ.
- **Check**: is the credential in the mounted Secret current? Was it rotated in ES
  without the Secret, or the reverse? For 403: does the identity's role still have
  `write` (or `create_doc` for `op: create`), `auto_configure`/`create_index` as
  needed and `read_pipeline` for a named ingest pipeline on the index patterns
  ([DESIGN §5.2.1](DESIGN.md#521-identities-kafka-and-elasticsearch))? An **item**
  403 (identity may not write one index) pauses the same way.
- **Action**: fix the credential or role at the source. Retries continue with backoff;
  when ES accepts the identity again the sink logs INFO `Bulk sink ...: Elasticsearch
  accepts the identity again` and the pause clears by itself. Updating the Secret also
  restarts the pipelines using that identity ([§4.3](#43-rotating-a-secret)).

### 5.3 Kafka unreachable

**Alerts**: [`KafkafimKafkaDisconnected`](../deploy/grafana/alerts/kafkafim-alerts.yaml).

- **At pipeline start**: the pipeline stays `starting`; WARN `Pipeline '<p>': preflight
  could not complete, retrying: ...` or `source could not start, retrying: ...`.
  Retries 1s doubling to 30s, forever; no restart needed once the brokers are back.
  If every pipeline of a new pod is `starting`, the pod stays unready and a rollout
  waits (old pods keep running).
- **After start**: kafka-clients reconnects with backoff on its own; state stays
  `running`. `kafkafim_kafka_connected == 0` for the pipeline, and
  readiness goes out of service on a pod where every started pipeline lost its brokers.
  `kafkafim_records_consumed_total` stops; the Kafka team's monitoring shows the group's
  lag from the brokers' side.
- **Check**: broker health, DNS, network policy/firewall from the pod's namespace to
  `bootstrap-servers`, TLS trust (an expired or rotated cluster CA shows up as an SSL
  handshake failure in the logs; a rotated truststore file needs a pod restart,
  [§4.3](#43-rotating-a-secret)).
- **Action**: fix the path; kafkafim recovers by itself. Restart pods only if a
  pipeline stays wedged after Kafka is back (see [§5.7](#57-pipeline-shows-running-but-consumes-nothing)).

### 5.4 Kafka authentication or ACL missing

- **Source topics not visible at start** (topic absent, no `Describe` for the
  consumer identity, SASL rejected): the pipeline goes `failed_preflight` (see
  [§5.5](#55-failed_preflight-pipelines)). The start check is a metadata fetch, which
  needs only `Describe`.
- **`Read` missing on a source topic** (but `Describe` present): not checked at start.
  The pipeline shows **`running`**; its fetches fail with a topic authorization error,
  retried every 10s and logged; nothing is consumed.
- **Consumer group not permitted** (missing `Read` on the group): not checked before
  the first join. The pipeline shows **`running`**, but its consumers retry the join
  every 10s and log the group authorization error; it never gets partitions.
  `kafkafim_assigned_partitions == 0` on every pod and lag grows.
- **Auth or ACL revoked after start**: consumers retry every 10s instead of stopping;
  state stays `running`, consumption stops.
- **DLQ producer** (missing `Write` on the DLQ topic, which the idempotent producer
  already needs for `InitProducerId`): see [§5.6](#56-dlq-filling-or-dlq-blocked).
- **Action**: add the ACL in the provisioning system (consumer identity: `Read` and
  `Describe` on the topics and `Read` on the group; DLQ identity: `Write` and
  `Describe` on the DLQ topic). Running consumers pick it up within 10s; for a
  `failed_preflight` pipeline, run a no-op `POST /actuator/reload` on each pod
  ([§5.5](#55-failed_preflight-pipelines)).

### 5.5 `failed_preflight` pipelines

**Alerts**: [`KafkafimPipelineFailedPreflight`](../deploy/grafana/alerts/kafkafim-alerts.yaml).

- **Symptom**: `kafkafim_pipeline_state{state="failed_preflight"} == 1`; ERROR
  `Pipeline '<p>': failed preflight, staying stopped: <problems>`. One line lists every
  problem found. The other pipelines of the pod keep running and the pod stays ready
  (unless this was its only pipeline).
- **Check**: the message names the resource and identity. Typical causes:

  | Message contains | Cause | Fix (outside kafkafim) |
  |---|---|---|
  | `source topics [...] do not exist or are not visible to identity '<id>'` | Topic missing, or no `Describe` for the consumer identity | Create the topic / add the ACL |
  | `identity '<id>' on Kafka cluster '<c>' rejected reading [...]` | SASL credentials wrong, or authorization denied | Fix the Secret or ACL |
  | `DLQ topic '<t>' (identity '<id>' on Kafka cluster '<c>') does not exist` or `...: identity may not describe it` | DLQ topic (default `<first topic>.kafkafim-dlq`) not provisioned, or no `Describe` for the DLQ identity | Create it / add the ACL; kafkafim never auto-creates it |
  | `Elasticsearch identity '<id>' on cluster '<es>' rejected: HTTP 401 from _security/_authenticate` (or 403) | Bad ES credential | Fix the Secret or the ES user/API key |
  | `Elasticsearch identity ... rejected: TLS handshake failed: ...` | Wrong or expired cluster CA (`ssl.ca-certificate`) or hostname mismatch | Fix the CA Secret (then restart the pods, [§4.3](#43-rotating-a-secret)) or `uris` |
  | `configuration cannot run: ...`, unknown identity, unreadable trust material | Bad mount or config | Fix the values / Secret mounts |

- **Action**: fix the cause, then run a no-op `POST /actuator/reload` on each pod of
  the group ([§4.1](#41-how-a-change-gets-in)); any accepted reload, such as the next
  `helm upgrade` of the group, does the same. The pipeline appears under `retried` in
  the response and goes `starting` → `running`; if it is back in `failed_preflight`,
  the ERROR line says what is still wrong, and if it stays `starting`, something is
  unreachable ([§5.12](#512-pipeline-stuck-in-starting)). **Fixing the resource alone does not
  recover the pipeline**: nothing retries on its own, only a reload or a pod restart
  ([ADR-0032](decisions/0032-accepted-reload-retries-failed-preflight-pipelines.md)).
  Transient causes (unreachable broker or ES, 5xx) never produce `failed_preflight`;
  they keep the pipeline in `starting`.

### 5.6 DLQ filling, or DLQ blocked

**Alerts**: [`KafkafimDlqRecords`](../deploy/grafana/alerts/kafkafim-alerts.yaml).

**DLQ filling** (records are being dead-lettered):

- **Symptom**: `rate(kafkafim_dlq_records_total[10m]) > 0`, by `error_type`; WARN
  lines per DLQ record naming the source coordinates, error type and target.
- **Check**: what the records say. Read the DLQ topic with headers:

  ```bash
  kafka-console-consumer.sh --bootstrap-server <broker> --consumer.config <client.properties> \
    --topic <topic>.kafkafim-dlq --from-beginning --max-messages 5 \
    --property print.headers=true --property print.timestamp=true
  ```

  `kafkafim.error.type` and `kafkafim.error.reason` (the ES item error, cut to 4 KiB)
  tell you why; `kafkafim.source.topic/partition/offset` where it came from
  ([DESIGN §9](DESIGN.md#9-dlq)). By type:

  | `error.type` | Usually | Fix |
  |---|---|---|
  | `es_error` | Mapping conflict, strict mapping, missing ingest pipeline | Fix the mapping/template/ingest pipeline (or the producer) |
  | `es_retries_exhausted` | Item-level 429/5xx or `index_not_found` for longer than `max-attempts`/`max-elapsed` | ES capacity, or the index/template is not provisioned |
  | `decode`, `missing_id`, `bad_timestamp` | Producer sends invalid JSON, a value over `source.max-record-bytes` (oversize records are `decode`), no id, or an unparsable timestamp | Producer side, or the pipeline's `document.*` settings |
  | `no_route`, `unresolved_index` | Routing rules do not cover the document, or an index pattern field is missing/invalid | Routes / producer |
  | `version_conflict` | `on-version-conflict: dlq` and an older version arrived | Usually expected |
  | `too_large` | A single document over ES's `http.max_content_length` | ES setting or producer |

- **Action**: fix the cause; then replay with a pipeline whose source is the DLQ topic
  ([DESIGN §9](DESIGN.md#9-dlq), "Replay is configuration"), deployed like any other
  pipeline (its own DLQ topic must exist). A replay pipeline without
  `document.timestamp-field` uses the original record time
  (`kafkafim.source.timestamp`) for date-based index names
  ([ADR-0027](decisions/0027-dlq-record-timestamp-and-headers.md)). Remove it once its
  consumer group's lag, in the Kafka team's consumer-lag monitoring, is zero. Purge is
  retention; there is no DLQ tool.

**DLQ blocked** (the DLQ cannot be written):

- **Symptom**: pipeline `paused`, WARN `Pipeline '<p>': pausing (dlq-blocked)`, and WARN
  (transient) or ERROR (configuration) `DLQ produce for pipeline '<p>' to topic '<t>'
  (<identity>) failed (<kind>), blocking until it succeeds: ...`. The source partition's
  committed offset does not move: losing data is worse than lag.
- **Check**: configuration failures are a missing DLQ topic, failed SASL
  authentication, a missing ACL, an invalid topic, or a record larger than the
  producer's `max.request.size` (16 MiB default) or the topic's `max.message.bytes`.
  Anything else (broker down, timeouts) is transient.
- **Action**: fix the cause; the send is retried forever (100ms doubling to 30s) and
  the writer logs INFO `DLQ produce for pipeline '<p>' to topic '<t>' recovered`, then
  the pause clears. After an error that kafka-clients *can* make **fatal** (cluster
  authorization, authentication, unsupported version, a fenced producer, an invalid
  producer ID mapping), the shared producer is replaced automatically: WARN `DLQ
  producer <client.id> may be in a fatal state (<Exception>); replacing it`. A missing
  `Write` ACL failing `InitProducerId` is usually *not* fatal in kafka-clients 4.2 (it
  is abortable, and the producer would recover on its own), but kafkafim replaces the
  producer anyway, so this WARN and the kafka-clients ERROR `Aborting producer batches
  due to fatal error` appear on that path too. Neither means data was lost; see
  [DESIGN §7](DESIGN.md#7-backpressure-and-failure-handling). Sends that fail because
  of the replacement are reported with the failure that triggered it
  (`ProducerReplacedException: DLQ producer was being replaced after ...`), so they
  get the trigger's kind (configuration for a missing ACL). Fixing the ACL unblocks
  the next attempt without a restart. For an oversize DLQ record, raise
  `max.request.size` through the Kafka cluster's `properties` together with the DLQ
  topic's `max.message.bytes`.

### 5.7 Pipeline shows `running` but consumes nothing

**Alerts**: [`KafkafimNoAssignedPartitions`](../deploy/grafana/alerts/kafkafim-alerts.yaml).

- **Symptom**: state `running`, but lag grows and `kafkafim_records_consumed_total`
  is flat for that pipeline on that pod.
- **Check**, in order:
  1. Group or topic `Read` authorization errors ([§5.4](#54-kafka-authentication-or-acl-missing)).
  2. `kafkafim_assigned_partitions`: 0 on this pod is normal only if the group has
     more pods than partitions (replicas above partitions are idle).
  3. `kafkafim_in_flight_bytes` at or above `max-in-flight-bytes`:
     backpressure, see [§5.8](#58-consumer-lag-growing).
- **Action**: for 1, fix the ACL ([§5.4](#54-kafka-authentication-or-acl-missing));
  for 2, nothing if the group has more pods than partitions, else check the group's
  other members; for 3, see [§5.8](#58-consumer-lag-growing). A record handler that
  threw or a listener container that stopped on its own no longer shows as `running`:
  the pipeline is `failed` ([§5.13](#513-failed-pipelines)).

### 5.8 Consumer lag growing

**Alerts**: the Kafka team's consumer-lag alert for the pipeline's consumer group,
[`KafkafimE2eLatencyHigh`](../deploy/grafana/alerts/kafkafim-alerts.yaml), [`KafkafimBulkRejectRatioHigh`](../deploy/grafana/alerts/kafkafim-alerts.yaml).

- **Symptom**: the consumer group's lag (`source.group-id`) rising for 5+ minutes in
  the Kafka team's consumer-lag monitoring; `kafkafim_e2e_latency_seconds` p99 above
  target.
- **Check**, in order:
  1. Is the pipeline `paused` on any pod? Then it is [§5.1](#51-elasticsearch-outage-circuit-breaker-open),
     [§5.2](#52-elasticsearch-authentication-failure-pipeline-paused) or
     [§5.6](#56-dlq-filling-or-dlq-blocked).
  2. Is it consuming at all? [§5.7](#57-pipeline-shows-running-but-consumes-nothing).
  3. Is Elasticsearch slow? `kafkafim_bulk_latency_seconds` up, `status="429"` in
     `kafkafim_bulk_requests_total`, `kafkafim_retries_total` rising. With
     `kafkafim_in_flight_bytes` pinned at `max-in-flight-bytes`, kafkafim is waiting
     for ES (backpressure by design); scaling kafkafim will not help, ES capacity will.
  4. Is kafkafim CPU-bound? CPU throttling at the pod limit, in-flight bytes low, ES
     latency normal. Scale the group (`replicas` or the HPA's `maxReplicas`) up to the
     partition count; replicas beyond it get no partitions. Each pod runs as many
     consumers per pipeline as the JVM sees processors, capped at the pipeline's
     partition count, so a CPU limit also caps consumer parallelism.
  5. A traffic spike or a new producer: lag that falls after the spike needs nothing.
- **Action**: as above. Moving a hot pipeline into its own group gives it dedicated
  pods without changing anything in Kafka (same `group-id`,
  [HELM.md](HELM.md#operating-notes)). Repartitioning is a Kafka-side action.

### 5.9 Pod refuses to start

- **Symptom**: `CrashLoopBackOff`, or the startup probe (liveness, 30 × 2s) kills the
  pod. Read the first ERROR of the previous container:

  ```bash
  kubectl -n <ns> logs <pod> --previous | head -50
  ```

- **Check**: the cause is in the message:

  | Message | Cause | Fix |
  |---|---|---|
  | `The 'local' profile (development sample, inline secrets allowed) is active on Kubernetes. Set SPRING_PROFILES_ACTIVE=kafkafim ...` | The `local` profile got activated: a hand-edited Deployment dropped `SPRING_PROFILES_ACTIVE=kafkafim`, or `SPRING_PROFILES_INCLUDE` / `spring.profiles.include` adds `local` (via `extraEnv` or `extraEnvFrom`) | Restore the chart's env; remove the include |
  | `kafkafim.allow-inline-secrets=true is set on Kubernetes. Secrets must come from mounted Secret files ...` | Someone set the local-only switch (e.g. via `extraEnv`) | Remove it; mount secrets as files ([§4.3](#43-rotating-a-secret)) |
  | A binding/validation report listing `<property path>: <problem>` | Invalid configuration: unknown key under `kafkafim.*`, broken reference, an **inline secret** literal in YAML or env, or a `${...}` placeholder with no secret file behind it | Fix the values; for a missing secret check the `secrets[]` mount path against the placeholder name ([HELM.md](HELM.md#secrets-and-configtree-naming)) |
  | `ConfigDataLocationNotFoundException` for `/etc/kafkafim/config/` | The ConfigMap is not mounted | Check the release rendered the group's `ConfigMap` |
  | Pod stuck in `ContainerCreating` with `FailedMount` events (`kubectl describe pod`) | A `secrets[]` entry names a `Secret` that does not exist | Create the `Secret` (the chart never does) |

  Validation lists every violation at once and never prints secret values. The same
  report appears in a rejected reload ([§4.2](#42-reading-the-result)): if running pods
  rejected a change, new pods crash on it.
- Preflight does **not** block startup: it runs per pipeline in the background, so an
  unreachable cluster leaves the pod running but unready, not crash-looping.

### 5.10 Out of memory, back-pressure

- **Symptom**: container restarts with `OOMKilled` (kernel, memory limit), or exits
  after a `java.lang.OutOfMemoryError` (`-XX:+ExitOnOutOfMemoryError` makes the JVM
  exit rather than limp on). Either way, records not yet committed are re-delivered;
  nothing is lost.
- **Check**: per pipeline, `source.max-in-flight-bytes` (default 64MB) bounds the raw
  key + value bytes delivered and not yet completed; the source pauses at the budget
  and resumes at half of it. That bound is **per pipeline and in raw bytes**:
  - a pod's in-flight memory is the sum over all its pipelines;
  - a poll that crosses the budget is delivered whole, so the overshoot is up to one
    poll per consumer (`max-poll-records` records);
  - parsed documents take more heap than their bytes: a JSON array of many small
    objects can retain ~28× its size ([DESIGN §4.5](DESIGN.md#45-decoding)), and each
    record may be up to `max-record-bytes` (default 8MB);
  - heap is `MaxRAMPercentage=75` of the container limit; the rest is metaspace,
    threads, direct buffers.
  `kafkafim_in_flight_bytes` per pipeline against heap use.
- **Action**: lower `max-in-flight-bytes` or `max-poll-records` for the heavy
  pipeline (a config change, hot-reloaded), raise the group's memory limit (rolls the
  pods), or move the pipeline to its own group. A lower budget pauses fetching
  earlier; it costs throughput, not data.

### 5.11 Reload failure alert

**Alerts**: [`KafkafimConfigReloadFailed`](../deploy/grafana/alerts/kafkafim-alerts.yaml).

`increase(kafkafim_config_reload_total{result="failure"}[10m]) > 0`: see
[§4.2](#42-reading-the-result). Find the ERROR `Configuration reload (...) rejected`
(or `... failed while applying generation`) line on any pod of the group for the violations, fix the values and upgrade again,
or roll back. Until then the pods run the previous generation and new pods fail.

### 5.12 Pipeline stuck in `starting`

**Alerts**: [`KafkafimPipelineStuckStarting`](../deploy/grafana/alerts/kafkafim-alerts.yaml).

- **Symptom**: `kafkafim_pipeline_state{state="starting"} == 1` for minutes; WARN
  `Pipeline '<p>': preflight could not complete, retrying: <problems>` (or `source
  could not start, retrying: ...`) repeated with a growing interval. A new pod whose
  every pipeline is `starting` stays unready, so a rollout waits; the old pods keep
  running.
- **Check**: the WARN line names what did not answer:

  | Log line contains | Cause | See |
  |---|---|---|
  | `DLQ topic '<t>' (...): broker did not answer` in `<problems>` | Kafka unreachable for the DLQ identity | [§5.3](#53-kafka-unreachable) |
  | `source could not start, retrying: <KafkaException>` (its own WARN line, after preflight passed) | Kafka unreachable for the source | [§5.3](#53-kafka-unreachable) |
  | `Elasticsearch identity '<id>' on cluster '<es>' not reachable: <exception>` | No connection to the ES cluster within the preflight's short timeouts: DNS, network policy or firewall, ES down | The cluster and the network path from the pod's namespace to its `uris` |
  | `Elasticsearch identity ... not reachable: HTTP 5xx from _security/_authenticate` | ES or a proxy in front of it answers 5xx | The ES cluster's own monitoring |

  Preflight sends one `GET /_security/_authenticate` per Elasticsearch cluster and
  identity the pipeline routes to. A TLS handshake failure, 401 or 403 is not transient: it sends
  the pipeline to `failed_preflight` ([§5.5](#55-failed_preflight-pipelines)).
- **Action**: fix the path or the cluster. Preflight retries on its own (1s doubling
  to 30s, forever, [§3.1](#31-pipeline-state)) and the pipeline goes `running` at the
  next attempt that gets through; no restart or reload is needed. Elasticsearch
  becoming unreachable **after** start is [§5.1](#51-elasticsearch-outage-circuit-breaker-open)
  instead: the pipeline is `paused`, not `starting`.

### 5.13 `failed` pipelines

**Alerts**: [`KafkafimPipelineFailed`](../deploy/grafana/alerts/kafkafim-alerts.yaml)
(per pod, after 1m: the reload that fixes it is per pod).

- **Symptom**: `kafkafim_pipeline_state{state="failed"} == 1`; ERROR
  `Pipeline '<p>': failed while running; stopping it until the next accepted reload or
  a restart: <cause>`, then INFO `Pipeline '<p>': running -> failed` (or `paused ->
  failed`, or `starting -> failed` if it failed right after its source started). The pipeline consumes nothing on this pod. It left the consumer group, so
  the group's other pods take its partitions; records it had not finished are
  re-delivered to them (idempotent by `_id`). After a handler failure the stop waits
  the full drain timeout (30s) for the failed batch before it leaves the group, and a
  shutdown or reload in that time waits with it. Other pipelines of the pod keep running;
  the pod goes unready only if this was its last started pipeline.
- **Check**: the cause, and the ERROR line just before it, which carries the stack
  trace:

  | `<cause>` | Before it | Meaning |
  |---|---|---|
  | `record handler failed: <exception>` | `Pipeline '<p>': record handler failed; pausing until the pipeline is restarted` | Decoding, routing or submitting a batch threw: a kafkafim bug. The batch's records were not committed, so whoever reads them next (a retry, or another pod) may hit it again. |
  | `Kafka consumer stopped (<reason>)` | Spring Kafka's ERROR from the listener container, such as `Stopping container due to an Error` | A consumer hit an error the container does not retry. Authentication and authorization errors are retried every 10s instead ([§5.4](#54-kafka-authentication-or-acl-missing)), so they do not end here. |

- **Action**: collect the ERROR lines ([§6](#6-escalation-data-to-collect)), fix the
  cause if it is outside kafkafim, then run a no-op `POST /actuator/reload` on the pod
  ([§4.1](#41-how-a-change-gets-in)). The pipeline appears under `retried` and goes
  `starting` → `running`. A pipeline that fails again on the same records points to a
  bug: escalate with the logs. A pod restart also recovers it. Nothing retries on its
  own ([ADR-0036](decisions/0036-pipeline-that-fails-while-running-is-stopped-as-failed.md)).

### 5.14 Documents missing from Elasticsearch: dropped or lost?

**Alerts**: none. Dropping is configured behaviour, so there is no default alert
([ADR-0037](decisions/0037-routes-can-drop-documents.md)).

- **Symptom**: someone expects documents in an index that are not there, and the
  pipeline is `running` with no DLQ records for them.
- **Check**:
  1. Does the pipeline have drop routes? Look for `drop: true` in its `routes`
     (`/actuator/configprops`, or the chart values). The startup log warns
     `every route has drop: true` for a pipeline that indexes nothing, and
     `cancels every other route` for a `route-mode: all` drop route without `when`.
  2. `sum by (route) (rate(kafkafim_docs_total{pipeline="<p>",outcome="dropped"}[5m]))`.
     Every dropped document is counted once, under the label of the drop route that
     selected it (in `route-mode: all`, the first matching drop route). The dashboard's
     "Dropped documents" panel shows the same thing. kafkafim logs nothing per dropped
     document.
  3. Compare the totals: over the same window, the records consumed should equal
     `indexed + stale + dlq + dropped` for an object-per-record topic. Array records
     add one count per element, and `route-mode: all` adds one per extra copy. If the
     documents are not under `dropped` and not in the DLQ
     ([§5.6](#56-dlq-filling-or-dlq-blocked)), they were not consumed yet, or the
     records were never produced. Apart from a drop route, every document of a
     committed record is in Elasticsearch or in the DLQ
     ([ADR-0006](decisions/0006-at-least-once-with-id-idempotence.md)).
- **Action**: if a drop route matches more than intended, narrow its `when` or move it
  below the indexing route that should win (`route-mode: first` decides by order). The
  reload restarts only that pipeline and applies to records not yet committed. Dropped
  documents are gone from kafkafim's point of view: to get them back, re-read the topic
  with a temporary pipeline that has the corrected routes, a new `source.group-id` and
  `offset-reset: earliest`, while the topic still retains them. `_id` idempotence makes
  the overlap with what is already indexed harmless.

## 6. Escalation data to collect

Before escalating to the kafkafim maintainers, collect for the affected pod(s):

- `kubectl -n <ns> describe pod <pod>` and `kubectl -n <ns> logs <pod>` (plus
  `--previous` after a restart);
- `curl -s localhost:8080/actuator/prometheus | grep '^kafkafim_'` (port-forwarded);
- `helm history kafkafim -n <ns>` and the values diff of the last revision;
- the consumer group description from Kafka tooling.

Logs never contain document bodies or secret values; they are safe to attach.

## Known gaps

Current behaviour that this runbook works around; each is a follow-up, not a feature:

- **A missing topic `Read` ACL shows `running`.** The start check fetches metadata
  only (`Describe`); fetches then fail and are retried every 10s.
- **A denied consumer-group join shows `running`.** The group permission is not part
  of preflight; the source retries the join every 10s.
- **Rotated TLS files are not reloaded.** Certificate and keystore files are bound
  by path, so a content change with an unchanged path and password is `unchanged` to
  the reload; restart the pods ([§4.3](#43-rotating-a-secret)).
- **Pause reasons are only in logs.** The metric and the health endpoint say `paused`
  without the reason; health details are hidden by `show-details: when-authorized`.
- **No `validate` profile yet** ([DESIGN §5.6](DESIGN.md#56-validation-and-hot-reload),
  roadmap 0.2): an invalid configuration is found only by a running pod's reload or a
  new pod's startup.
