# Grafana dashboards and Prometheus alert rules

What [DESIGN §10](../../docs/DESIGN.md#shipped-with-the-repo) promises under "Shipped
with the repo". Every query uses the metric names and labels of `/actuator/prometheus`
([DESIGN §10](../../docs/DESIGN.md#10-observability), [RUNBOOK §3.3](../../docs/RUNBOOK.md#33-metrics-to-look-at-first)).

| File | What |
|---|---|
| [`dashboards/kafkafim-overview.json`](dashboards/kafkafim-overview.json) | Per pipeline: state and pause reasons, end-to-end latency, records/bytes/documents, dropped documents per route, DLQ, in-flight bytes and heap, assigned partitions, Kafka connectivity, config generation and reloads |
| [`dashboards/kafkafim-elasticsearch.json`](dashboards/kafkafim-elasticsearch.json) | Per Elasticsearch cluster: breaker state, bulk requests by status, 429 ratio, bulk latency, docs and bytes per request, flush reasons, retries, item errors, auth failures |
| [`alerts/kafkafim-alerts.yaml`](alerts/kafkafim-alerts.yaml) | Prometheus alerting rules (below) |
| [`alerts/kafkafim-alerts.test.yaml`](alerts/kafkafim-alerts.test.yaml) | `promtool test rules` unit tests for every alert |
| [`provisioning/kafkafim-dashboards.yaml`](provisioning/kafkafim-dashboards.yaml) | Grafana dashboard provider used by the compose demo |

## Dashboards

Both dashboards have a `datasource` variable (any Prometheus-compatible data source)
and multi-value `pipeline` / `es_cluster` variables. Import them through
*Dashboards → New → Import*, or provision them with a file provider like
`provisioning/kafkafim-dashboards.yaml` pointed at a copy of `dashboards/`. The
UIDs are fixed (`kafkafim-overview`, `kafkafim-elasticsearch`), so links survive a
re-import. To change a panel, edit it in Grafana, export the JSON (*Share → Export*,
without "export for sharing externally") and commit it here.

Counters and histograms use `$__rate_interval`; set the data source's scrape interval
to the real one (Prometheus default 1m, the OTLP export step is 15s).

## Alerts

| Alert | Fires when | Severity | Runbook |
|---|---|---|---|
| `KafkafimPipelineFailedPreflight` | a pipeline is `failed_preflight` on any pod for 1m | critical | [§5.5](../../docs/RUNBOOK.md#55-failed_preflight-pipelines) |
| `KafkafimPipelineFailed` | a pipeline is `failed` (stopped after failing while running) on a pod for 1m, per pod | critical | [§5.13](../../docs/RUNBOOK.md#513-failed-pipelines) |
| `KafkafimPipelinePaused` | a pipeline is `paused` (breaker, identity, DLQ blocked) for 5m | warning | [§3.1](../../docs/RUNBOOK.md#31-pipeline-state) |
| `KafkafimPipelineStuckStarting` | a pipeline is still `starting` (preflight retrying a transient failure) after 10m | warning | [§5.12](../../docs/RUNBOOK.md#512-pipeline-stuck-in-starting) |
| `KafkafimE2eLatencyHigh` | end-to-end p99 over 5m above **10s** for 10m (tune) | warning | [§5.8](../../docs/RUNBOOK.md#58-consumer-lag-growing) |
| `KafkafimDlqRecords` | DLQ rate > 0 for 10m, per `error_type` | warning | [§5.6](../../docs/RUNBOOK.md#56-dlq-filling-or-dlq-blocked) |
| `KafkafimNoAssignedPartitions` | a pod runs a pipeline but owns none of its partitions for 10m | info | [§5.7](../../docs/RUNBOOK.md#57-pipeline-shows-running-but-consumes-nothing) |
| `KafkafimKafkaDisconnected` | `kafkafim_kafka_connected` is 0 for a running or paused pipeline for 2m | critical | [§5.3](../../docs/RUNBOOK.md#53-kafka-unreachable) |
| `KafkafimBreakerOpen` | `kafkafim_breaker_state` is 2 (open) for 1m | critical | [§5.1](../../docs/RUNBOOK.md#51-elasticsearch-outage-circuit-breaker-open) |
| `KafkafimBulkRejectRatioHigh` | more than **1%** of bulk requests to a cluster answered 429 for 5m (tune) | warning | [§5.8](../../docs/RUNBOOK.md#58-consumer-lag-growing) |
| `KafkafimEsAuthFailures` | any 401/403 in the last 5m (the first one too) | critical | [§5.2](../../docs/RUNBOOK.md#52-elasticsearch-authentication-failure-pipeline-paused) |
| `KafkafimConfigReloadFailed` | a reload was rejected in the last 10m | warning | [§5.11](../../docs/RUNBOOK.md#511-reload-failure-alert) |

Notes:

- There is no consumer-lag panel or alert: kafkafim exports no lag
  ([ADR-0034](../../docs/decisions/0034-consumer-lag-is-monitored-outside-kafkafim.md)).
  Lag and its alerts come from the Kafka team's consumer-lag monitoring, per consumer
  group (`source.group-id`, default `kafkafim.<pipeline>`).
- There is no alert on dropped documents: a `drop: true` route discards documents on
  purpose ([ADR-0037](../../docs/decisions/0037-routes-can-drop-documents.md)). The
  overview's "Dropped documents by route" panel shows
  `kafkafim_docs_total{outcome="dropped"}` per drop route, and the DLQ ratio leaves
  dropped documents out of its denominator. [RUNBOOK §5.14](../../docs/RUNBOOK.md#514-documents-missing-from-elasticsearch-dropped-or-lost)
  tells dropped from lost.
- `KafkafimNoAssignedPartitions` is expected on pods beyond a pipeline's partition
  count; silence it for such groups (severity `info`).
- Some counters appear with their first event instead of at 0: DLQ records, item
  errors, retries, bulk requests by status and flushes by reason. `rate()` and
  `increase()` need two samples, so they miss that first increment.
  `kafkafim_auth_failures_total` exists from startup with 0 for every Elasticsearch
  cluster and identity a pipeline's routes write with, so `increase()` sees a 401/403
  once the pod has been scraped. The auth alert and the "Auth failures" panel still add
  the starting value of a series that is new in the window, for a 401/403 before a pod's
  first scrape: a single one matters, and nothing else shows it. The panel is
  approximate (`increase()` extrapolates), and a scrape gap longer than the window plus
  5m makes an old series look new, so the panel then shows its total once; a single
  failed scrape does not. "DLQ ratio" shows the first DLQ'd document, because
  `kafkafim_docs_total` starts at 0 for every configured route and outcome and for
  `route="none"`, `outcome="dlq"` (failed before a route was chosen). The other
  counters come in streams, where one missed event doesn't matter. The DLQ alert needs a
  sustained rate, so it has no such term.
- Rules group by `job`/`instance` where a per-pod answer matters; relabel to your own
  pod label if your scrape config uses another one.
- Each alert carries a `runbook_url` annotation to the matching RUNBOOK section.

Load the rules into Prometheus (`rule_files:`), or into a Prometheus Operator
`PrometheusRule` by wrapping the `groups:` list.

## Validate

```bash
jq empty deploy/grafana/dashboards/*.json
```

```bash
nix build --no-link --print-out-paths nixpkgs#prometheus.cli   # promtool is in the cli output
```

```bash
promtool check rules deploy/grafana/alerts/kafkafim-alerts.yaml
```

```bash
(cd deploy/grafana/alerts && promtool test rules kafkafim-alerts.test.yaml)
```

## Compose demo

`compose.yaml` mounts `dashboards/` and the provider into `grafana/otel-lgtm`, so
`./mvnw spring-boot:run` shows both dashboards at <http://localhost:3000> in the
*kafkafim* folder. The app pushes metrics over OTLP there, and otel-lgtm's
Prometheus stores them under the names below. The alert rules are not loaded in the
demo (otel-lgtm's Prometheus has no `rule_files`).

Through OTLP every name matches `/actuator/prometheus`, the timers included: the app
exports times in seconds (`management.otlp.metrics.export.base-time-unit: seconds`), so
otel-lgtm stores `kafkafim_e2e_latency_seconds_*` and `kafkafim_bulk_latency_seconds_*`
([DESIGN §10](../../docs/DESIGN.md#10-observability)). `job` is `kafkafim/kafkafim`
(`service.namespace`/`service.name`), and `instance` comes from `service.instance.id`:
the pod name on Kubernetes (`POD_NAME` from the chart), the container's `HOSTNAME` in a
plain container, and `localhost` for `./mvnw spring-boot:run` without either.
The one OTLP-only difference is the timers' max gauge, `kafkafim_e2e_latency_max_seconds`
(`kafkafim_e2e_latency_seconds_max` on `/actuator/prometheus`); no query here uses it.
