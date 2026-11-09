# Grafana dashboards and Prometheus alert rules

What [DESIGN §10](../../docs/DESIGN.md#shipped-with-the-repo) promises under "Shipped
with the repo". Every query uses the metric names and labels of `/actuator/prometheus`
([DESIGN §10](../../docs/DESIGN.md#10-observability), [RUNBOOK §3.3](../../docs/RUNBOOK.md#33-metrics-to-look-at-first)).

| File | What |
|---|---|
| [`dashboards/kafkafim-overview.json`](dashboards/kafkafim-overview.json) | Per pipeline: state, consumer lag, end-to-end latency, records/bytes/documents, DLQ, in-flight bytes and heap, assigned partitions, Kafka connectivity, config generation and reloads |
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
| `KafkafimPipelinePaused` | a pipeline is `paused` (breaker, identity, DLQ blocked) for 5m | warning | [§3.1](../../docs/RUNBOOK.md#31-pipeline-state) |
| `KafkafimPipelineStuckStarting` | a pipeline is still `starting` (preflight retrying a transient failure) after 10m | warning | [§3.1](../../docs/RUNBOOK.md#31-pipeline-state) |
| `KafkafimConsumerLagGrowing` | lag summed over pods is above **1000** (tune) and rises (`deriv` over 5m) for 5m | warning | [§5.8](../../docs/RUNBOOK.md#58-consumer-lag-growing) |
| `KafkafimE2eLatencyHigh` | end-to-end p99 over 5m above **10s** for 10m (tune) | warning | [§5.8](../../docs/RUNBOOK.md#58-consumer-lag-growing) |
| `KafkafimDlqRecords` | DLQ rate > 0 for 10m, per `error_type` | warning | [§5.6](../../docs/RUNBOOK.md#56-dlq-filling-or-dlq-blocked) |
| `KafkafimNoAssignedPartitions` | a pod runs a pipeline but owns none of its partitions for 10m | info | [§5.7](../../docs/RUNBOOK.md#57-pipeline-shows-running-but-consumes-nothing) |
| `KafkafimKafkaDisconnected` | `kafkafim_kafka_connected` is 0 for a running or paused pipeline for 2m | critical | [§5.3](../../docs/RUNBOOK.md#53-kafka-unreachable) |
| `KafkafimBreakerOpen` | `kafkafim_breaker_state` is 2 (open) for 1m | critical | [§5.1](../../docs/RUNBOOK.md#51-elasticsearch-outage-circuit-breaker-open) |
| `KafkafimBulkRejectRatioHigh` | more than **1%** of bulk requests to a cluster answered 429 for 5m (tune) | warning | [§5.8](../../docs/RUNBOOK.md#58-consumer-lag-growing) |
| `KafkafimEsAuthFailures` | any 401/403 in the last 5m (the first one too) | critical | [§5.2](../../docs/RUNBOOK.md#52-elasticsearch-authentication-failure-pipeline-paused) |
| `KafkafimConfigReloadFailed` | a reload was rejected in the last 10m | warning | [§5.11](../../docs/RUNBOOK.md#511-reload-failure-alert) |

Notes:

- `KafkafimNoAssignedPartitions` is expected on pods beyond a pipeline's partition
  count; silence it for such groups (severity `info`).
- `kafkafim_auth_failures_total` and `kafkafim_dlq_records_total` appear with their
  first event, so `increase()` alone misses the first increment; the auth alert has a
  second clause for a series that is new in the window. The DLQ alert needs a sustained
  rate and does not.
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

Through OTLP, every name matches `/actuator/prometheus` except the two timers:
Micrometer's OTLP registry exports times in milliseconds by default, so otel-lgtm
stores `kafkafim_bulk_latency_milliseconds_*` and `kafkafim_e2e_latency_milliseconds_*`
and the latency panels stay empty. Export seconds to fill them:

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments=--management.otlp.metrics.export.base-time-unit=seconds
```

The OTLP series also carry no `instance` label (otel-lgtm's Prometheus derives it from
`service.instance.id`, which kafkafim does not set), so per-pod legends are blank with
a single local app; `job` is `kafkafim/kafkafim`.
