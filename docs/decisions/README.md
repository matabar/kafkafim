# Architecture Decision Records

One file per decision, MADR-style. Supersede with a new ADR rather than editing history.

| ADR | Decision |
|---|---|
| [0001](0001-build-instead-of-logstash-connect-vector.md) | Build kafkafim instead of keeping Logstash or adopting Kafka Connect / Vector |
| [0002](0002-java-and-spring-boot.md) | Java 27 on Spring Boot 4 |
| [0003](0003-maven-single-module.md) | Maven, a single module, CI-friendly versioning, enforced package boundaries |
| [0004](0004-spring-kafka-programmatic-containers.md) | Spring for Apache Kafka with programmatic listener containers |
| [0005](0005-elasticsearch-low-level-rest-client.md) | Elasticsearch low-level REST client; we build the bulk body ourselves |
| [0006](0006-at-least-once-with-id-idempotence.md) | At-least-once delivery with ES-side idempotence via `_id` |
| [0007](0007-deployment-groups-no-crds.md) | Deployment groups: plain Deployments, no CRDs, operators or storage |
| [0008](0008-batching-per-es-cluster-with-offset-tracker.md) | Batch per ES cluster; track completion per partition |
| [0009](0009-declarative-routing-no-expression-language.md) | Declarative routing rules with Logstash-style index patterns; no expression language |
| [0010](0010-kafka-as-the-only-buffer-backpressure.md) | Kafka is the only buffer; backpressure by pausing consumption |
| [0011](0011-dlq-on-source-kafka-cluster.md) | DLQ topic on the source Kafka cluster; original bytes preserved; poison messages bounded (replay-lag backlog number superseded by 0034) |
| [0012](0012-spring-configuration-helm-configtree-hot-reload.md) | Configuration is Spring Boot configuration rendered by Helm; secrets via `configtree`; hot reload with `Binder` |
| [0013](0013-observability-otel-via-spring-boot.md) | Observability through Spring Boot's OpenTelemetry starter, OTLP to Elastic APM Server, plus Prometheus endpoint |
| [0014](0014-scaling-static-plus-cpu-hpa.md) | Scaling: static replicas per group, optional CPU HPA, lag metric exported for future KEDA (lag metric superseded by 0034) |
| [0015](0015-local-dev-compose-and-k8s-demo.md) | Local development with Docker Compose; demo on Docker Desktop Kubernetes; production chart deploys only kafkafim |
| [0016](0016-resilience4j-for-breaker-and-retry.md) | Resilience4j core for circuit breaker and retry |
| [0017](0017-migration-from-logstash.md) | Migration from Logstash: run in parallel on the same `_id`s, then stop Logstash |
| [0018](0018-no-cli-single-run-mode.md) | No CLI in the MVP; one run mode plus a `validate` profile |
| [0019](0019-testing-strategy.md) | Testing: unit tests for classification and routing, Testcontainers for end-to-end, explicit chaos cases |
| [0020](0020-mvp-scope.md) | MVP scope — no transformations, no expressions, no CLI |
| [0021](0021-no-resource-provisioning.md) | kafkafim never provisions Kafka or Elasticsearch resources |
| [0022](0022-delivery-github-actions-ghcr-airgap.md) | Delivery via GitHub Actions and GHCR; signed, versioned air-gap bundles |
| [0023](0023-helm-chart-one-release-per-cluster.md) | One Helm chart, one release per Kubernetes cluster, groups rendered as Deployments |
| [0024](0024-google-java-style.md) | Google Java Style, enforced by Spotless and Checkstyle |
| [0025](0025-own-offset-tracker-instead-of-async-acks.md) | Own per-partition offset tracker instead of spring-kafka `asyncAcks` (supersedes that part of 0004) |
| [0026](0026-bulk-retry-loop-in-the-sink.md) | Bulk retries are a small loop in the sink; Resilience4j supplies the breaker and the backoff (refines 0016) |
| [0027](0027-dlq-record-timestamp-and-headers.md) | DLQ records carry the send time; the original timestamp and a fresh `kafkafim.*` header set travel in headers (refines 0011) |
| [0028](0028-hot-reload-rereads-config-data-and-shares-building-blocks-per-cluster.md) | Hot reload re-runs Spring Boot's config data loading and shares building blocks per cluster definition (refines 0012) |
| [0029](0029-batch-spans-recorded-at-batch-end-failed-batches-always-sampled.md) | Batch spans are recorded when the batch ends, failed batches are always sampled, and the supervisor feeds telemetry (refines 0013) |
| [0030](0030-request-level-retry-logs-error-types-not-response-bodies.md) | Request-level retry logs name Elasticsearch's error types, not the response body; the body only at DEBUG (refines 0026) |
| [0031](0031-elasticsearch-client-timeouts-per-cluster.md) | Elasticsearch client timeouts are set per cluster; the breaker's half-open limit follows the response timeout (refines 0026) |
| [0032](0032-accepted-reload-retries-failed-preflight-pipelines.md) | An accepted reload retries the pipelines it leaves in `failed_preflight` (refines 0028) |
| [0033](0033-consumer-lag-read-per-pipeline-in-parallel-staleness-as-timestamp.md) | Consumer lag is read per pipeline in parallel under a 5s deadline; stale lag shows as an old `kafkafim_consumer_lag_timestamp_seconds` (refines 0029) — **superseded by 0034** |
| [0034](0034-consumer-lag-is-monitored-outside-kafkafim.md) | Consumer lag is monitored outside kafkafim; kafkafim reads and exports none (supersedes 0033 and the lag parts of 0014 and 0011) |
| [0035](0035-dlq-writer-evicted-when-its-pipeline-leaves-the-instance.md) | A DLQ writer is evicted when its pipeline leaves its `DlqProducers` instance (refines 0028) |
| [0036](0036-pipeline-that-fails-while-running-is-stopped-as-failed.md) | A pipeline that fails while running (handler threw, consumer stopped on its own) is stopped in a new `failed` state; an accepted reload retries it (refines 0032) |
| [0037](0037-routes-can-drop-documents.md) | Routes can drop documents (`drop: true`); in `route-mode: all` a drop cancels every other copy; no `allow-drop` switch (refines 0009 and 0006) |
| [0038](0038-preflight-checks-consumer-group-permission.md) | Preflight checks the consumer group permission with one `DescribeGroups` (authorized operations); a never-joined group passes (refines 0021) |
| [0039](0039-preflight-checks-read-on-the-source-topics.md) | Preflight checks `Read` on the source topics with the group check's admin client (`describeTopics`, authorized operations); missing or invisible topics stay with the source's start (refines 0038) |
| [0040](0040-validate-profile-accepts-unmounted-secrets.md) | The `validate` profile accepts secrets that are not mounted and lists the names it expects; it starts no pipeline, web server or telemetry export (refines 0012 and 0018) |
| [0041](0041-logs-over-otlp-stdout-is-readable-text.md) | Logs are exported over OTLP with metrics and traces; stdout is readable text, never JSON (supersedes the logs part of 0013) |
| [0042](0042-local-kubernetes-demo-is-one-umbrella-chart.md) | The local Kubernetes demo is one umbrella chart (`deploy/local/kafkafim-demo`): Kafka, Elasticsearch, Grafana LGTM and `charts/kafkafim` as a subchart in a single `helm install` (refines 0015) |
