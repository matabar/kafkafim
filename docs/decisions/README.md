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
| [0011](0011-dlq-on-source-kafka-cluster.md) | DLQ topic on the source Kafka cluster; original bytes preserved; poison messages bounded |
| [0012](0012-spring-configuration-helm-configtree-hot-reload.md) | Configuration is Spring Boot configuration rendered by Helm; secrets via `configtree`; hot reload with `Binder` |
| [0013](0013-observability-otel-via-spring-boot.md) | Observability through Spring Boot's OpenTelemetry starter, OTLP to Elastic APM Server, plus Prometheus endpoint |
| [0014](0014-scaling-static-plus-cpu-hpa.md) | Scaling: static replicas per group, optional CPU HPA, lag metric exported for future KEDA |
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
