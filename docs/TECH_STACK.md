# Tech stack

Versions are the ones pinned at project creation (2026-11-07). Everything marked
*BOM* is managed by the Spring Boot parent; do not pin it in `pom.xml`.

| Area | Choice | Version | Why | ADR |
|---|---|---|---|---|
| Language | Java | 27 | Team runs the JVM; current release line | [0002](decisions/0002-java-and-spring-boot.md) |
| Framework | Spring Boot | 4.1.2 | Config binding + validation, Actuator, OTel starter, Kafka starter, graceful shutdown | [0002](decisions/0002-java-and-spring-boot.md) |
| Build | Maven (wrapper 3.3.4 → Maven 3.10), single module, `${revision}` versioning, Surefire + Failsafe | — | Low ceremony, no local install, no version-bump commits | [0003](decisions/0003-maven-single-module.md) |
| Code style | Google Java Style via Spotless (`google-java-format`) + Checkstyle (`google_checks.xml`) | see pom | Formatting is mechanical and fails the build in `validate`; conventions checked by Checkstyle | [0024](decisions/0024-google-java-style.md) |
| Architecture tests | ArchUnit JUnit 5 | 1.5.1 (1.4.1 cannot read Java 27 class files) | Enforce package boundaries so a later module split is mechanical | [0003](decisions/0003-maven-single-module.md) |
| Configuration | `@ConfigurationProperties` + Bean Validation, Helm-rendered ConfigMap, `configtree` secrets | BOM | One config system; secrets never in ConfigMaps | [0012](decisions/0012-spring-configuration-helm-configtree-hot-reload.md) |
| Kafka | `spring-boot-starter-kafka` → spring-kafka + `kafka-clients` | BOM (clients 4.2.x) | Programmatic containers, cooperative-sticky, pause/resume; own out-of-order offset tracker | [0004](decisions/0004-spring-kafka-programmatic-containers.md), [0025](decisions/0025-own-offset-tracker-instead-of-async-acks.md) |
| Elasticsearch | `co.elastic.clients:elasticsearch-rest5-client` (low-level, HttpClient 5) | BOM (9.4.x). HTTP-only, no compatibility headers, works with 8.x | Version-agnostic; own bulk builder and per-item parsing | [0005](decisions/0005-elasticsearch-low-level-rest-client.md) |
| JSON | Jackson 3 (`tools.jackson`) | BOM | Record decoding, array explosion, JSON-pointer field extraction, bulk item parsing | — |
| Routing | Declarative rules + Logstash-style index patterns (own code, `DateTimeFormatter`) | — | No expression language in the MVP | [0009](decisions/0009-declarative-routing-no-expression-language.md) |
| Resilience | `resilience4j-circuitbreaker` (brings `resilience4j-core`) | 2.3.0 | Per-ES-cluster circuit breaker and backoff intervals only (`resilience4j-retry` is not used); the bulk retry loop itself is the sink's own | [0016](decisions/0016-resilience4j-for-breaker-and-retry.md), [0026](decisions/0026-bulk-retry-loop-in-the-sink.md) |
| Metrics | Micrometer → OTLP + Prometheus registry | BOM | OTLP to Elastic APM Server; `/actuator/prometheus` for free | [0013](decisions/0013-observability-otel-via-spring-boot.md) |
| Tracing | Micrometer Tracing + OpenTelemetry bridge → OTLP | BOM (OTel 1.62) | Bulk-level spans, span links from Kafka headers | [0013](decisions/0013-observability-otel-via-spring-boot.md) |
| Logs | Spring Boot structured logging (ECS JSON) | BOM | Native to the Elastic stack | [0013](decisions/0013-observability-otel-via-spring-boot.md) |
| Web | `spring-boot-starter-webmvc` | BOM | Actuator endpoints and the reload endpoint only | — |
| Validation | `spring-boot-starter-validation` | BOM | Bean Validation of the bound config | [0012](decisions/0012-spring-configuration-helm-configtree-hot-reload.md) |
| Tests | JUnit 5, Spring Boot Test, Testcontainers (Kafka, Elasticsearch, LGTM), Awaitility | BOM (Testcontainers 2.0.x) | End-to-end and chaos cases against real brokers | [0019](decisions/0019-testing-strategy.md) |
| Local stack | Docker Compose (Kafka 4.1 native, Elasticsearch 8.19, Grafana LGTM) | — | Auto-started by `spring-boot:run` | [0015](decisions/0015-local-dev-compose-and-k8s-demo.md) |
| Container image | `eclipse-temurin:27-jre`, Spring Boot layered jar extracted with `jarmode=tools`, UID 65532, read-only FS | 27-jre | Runtime-only Dockerfile wraps the tested jar; distroless has no Java 27 yet | [0022](decisions/0022-delivery-github-actions-ghcr-airgap.md) |
| Registries | GHCR: `ghcr.io/matabar/kafkafim` (image), `oci://ghcr.io/matabar/charts/kafkafim` (chart) | — | GitHub-native, free for public repos, OCI for both | [0022](decisions/0022-delivery-github-actions-ghcr-airgap.md) |
| Supply chain | Cosign (keyless), Syft SBOM, BuildKit provenance, Dependabot | — | Verify on the internet side before carrying into the air gap | [0022](decisions/0022-delivery-github-actions-ghcr-airgap.md) |
| Deploy | Helm chart, one release per Kubernetes cluster, plain `Deployment`/`ConfigMap`/`PDB`/`Service`/`HPA` | — | No CRDs or operators in production; no Secrets created | [0023](decisions/0023-helm-chart-one-release-per-cluster.md) |
| CI | GitHub Actions: `./mvnw verify`, Dockerfile build + hadolint, `helm lint`/`template`/`kubeconform`/`unittest` | — | All required for merge to `main` | [0022](decisions/0022-delivery-github-actions-ghcr-airgap.md) |
| CD | GitHub Actions on `v*` tags: multi-arch image, chart, SBOM, signatures, air-gap bundle on the GitHub Release | — | Production is air-gapped; CD publishes, humans import | [0022](decisions/0022-delivery-github-actions-ghcr-airgap.md) |

## Explicitly not used

- **CEL / SpEL / any expression language**: routing is rules (ADR-0009, ADR-0020).
- **picocli / CLI subcommands**: one run mode plus a `validate` profile (ADR-0018).
- **Own YAML loader + JSON Schema**: Spring's binder and Bean Validation are the
  config system (ADR-0012).
- **Spring Data Elasticsearch / typed `elasticsearch-java` client**: single-cluster
  assumption and server-major coupling (ADR-0005).
- **`@KafkaListener` annotations**: pipelines are dynamic (ADR-0004).
- **OpenTelemetry Java agent**: the Boot starter covers it (ADR-0013).
- **Spring Cloud (Kubernetes, Circuit Breaker)**: extra BOM, no benefit (ADR-0012, ADR-0016).
- **Kotlin, Gradle, GraalVM native**: not now; none is precluded.

## Source layout (planned)

```
io.github.matabar.kafkafim
├── config/         @ConfigurationProperties model, validators, watcher, Binder reload, diff
├── supervisor/     pipeline lifecycle, hot reload orchestration
├── source/kafka/   consumer factories per cluster, container per pipeline, offset tracker
├── decode/         object / array-of-objects decoding, JSON-pointer field extraction, timestamp parsing
├── route/          condition evaluation, index pattern resolution
├── sink/es/        Rest5Client per (cluster, identity), batcher + bulk sender, NDJSON body builder, item parser, error table, breaker per cluster
├── dlq/            producer, record format
└── telemetry/      meters, spans, health indicators, reload endpoint
```
