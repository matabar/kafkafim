# ADR-0003: Maven, a single module, CI-friendly versioning, enforced package boundaries

Status: accepted · Date: 2026-11-07

## Decision
- **Maven** with the wrapper (`./mvnw`), no local install. Lowest ceremony; the
  Spring Boot parent does the heavy lifting; a platform team edits the build rarely.
- **Single module.** There is one deployable (the worker) and no second consumer of
  any library code. A multi-module build would add ceremony with nothing to separate.
- **Package boundaries enforced by ArchUnit tests** (`config`, `supervisor`,
  `source.kafka`, `decode`, `route`, `sink.es`, `dlq`, `telemetry`), so a later split
  into modules is mechanical. Dependencies flow inward: `supervisor` may use
  everything; `sink.es`, `source.kafka`, `dlq` depend on `config` and `telemetry`
  only; `route` and `decode` depend on nothing but the JDK and Jackson.
- **CI-friendly versioning**: `<version>${revision}</version>` with default
  `0.1.0-SNAPSHOT`; releases pass `-Drevision=<tag>`. No version-bump commits.
- **Surefire defaults minus `*IntegrationTest`, Failsafe for `*IntegrationTest`** so unit tests stay fast and
  Testcontainers-based tests run in the `integration-test` phase.

## When to split
A second deployable (for example DLQ tooling), or a library another service wants.
Then: `kafkafim-core` (decode, route, sink, dlq), `kafkafim-app`, `kafkafim-tools`.
Not before.
