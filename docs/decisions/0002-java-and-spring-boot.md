# ADR-0002: Java 27 on Spring Boot 4

Status: accepted · Date: 2026-11-07

## Context
The team runs JVM infrastructure today (Logstash, Elasticsearch, Kafka). The initial
design draft proposed Go; the team chose the JVM.

## Decision
- **Java 27**, current release line, `--release 27`, virtual threads available for
  I/O-bound sender pools.
- **Spring Boot 4.1.x** via Spring Initializr: dependency management, Actuator
  (health probes, Prometheus endpoint), built-in OpenTelemetry starter, Kafka starter,
  graceful shutdown, Docker Compose and Testcontainers integration.
- Plain Java, not Kotlin: broadest familiarity, no extra toolchain.

## Consequences
- Larger image and slower cold start than Go. Mitigated with CDS/AppCDS and
  layered jars; GraalVM native image is a possible later step, not a goal.
- We stay on the Boot BOM for every library it manages (Kafka clients, ES REST
  client, Jackson 3, OpenTelemetry, Testcontainers) and pin only the few it does not.
