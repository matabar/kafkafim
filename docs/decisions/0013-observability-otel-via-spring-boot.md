# ADR-0013: Observability through Spring Boot's OpenTelemetry starter, OTLP to Elastic APM Server, plus Prometheus endpoint

Status: accepted · Date: 2026-11-07

## Decision
- Metrics: Micrometer → OTLP (APM Server) and `/actuator/prometheus`.
- Traces: Micrometer Tracing with the OpenTelemetry bridge → OTLP. One span per bulk
  request under a batch span; incoming `traceparent` Kafka headers become **span
  links**, not parents. Head sampling 1% of batches, errors always.
- Logs: ECS-structured JSON on stdout (Spring Boot structured logging), OTLP log
  export optional.
- Label discipline: `route` is the route name or position, never the resolved index.
  Partition labels are opt-in.
- Health: `/actuator/health/liveness` = process alive; `/readiness` = config valid
  and Kafka clients connected, deliberately **not** gated on ES health.
- Repo ships Grafana dashboards and alert rules (`deploy/grafana/`).

## Alternatives
OpenTelemetry Java agent (zero-code). Rejected as the primary path: the Boot starter
gives the same OTLP output with explicit, testable instrumentation and no agent to
manage in the image; the agent remains compatible if ever wanted.
