# ADR-0016: Resilience4j core for circuit breaker and retry

Status: accepted · Date: 2026-11-07

## Decision
`resilience4j-circuitbreaker` and `resilience4j-retry` (plain Java, no Spring Cloud
starter) implement the per-ES-cluster breaker and the per-item retry policy.

## Rationale
Well-tested state machines with Micrometer binding; avoids pulling the Spring Cloud
BOM into a Boot 4 project for two classes' worth of behaviour.
