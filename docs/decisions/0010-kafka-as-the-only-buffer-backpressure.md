# ADR-0010: Kafka is the only buffer; backpressure by pausing consumption

Status: accepted · Date: 2026-11-07

## Decision
No disk queue, no in-memory overflow buffer. When an ES cluster is slow or down:
exponential backoff with jitter, per-cluster circuit breaker (Resilience4j), and the
pipeline's containers are paused once in-flight bytes hit the budget. Kafka lag grows
and is the alert signal.

## Consequences
Maximum tolerable ES outage = topic retention. startup validation cannot know retention, so
the runbook requires checking it per topic. A pipeline fanning out to two clusters
stalls on both if one is down; accepted, since the alternative is data loss or
reordering.
