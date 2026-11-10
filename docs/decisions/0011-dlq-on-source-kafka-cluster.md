# ADR-0011: DLQ topic on the source Kafka cluster; original bytes preserved; poison messages bounded

Status: accepted; the replay pipeline's `kafkafim_consumer_lag` as the backlog number is superseded by [ADR-0034](0034-consumer-lag-is-monitored-outside-kafkafim.md) · Date: 2026-11-07

## Decision
Every document that cannot be indexed goes to a DLQ topic on the **same Kafka
cluster as the source** (default `<first topic>.kafkafim-dlq`). The record value is
the original bytes, untouched; the error and provenance travel in headers
(`kafkafim.pipeline`, `kafkafim.error.type`, `kafkafim.error.reason`,
`kafkafim.source.topic/partition/offset`, `kafkafim.target.cluster/index`,
`kafkafim.attempts`, `kafkafim.first-failure-ts`, `kafkafim.doc-index`).

Classification:
- Non-retryable item errors (400 family: mapping, parsing, illegal argument) → DLQ now.
- Retryable **item** errors inside a successful bulk response (e.g.
  `es_rejected_execution_exception`) → retry; after `max-attempts` or `max-elapsed`
  → DLQ. This is the poison-message bound: a partition always progresses.
- Request-level failures (connection errors, 5xx, request-level 429, circuit breaker
  open, 401/403) are retried indefinitely with backoff and never count toward that
  bound: an Elasticsearch outage produces Kafka lag, never DLQ records (ADR-0010).
- Undecodable record, missing `_id`, no matching route, oversized document → DLQ.
- DLQ produce failure → block the partition and alert. Loss is worse than lag.

Replay is configuration: a pipeline whose source is the DLQ topic, with the
original (or repaired) routes. Inspection uses standard Kafka tooling; purge is
retention. Dedicated DLQ tooling is post-MVP (ADR-0020).

## Consequences
No extra network paths for DLQs. The DLQ producer runs under the identity named by
`dlq.identity` (default: the pipeline's consumer identity), so reader and writer
service accounts can be separated; the writer identity needs `Write`/`Describe` on
the DLQ topics it serves. the replay pipeline's `kafkafim_consumer_lag` is the unresolved-backlog number.
