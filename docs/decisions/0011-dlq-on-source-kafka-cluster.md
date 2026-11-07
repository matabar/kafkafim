# ADR-0011: DLQ topic on the source Kafka cluster; original bytes preserved; poison messages bounded

Status: accepted · Date: 2026-11-07

## Decision
Every document that cannot be indexed goes to a DLQ topic on the **same Kafka
cluster as the source** (default `<first topic>.kafkafim-dlq`). The record value is
the original bytes, untouched; the error and provenance travel in headers
(`kafkafim.pipeline`, `error.type`, `error.reason`, `source.topic/partition/offset`,
`target.cluster/index`, `attempts`, `first_failure_ts`, `doc_index`).

Classification:
- Non-retryable item errors (400 family: mapping, parsing, illegal argument) → DLQ now.
- Retryable errors (429, 5xx, connection) → retry; after `max_attempts` or
  `max_elapsed` → DLQ. This is the poison-message bound: a partition always progresses.
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
