# ADR-0004: Spring for Apache Kafka with programmatic listener containers

Status: accepted · Date: 2026-11-07

## Context
Pipelines are defined in YAML, created and destroyed at runtime (hot reload), one
consumer group each. `@KafkaListener` annotations are static and do not fit.

## Decision
Use `spring-kafka`'s `ConcurrentMessageListenerContainer`, created programmatically
per pipeline from a `ConsumerFactory` keyed by `(Kafka cluster, identity)`, since
each pipeline names the identity its consumer group authenticates with (ADR-0012).

- Batch listener, `AckMode.MANUAL`, **out-of-order acknowledgements enabled**
  (`asyncAcks`), so a batch can be acked when its bulks finish in any order while
  the container commits only contiguous offsets.
- `CooperativeStickyAssignor` to avoid stop-the-world rebalances on scale and rollout.
- Container concurrency is never set above the pipeline's partition count
  (startup warns; runtime caps).
- Pause/resume on the container implements backpressure when a target ES cluster's
  circuit breaker is open.

## Alternatives
Raw `kafka-clients` loop. Rejected: we would re-implement lifecycle, rebalance
listeners, commit bookkeeping and pause/resume that spring-kafka already provides
and tests.

## Consequences
We depend on spring-kafka's out-of-order commit semantics; integration tests cover
rebalance-mid-batch and shutdown-mid-batch explicitly.
