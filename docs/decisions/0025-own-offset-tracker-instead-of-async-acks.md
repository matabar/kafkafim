# ADR-0025: Own per-partition offset tracker instead of spring-kafka `asyncAcks`

Status: accepted · Date: 2026-11-07 · Supersedes the `asyncAcks` bullet of [ADR-0004](0004-spring-kafka-programmatic-containers.md)

## Context
ADR-0004 planned to commit out of order with spring-kafka's `AckMode.MANUAL` plus
`asyncAcks`. In spring-kafka 4.1 that mode **pauses the consumer after every poll until
every record of that poll is acknowledged** (`KafkaMessageListenerContainer`,
`pausedForAsyncAcks`). A pipeline could then have at most one poll (`max-poll-records`)
in flight per consumer, however large `max-in-flight-bytes` is, and every slow bulk
would stall its partitions' next fetch. ADR-0008 and ADR-0010 need in-flight work bounded
by bytes across many polls, with batches per ES target completing in any order.

## Decision
kafkafim tracks completion itself (`source/kafka/OffsetTracker`) and commits from the
consumer thread; spring-kafka's acknowledgement machinery is not used (`AckMode.MANUAL`,
never acknowledged). The container, batch listener, cooperative-sticky assignor,
lifecycle and pause/resume stay spring-kafka's (ADR-0004 otherwise unchanged).

- **Per partition, per assignment**: every delivered record is pending until it is
  completed (once, or after `n` declared parts for a multi-document record). The commit
  offset is the lowest pending offset, or one past the highest delivered offset when none
  is pending; offset gaps (compaction, transaction markers) need no handling.
- **Commit points**, all on the owning consumer thread: before a poll once
  `commit-interval` elapsed (a `BatchInterceptor`'s per-poll hook, which also runs while
  paused or idle; the poll timeout is at most `commit-interval`), `commitAsync`; on revoke,
  `commitSync`; on stop, through the revoke the container's unsubscribe triggers.
- **Revoke and loss**: the handler is told first, then completed offsets are committed
  (revoke only) and the partition's state is closed. Pending records of a closed
  assignment are abandoned: their bytes are released once, their late completions are
  ignored, and the next owner re-reads them (ADR-0006).
- **Budget**: each pending record's key and value bytes count against
  `max-in-flight-bytes`; reaching it pauses the whole container, resuming at half the
  budget. The supervisor adds its own named pause reasons (breaker, auth); the
  container is paused while any reason is active.

## Alternatives
- `asyncAcks`: rejected for the reason above.
- Ack whole batches without `asyncAcks`: spring-kafka keeps the maximum acknowledged
  offset per partition, so an early-finishing later batch would commit past an unfinished
  earlier one — data loss on restart.
- A raw `kafka-clients` loop: rejected in ADR-0004 for the same reasons as before.

## Consequences
Commit correctness is kafkafim code, unit-tested (`OffsetTrackerTest`) and covered against
a real broker (`KafkaPipelineSourceIntegrationTest`: contiguous-prefix commits, revoke,
restart, drain). The commit hook relies on `BatchInterceptor.setupThreadState` running
before each poll; a spring-kafka upgrade that changes that is caught by those tests.
