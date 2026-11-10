# ADR-0035: A DLQ writer is evicted when its pipeline leaves its `DlqProducers` instance

Status: accepted · Date: 2026-11-10 · Refines [ADR-0028](0028-hot-reload-rereads-config-data-and-shares-building-blocks-per-cluster.md)

## Context
Under ADR-0028 a reload can move a pipeline to new DLQ producers (its DLQ settings
changed, or another pipeline new to its Kafka cluster made a new instance current),
while other pipelines keep the old instance. The old `DlqProducers` cached the moved
pipeline's `DlqWriter` by name, and nothing removed it. Its failed sends kept retrying,
and kept reporting to telemetry under the pipeline's name, until the old instance closed
with the last pipeline still using it. That could take until the next pod restart. The
records themselves are not at stake: their offsets were never committed, so the
pipeline's next run gets them again. But the writer cost memory, produced duplicate DLQ
records and showed misleading DLQ failure counts for the pipeline.

## Decision
- **`DlqProducers.evict(pipeline)`** drops one writer. Its unacknowledged sends, including
  those waiting for a retry, complete with `DlqClosedException`, exactly as at `close()`.
  Later sends through the dropped writer fail the same way, and it stops retrying. A
  producer callback already under way at the eviction may still report one last event
  (`onWritten`, `onSendFailed`, `onBlocked` or `onUnblocked`); the supervisor ignores it,
  since it filters by owner. The shared producer of the `(cluster, DLQ identity)` stays
  until the instance closes, because other writers may use it. A later `writer(pipeline)`
  call builds a new writer.
- **`PipelineResources` evicts on the last use**: it counts each pipeline's uses per
  `DlqProducers` instance. When the count drops to zero and the instance stays open
  (another pipeline still uses it), it evicts the pipeline's writer there. This applies
  to a pipeline that moved to a new instance and to a removed pipeline. A pipeline
  restarted onto the same instance takes its new use before it gives back the old one,
  so it keeps its writer, and with it its `dlq-blocked` state, as before.
- Evicting a blocked writer sends no `onUnblocked` event of its own. The supervisor
  already ignores the old instance's events for that pipeline (ADR-0028), and telemetry
  does not track the blocked state.

## Alternatives
- **Tagging telemetry events with their owner instance**, and filtering out the old
  instance's events: hides the counts but leaves the writer retrying, so memory use and
  duplicate DLQ records stay. The supervisor already filters by owner.
- **Closing the shared producer once it has no writers left**: saves one connection for
  the rest of the old instance's life, but adds a second lifecycle to the producers.
  The instance closes with its last pipeline anyway.

## Consequences
- After a reload, a pipeline's DLQ writer in an instance it left stops retrying and
  reporting, apart from at most one callback already under way. Each eviction logs one
  INFO line with the number of unacknowledged records dropped. Their offsets stay
  uncommitted, as at a close: the pipeline's next run gets them again, and a removed
  pipeline's records wait in its consumer group if it ever comes back.
- The window in which two producers for the same `(cluster, identity)` coexist is
  unchanged (ADR-0028).
