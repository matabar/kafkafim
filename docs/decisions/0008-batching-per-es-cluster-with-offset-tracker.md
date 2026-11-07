# ADR-0008: Batch per ES cluster; track completion per partition

Status: accepted · Date: 2026-11-07

## Decision
Decoded and routed actions are batched **per target (ES cluster, identity)** (flush
on `bulk.max-docs | max-bytes | max-age`), not per partition, since one bulk request
carries one `Authorization` header (ADR-0012). A per-partition tracker records
completed offsets as bulks ack, possibly out of order, and commits the highest
contiguous offset.

## Rationale
A topic with many quiet partitions still produces full-size bulks; sub-second
`max-age` bounds latency. A record that explodes into N documents (JSON array input)
is complete only when all N are acked or DLQ'd.

## Consequences
Memory is bounded by `max-in-flight-bytes` per pipeline. Rebalance safety: on
revoke, in-flight batches for revoked partitions are flushed or abandoned and only
completed offsets are committed; the new owner re-reads the rest (idempotent, ADR-0006).
