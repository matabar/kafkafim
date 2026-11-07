# ADR-0006: At-least-once delivery with ES-side idempotence via `_id`

Status: accepted · Date: 2026-11-07

## Decision
Offsets are committed only after every document from that record is acked by ES or
written to the DLQ. Duplicates on redelivery are neutralised by a deterministic
`_id` taken from the record (`document.id-field`, a JSON pointer, default `/id`).

- `op: index` (upsert semantics) by default; `op: create` for data streams, where a
  409 on retry is treated as success.
- Optional external versioning (`version`, `version_type: external`);
  `version_conflict_engine_exception` is counted as `stale` and treated as success
  unless the pipeline says `on_version_conflict: dlq`.
- `id: auto` (ES-generated) is allowed but startup warns: retries duplicate.

## Consequences
Exactly-once is not needed. Migration from Logstash can run both systems in parallel
on the same `_id`s (ADR-0017).
