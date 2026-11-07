# ADR-0017: Migration from Logstash: run in parallel on the same `_id`s, then stop Logstash

Status: accepted · Date: 2026-11-07

## Decision
1. Translate one Logstash pipeline into a kafkafim pipeline; the `validate` profile in CI.
2. Deploy kafkafim with a **new** consumer group at `offset_reset: latest` while the
   Logstash pipeline keeps running. Same `_id`s → identical documents. Compare
   counts and DLQ rate for a day.
3. Stop the Logstash pipeline. No gap, no duplicates.
4. Pipelines with ES-generated ids: stop Logstash first, then start kafkafim from
   Logstash's committed offsets (set `source.group-id` to Logstash's group).

## Consequences
Requires `_id`-based idempotence (ADR-0006) for the zero-downtime path.
