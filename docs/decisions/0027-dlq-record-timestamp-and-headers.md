# ADR-0027: DLQ records carry the send time; the original timestamp and a fresh `kafkafim.*` header set travel in headers

Status: accepted · Date: 2026-11-08 · Refines [ADR-0011](0011-dlq-on-source-kafka-cluster.md)

## Context
ADR-0011 keeps the original key and value byte-for-byte and puts error and provenance
into `kafkafim.*` headers, but says nothing about the DLQ record's own timestamp or about
incoming headers that already use the `kafkafim.` namespace. Both matter once records
are dead-lettered late or replayed:

- A record can reach the DLQ long after it was produced (a backlog, a long
  `max-elapsed`). With its original timestamp, time-based retention on the DLQ topic
  can delete it as soon as it lands, and a broker rejects timestamps outside its
  `message.timestamp.*` window. A rejected send is retried forever (ADR-0011: loss is
  worse than lag), so one old record would block its source partition for a value
  kafkafim cannot change.
- A replayed record that fails again would carry two sets of `kafkafim.*` headers: a
  reader taking the first value sees the old failure, and a stale header that the new
  failure does not set (for example `kafkafim.doc-index`) would survive.
- Ingest time is the Kafka record timestamp (DESIGN §4.5). If DLQ records carry the
  send time, a replay pipeline without `document.timestamp-field` resolves `%{+...}`
  index patterns to the replay date instead of the original one.

## Decision
- **Timestamp**: a DLQ record's Kafka timestamp is the send time (the producer sets it).
  The original record timestamp travels in the `kafkafim.source.timestamp` header
  (ISO-8601 UTC, like the other instant headers), omitted when the source record had
  none.
- **Headers**: incoming headers in the reserved `kafkafim.` namespace (compared
  case-insensitively) are **dropped and replaced** by the current set. All other
  original headers are kept, first and in their order, duplicates included; kafkafim's
  headers follow. A record dead-lettered again after replay therefore carries exactly
  one, current set; the earlier failure stays readable at the coordinates the new
  `kafkafim.source.*` headers name.
- **Replay ingest time**: a pipeline consuming a DLQ topic uses
  `kafkafim.source.timestamp` as the ingest time when `document.timestamp-field` is
  unset, so replayed documents land in the original date's index. Records without the
  header keep the Kafka record timestamp.

## Alternatives
- Keep the original timestamp on the DLQ record: simpler for replay, but exposed to
  retention and to the broker's timestamp window, with a blocked partition as the
  failure mode.
- Set `message.timestamp.type=LogAppendTime` on DLQ topics: kafkafim provisions nothing
  (ADR-0021), so it cannot rely on topic settings, and the original timestamp would
  still be lost without a header.
- Keep incoming `kafkafim.*` headers and append the new ones: preserves the failure
  history in one record, but leaves readers to guess which set is current and lets stale
  headers survive.

## Consequences
- DLQ retention counts from the time of dead-lettering, which is what operators expect.
- `kafkafim.source.timestamp` joins the ADR-0011 header list (DESIGN §9).
- Failure history across replays is a chain through `kafkafim.source.*`, not a stack of
  headers in one record.
- The DLQ writer already implements the timestamp and header rules
  (`dlq/DlqRecordFormat`). Reading `kafkafim.source.timestamp` as the ingest time of a
  replay pipeline is not implemented yet; it belongs to the supervisor slice, which
  wires sources to pipelines.
