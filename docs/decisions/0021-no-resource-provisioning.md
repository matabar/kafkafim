# ADR-0021: kafkafim never provisions Kafka or Elasticsearch resources

Status: accepted · Date: 2026-11-07

## Context
Topics, consumer groups, Kafka credentials, Elasticsearch indices, index templates,
ingest pipelines, users, roles and the clusters themselves are created and owned by
the team's external provisioning systems. Production uses SCRAM-SHA-256 for Kafka
and basic auth over TLS for Elasticsearch.

## Decision
kafkafim creates nothing. It consumes resources that already exist and verifies them
at pipeline start with the identities the pipeline references: source topics and DLQ
topic exist (`Describe`), the consumer group can be joined, each route's ES identity
authenticates. A failed check leaves that pipeline stopped in state
`failed_preflight` with a log line naming the missing resource and identity, and
does not affect other pipelines in the pod.

Consequences for the model:
- No `auto-create` options anywhere; no index template or ingest pipeline management.
- Identity privileges are the minimum to read, write and describe; never `Create`
  on Kafka or `manage*` on Elasticsearch.
- Document ids come only from the JSON content (`document.id-field`), never from the
  Kafka key or from ES-generated ids by default.
- Kafka values are always a JSON object or an array of objects; decoding is fixed and
  anything else is a DLQ record with `error.type=decode`.
- Timestamps are parsed per `document.timestamp-format` (ISO-8601 default,
  epoch-millis, epoch-seconds, custom pattern); a bad timestamp goes to the DLQ by
  default rather than silently landing in today's index.

## Consequences
Startup is slightly slower (one metadata and one authenticated request per
pipeline) and misconfiguration is visible within seconds instead of as runtime bulk
errors. The migration runbook gains a "provision first" checklist per pipeline.
