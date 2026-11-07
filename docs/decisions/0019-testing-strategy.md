# ADR-0019: Testing: unit tests for classification and routing, Testcontainers for end-to-end, explicit chaos cases

Status: accepted · Date: 2026-11-07

## Decision
- Unit: routing rules and index patterns, timestamp parsing per format, decoder (object / array / rejects), bulk body builder, ES error classification (recorded responses),
  offset tracker (out-of-order completion, revoke), config diff.
- Integration (Testcontainers Kafka + Elasticsearch): happy path, JSON array
  explosion, mapping conflict → DLQ, 429 storm → retry then success, ES down
  mid-batch → no loss, rebalance mid-batch → no loss, SIGTERM mid-batch → no loss,
  hot reload of a running pipeline, missing DLQ topic → `failed_preflight`, wrong
  ES credential → pipeline paused with auth-failure metric, SCRAM-SHA-256 and basic
  auth identities against secured containers.
- Load: a Testcontainers-based generator against the compose stack; results recorded in `docs/perf.md`.

## Consequences
CI runs unit + integration on every PR (Docker available on GitHub runners).
