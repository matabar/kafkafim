# ADR-0005: Elasticsearch low-level REST client; we build the bulk body ourselves

Status: accepted · Date: 2026-11-07

## Context
20 Elasticsearch clusters at different 8.x minors. The typed Java API client is
coupled to a server major; Spring Data Elasticsearch adds repository abstractions
we do not need and a single-cluster assumption.

## Decision
Use `org.elasticsearch.client:elasticsearch-rest-client` only (Apache HttpClient,
node round-robin, TLS). kafkafim builds the NDJSON `_bulk` body and parses per-item
results with Jackson.

## Rationale
- Version-agnostic: pure HTTP, works against any 8.x (and 9.x) cluster.
- Per-item outcomes must flow back to the per-partition offset tracker and the DLQ;
  a hand-written parser makes that path explicit and testable against captured
  responses.
- One `RestClient` per `(ES cluster, identity)`, because the low-level client fixes
  the `Authorization` header or client certificate at construction (ADR-0012).
  Bulk batching follows the same key. The circuit breaker stays per cluster.
- `401`/`403` is treated as a configuration failure: pause the pipeline, retry with
  backoff, alert. Never DLQ documents because of a bad credential.

## Consequences
We maintain a small bulk body builder and an error-classification table
(`retryable` vs `dlq` vs `ignore`). Both are unit-tested with recorded ES responses.
