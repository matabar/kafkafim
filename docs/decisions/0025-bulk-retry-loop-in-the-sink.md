# ADR-0025: Bulk retries are a small loop in the sink; Resilience4j supplies the breaker and the backoff

Status: accepted · Date: 2026-11-07 · Refines [ADR-0016](0016-resilience4j-for-breaker-and-retry.md)

## Context
ADR-0016 chose `resilience4j-circuitbreaker` and `resilience4j-retry` for the per-cluster
breaker and the per-item retry. Building the sink showed that the retry cases do not fit
`Retry`'s model of decorating one call and repeating it:

- a request-level failure (transport error, 429, 5xx, 401/403) repeats the same request
  **forever**, and between attempts the sender must wait while the cluster's breaker is
  open;
- 413 does not repeat the call, it splits it into two different calls;
- a retryable **item** error must not repeat the request: the item goes back into the
  batcher after a delay, travels in a later bulk with other documents, and keeps its own
  attempt count and first-failure time across requests (the poison bound, ADR-0011);
- auth failures are retried but count toward neither the breaker nor the poison bound.

## Decision
The bulk sender (`sink/es/BulkSink`) runs its own loop over the `ErrorClassifier` verdicts.
Resilience4j still provides:

- the **circuit breaker** per Elasticsearch cluster (`CircuitBreaker`, driven manually:
  `tryAcquirePermission`, then `onSuccess`, `onError` or `releasePermission` per verdict);
- the **backoff intervals**: exponential with jitter from `resilience4j-core`'s
  `IntervalFunction.ofExponentialRandomBackoff` (100ms doubling to 30s, ±50% by default).

`resilience4j-retry` is not used by the sink.

## Consequences
The retry behaviour lives in one class next to the classification table, and every branch
is unit-tested with a scripted transport. `resilience4j-retry` stays a declared dependency
until someone needs it or removes it (a `deps` change, outside the sink).
