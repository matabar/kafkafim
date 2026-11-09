# ADR-0031: Elasticsearch client timeouts are set per cluster; the breaker's half-open limit follows the response timeout

Status: accepted · Date: 2026-11-09 · Refines [ADR-0026](0026-bulk-retry-loop-in-the-sink.md)

## Context
The sink's HTTP clients (ADR-0005, ADR-0026) used code defaults that DESIGN §7 called "not
configurable in the MVP": connect timeout 5s, response (socket inactivity) timeout 60s,
30s to lease a pooled connection, 32 connections per node and 128 per client. The
per-cluster breaker's half-open safety net was `waitInOpen` plus a fixed 2 minutes, chosen
to outlast the 60s response timeout.

`bulk.max-bytes` and `bulk.in-flight` are already set per cluster. A `max-bytes` bulk must
be answered within the response timeout, and a timeout is a request-level failure that is
retried forever (DESIGN §7). A cluster that needs larger bulks or answers slowly therefore
times out every request and only produces lag, and the operator cannot fix it without a
release. A remote cluster with a slow TLS handshake has the same problem with the connect
timeout.

## Decision
- Each Elasticsearch cluster gets a `client` block with `connect-timeout` (default 5s) and
  `response-timeout` (default 60s), both validated as positive durations. Every
  `(cluster, identity)` client of that cluster uses them.
- The breaker's half-open safety net becomes `waitInOpen` plus **twice the cluster's
  response timeout**, so a slow but legitimate probe is never cut short. With the default
  60s this is the former 2 minutes.
- The block is part of the cluster definition. A change restarts every pipeline that
  routes to the cluster (`ConfigDiff`, DESIGN §5.6), and hot reload builds new clients and
  a new breaker for it (ADR-0028).
- These stay code defaults: the connection pool (lease timeout, per-node and per-client
  sizes), the breaker's window, thresholds, open time and probes, and the preflight
  authentication check's own short timeouts (§4.6).

## Consequences
Operators can fit timeouts to a cluster's latency and bulk size without a release. The
two exposed values are the ones tied to settings that were already configurable. The pool
and the breaker stay fixed until a production need appears; exposing them later adds keys
under the same `client` block or a new `breaker` block. Nothing yet warns when
`bulk.max-bytes` is unlikely to be answered within `response-timeout`.
