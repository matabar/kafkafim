# ADR-0037: Routes can drop documents

Status: accepted · Date: 2026-11-11 · Refines [ADR-0009](0009-declarative-routing-no-expression-language.md) and [ADR-0006](0006-at-least-once-with-id-idempotence.md)

## Context
Until now every decoded document was either indexed or dead-lettered (ADR-0006,
DESIGN §1: "DLQ topic for every event that cannot be indexed"). Teams migrating from
Logstash also filter: debug-level logs, synthetic test traffic, event types that one
index should not hold. Without a filter in kafkafim the choices are bad. One is to
index everything and delete it afterwards. Another is to route it to a throwaway
index, which still costs the bulk traffic and the storage. A third is to make the
producer split its topic. A route with no target is the smallest addition to the rule
set ADR-0009 defined.

Discarding by design is an exception to "never lose data silently", so the rule has
to make every drop deliberate, visible and countable.

Part 1 (PR #63) already changed the data path for this. Routing happens before the
id, version and timestamp checks, so those checks apply only to copies that go to
Elasticsearch (option B). Under the other option (A), checks first, a document that a
drop route matched would still reach the DLQ as `missing_id`, `decode` or
`bad_timestamp`. Operators would then have to fix fields on documents they had asked
to discard.

## Decision
- **`drop: true` on a route** takes the place of `cluster`, `identity`, `index` and
  `pipeline`. Validation rejects mixing `drop` with any of them. `name`, `when` and
  `default` work as on any route. `default: true` with `drop: true` is allowed and
  turns the routes into an allowlist. A pipeline where every route drops is valid,
  and startup logs a warning: it consumes and commits without indexing anything.
- **Selection.** In `route-mode: first`, order decides, as for any route. In
  `route-mode: all`, a selected drop route **cancels every other copy**: the document
  goes nowhere, and its label is the first matching drop route. A drop is a statement
  about the document, not about one target. An "index it elsewhere but not here"
  reading would need ordering or negation rules that ADR-0009 deliberately does not
  have. `Router.select` keeps its return type: a drop selection is that one route.
- **Accounting.** A dropped document is one part of its record that completes as soon
  as it is dispatched. It has no sink, no DLQ record, no source byte copy and no index
  resolution, and the id, version and timestamp checks do not apply (option B). A
  record whose documents all drop commits at once, like an empty array. Array
  elements drop one by one; the rest go ahead. Decode failures still go to the DLQ:
  invalid JSON, a scalar, a non-object element, an oversize record, a tombstone. They
  are not documents a route could have looked at.
- **Visibility.** `kafkafim_docs_total{route,outcome="dropped"}` counts every dropped
  document under its drop route's label. There is no new metric, no default alert and
  no log line per document. The shipped dashboard shows dropped documents per second
  by route. Batch spans do not change. A dropped document never joins a bulk batch,
  which belongs to one Elasticsearch cluster and identity, so a per-batch dropped
  count would always be 0 or would land on an unrelated batch.
- **Reload.** A drop route is a route: adding, changing or removing one restarts only
  its pipeline, and the change applies to records not yet committed. Nothing already
  in Elasticsearch is removed.
- **No `allow-drop` switch.** We do not add a per-pipeline or global flag that must
  be set before `drop: true` is accepted. The route itself is the explicit opt-in. It
  is reviewed in the infrastructure repository like any routing change, and its
  effect is counted per route. A second switch would guard the same decision twice,
  and kafkafim stays light.

## Alternatives
- **Checks first (option A)**: simpler, but it dead-letters documents the operator
  asked to discard. Rejected in #63.
- **In `all` mode, drop only removes itself**: then a drop route would do nothing
  (it has no copy to remove). Real filtering would need "every route but X", which is
  negation.
- **Drop to the DLQ, sampling, NOT/OR conditions, dropping decode failures**: not
  needed now, and each widens the rule language or the failure model. Each one would
  need its own ADR.

## Consequences
Documents can now leave kafkafim without reaching Elasticsearch or the DLQ, but only
through a configured drop route, and each one is counted. `outcome=dropped` is what
tells a dropped document from a lost one (RUNBOOK). Preflight and the auth checks skip
drop routes, which name no cluster. A pipeline whose routes all drop opens no bulk
sink.
