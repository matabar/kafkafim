# ADR-0009: Declarative routing rules with Logstash-style index patterns; no expression language

Status: accepted · Date: 2026-11-07 (replaces an earlier draft that chose CEL)

## Context
Pipelines need conditional routing and index names derived from fields and the
event timestamp (daily/monthly). The team ruled out an expression language and
transformations for the MVP; configuration must stay plain Spring properties that
Helm can render.

## Decision
- A route has a list of `when` conditions (all must hold): `equals`, `in`,
  `exists`, `prefix`, `matches` (anchored regex), `header`, `topic`. Fields are JSON
  pointers.
- Index names are Logstash-compatible patterns: `%{field}` and `%{+yyyy.MM.dd}`
  (Java `DateTimeFormatter`) over `document.timestamp-field` or ingest time.
- `route-mode: first | all`; exactly one `default: true` route unless `all`.
- Unresolvable pattern or no matching route → DLQ.

## Alternatives
- **CEL / SpEL / jq**: expressive, but a second language in config, harder to
  validate at bind time, and not needed for the ~200 existing pipelines.
- **Transform steps**: out of scope; enrichment lives in ES ingest pipelines
  (`pipeline:` on the route) or in the producer.

## Consequences
Every condition and pattern is parsed and validated at startup and on reload. A
routing case the rule set cannot express gets a new rule type via ADR, not code.
