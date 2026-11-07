# ADR-0020: MVP scope — no transformations, no expressions, no CLI

Status: accepted · Date: 2026-11-07

## Decision
The MVP ships a shipper, not a pipeline engine:

- **In**: JSON (and JSON array) records, `_id`/version/timestamp extraction by JSON
  pointer, declarative routing with Logstash-style index patterns, fan-out, per-ES
  cluster bulk batching, at-least-once with idempotent ids, DLQ, hot reload, Spring +
  Helm configuration, OpenTelemetry, Helm chart, local stack.
- **Out**: transformations of any kind, expression languages, CLI subcommands, DLQ
  tooling, `update`/`delete`, UI, non-JSON, schema registry.

## Rationale
Almost every existing pipeline is pass-through; enrichment already happens in the
producers or can move to ES ingest pipelines. Every excluded item is additive later
and none changes the data path.

## Consequences
Pipelines that today use Logstash filters are migrated by first moving the filter to
an ES ingest pipeline and naming it on the route.
