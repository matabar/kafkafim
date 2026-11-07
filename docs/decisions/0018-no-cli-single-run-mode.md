# ADR-0018: No CLI in the MVP; one run mode plus a `validate` profile

Status: accepted · Date: 2026-11-07 (replaces an earlier draft that chose picocli)

## Decision
The application has a single mode: start, bind and validate configuration, run
pipelines, watch for changes. There are no subcommands.

- Validation of rendered configuration: the `validate` profile binds, validates and
  exits 0/1, for the infrastructure repository's CI (ADR-0012). This repository's CI
  smoke-tests the image with a liveness probe.
- DLQ inspection: standard Kafka tooling (headers carry the error and provenance).
- DLQ replay: a pipeline whose source is the DLQ topic (ADR-0011).
- Load generation for demos: Testcontainers and `kafka-console-producer`.

## Consequences
No picocli dependency. If operational tooling is needed later, it is a separate
module or image, not a mode switch inside the worker.
