# ADR-0029: Batch spans are recorded when the batch ends, failed batches are always sampled, and the supervisor feeds telemetry

Status: accepted · Date: 2026-11-08 · Refines [ADR-0013](0013-observability-otel-via-spring-boot.md)

## Context
ADR-0013 asks for one `es.bulk` span per bulk request under a `pipeline.batch` span,
`traceparent` Kafka headers as span links, 1% head sampling of batches and errors
always. Two things do not fit a plain implementation:

- **Head sampling decides at the start of the root span**, before anyone knows whether
  the batch will fail. Keeping every failed batch would then need 100% sampling plus
  tail sampling in a collector, which production does not run (APM Server only).
- **The building blocks know nothing of each other.** The sink sees opaque completion
  handles, not Kafka records, so it cannot read `traceparent`; the source knows
  nothing of documents. ADR-0003 lets `sink`, `source` and `dlq` depend on
  `telemetry`, but `telemetry` cannot implement their listener interfaces
  (`SinkListener`, `DlqListener`) without a package cycle, nor the supervisor's
  (`SupervisorListener`), since nothing may depend on `supervisor`.

## Decision
- **Spans after the fact.** The sink reports each sealed batch once it is done (every
  action has an outcome or went back to the batcher for an item retry): sealing time,
  every request with its start, end, status, error, retries and item errors, and the
  batch's handles (`SinkListener.onBatchSent`, `BatchReport`). `telemetry/BatchTracer`
  then records the root `pipeline.batch` span and its `es.bulk` children with those
  timestamps (Micrometer Tracing over the OpenTelemetry bridge). A failed batch carries
  `error.type` from its first span attribute on.
- **Sampler.** `telemetry/BatchSampler` replaces Spring Boot's
  `parentBased(traceIdRatioBased(p))` with `parentBased(errors-or-ratio)`: a root
  `pipeline.batch` span with `error.type` is always sampled, every other root span by
  trace id at `management.tracing.sampling.probability` (0.01 in `application.yaml`),
  children follow their parent.
- **Links.** The supervisor turns the handles into the records' distinct
  `traceparent` values (at most 128, the SDK's link limit); invalid values are
  dropped. Links, never parents: a batch mixes documents of many traces.
- **Package direction.** `telemetry` depends on no kafkafim package but `config`
  (a new ArchUnit rule). It owns the meter names, label discipline, series lifecycle,
  spans, sampler and health indicators, with its own small types (`PipelineSpec`,
  `SourceProbe`, `BatchTrace`). The supervisor adapts the building blocks' listener
  events to it (`supervisor/TelemetryListener`, a `SinkListener` and `DlqListener`
  the `ListenerRelay` forwards to) and keeps the series in line with the
  configuration in effect (`supervisor/TelemetryBinder`, on every applied reload).
  `sink`, `source` and `dlq` stay free of telemetry code; they only report through
  their listeners and getters. ADR-0003's picture is unchanged.

## Consequences
- Spans appear when a batch ends. A batch that retries for as long as Elasticsearch
  is down produces its span only afterwards; the metrics (`kafkafim_retries_total`,
  `kafkafim_bulk_requests_total{status}`, `kafkafim_breaker_state`) cover the outage.
- No trace context is sent to Elasticsearch with the bulk request.
- Every batch costs one small report and, unsampled, two non-recording spans.
- Health and the existing `kafkafim_pipeline_state` / reload meters stay in
  `supervisor`, which owns the state they read.

## Alternatives
- **Live spans in the sink** (the sink depends on Micrometer Tracing): the outcome is
  unknown at span start, so "errors always" would need 100% sampling and a tail
  sampler, and the sink would still need the records' headers.
- **Sample everything, drop healthy batches at export** (`SpanExportingPredicate`):
  works, but creates and buffers a recording span for every batch and every HTTP
  request.
- **`telemetry` implements the listener interfaces** (telemetry depends on `sink`,
  `dlq`): reverses ADR-0003's direction and still cannot reach the supervisor's state.
