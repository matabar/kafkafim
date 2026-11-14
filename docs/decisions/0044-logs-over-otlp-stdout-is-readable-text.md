# ADR-0044: Logs are exported over OTLP with metrics and traces; stdout is readable text

Status: accepted · Date: 2026-11-13 · Supersedes the logs part of [ADR-0013](0013-observability-otel-via-spring-boot.md)

## Context
ADR-0013 put logs on stdout as ECS-structured JSON and called OTLP log export optional;
it was never built. That assumed a log shipper on the nodes would collect stdout.

In production and prep, kafkafim's logs, traces and metrics always go together over
OTLP to the APM Server, which is external to kafkafim (Matan, 2026-11-13). Nothing
collects or parses stdout there: it is only what a person sees with `kubectl logs`, or
in a terminal when running locally. JSON is the wrong format for that reader, and a
second machine format beside OTLP is dead weight ("we will never use ECS JSON for
stdout").

Spring Boot 4.1 auto-configures the log half of the OpenTelemetry SDK (logger provider,
batch processor, OTLP exporter, the same `Resource` as traces, `OTEL_*` variable
mapping). It does not connect Logback to it.

## Decision
- **Every log event is exported over OTLP** to `${OTEL_EXPORTER_OTLP_ENDPOINT}/v1/logs`,
  next to `/v1/metrics` and `/v1/traces`. It is on wherever those are, by default, and has
  the same kind of switch (`management.logging.export.otlp.enabled=false`).
- **Spring Boot's auto-configuration does the export.** kafkafim adds the one missing
  piece: OpenTelemetry's Logback appender (`opentelemetry-logback-appender-1.0`) in
  `logback-spring.xml`, and one bean (`telemetry/LogExportConfiguration`) that hands it the
  SDK. No hand-built exporter, processor or resource.
- **The OTLP record is the structured copy.** It carries the severity, the logger name
  (as the instrumentation scope), the thread, MDC entries and key-value pairs as
  attributes, the exception's type, message and stack trace, the trace and span id of the
  span in scope, and the resource attributes of metrics and traces (`service.name`,
  `service.namespace`, per-pod `service.instance.id`).
- **Startup lines are not lost.** The appender keeps the first 1000 events logged before
  the SDK exists and replays them when it is installed.
- **Export never blocks or fails the pipeline.** Records are batched off the logging
  thread; a full queue (2048 records) drops records, and an unreachable endpoint costs
  only the export. stdout always has every line.
- **stdout is plain text**: Spring Boot's default console line (time, level, pid,
  application, thread, trace and span id, logger, message, stack trace). There is no JSON
  option; `logging.structured.format.console` is gone from `application.yaml`.
- **Headers** follow the other two exporters: `OTEL_EXPORTER_OTLP_HEADERS` reaches all
  three through Spring Boot's mapping, and the chart mounts `otel.secretHeaders` at a
  third configtree path, `management/opentelemetry/logging/export/otlp/headers`
  (reserving `OTEL_EXPORTER_OTLP_LOGS_HEADERS` in `extraEnv`).

## Consequences
- A pod that never gets as far as creating the OpenTelemetry SDK (invalid configuration,
  a missing mount) exports nothing: its startup failure is on stdout only
  (`kubectl logs --previous`, RUNBOOK §5.9), as before.
- Lines logged while the process shuts down, after the logger provider closed, are on
  stdout only.
- With the APM Server unreachable, logs of that period exist only on stdout, and only
  until the pod is gone. The exporter reports the failure on stdout (throttled). That
  line passes through the appender like any other and is queued for export too; the
  SDK's throttling and the drop-on-full queue keep this from feeding itself.
- Anything that used to parse stdout JSON must read the OTLP records instead. Nothing in
  the repository did (no runbook step, alert or test).
- The appender is not managed by the Spring Boot or OpenTelemetry BOMs. `pom.xml` pins it
  to the instrumentation release built against the BOM's OpenTelemetry SDK, and Dependabot
  ignores it so that it moves with the Boot upgrade that moves the SDK.
- MDC capture includes Micrometer Tracing's `traceId` and `spanId` entries, so a record
  repeats them as attributes beside its native trace and span id.
- kafkafim sets no MDC entries or key-value pairs today. Any added later is exported
  automatically, so it must never hold a secret or document content (DESIGN §10).

## Alternatives
- **Keep ECS JSON on stdout and let a node agent ship it.** Rejected: production has no
  such path for kafkafim, and correlation with traces and the per-pod resource would have
  to be rebuilt in the shipper.
- **A stdout format switch** (text locally, JSON in pods). Rejected: no reader for the
  JSON; one format less to test and document.
- **Attach the appender in code instead of `logback-spring.xml`.** Rejected: the appender
  would exist only once the context is running, so it could not keep and replay the
  startup lines.
- **OpenTelemetry Java agent.** Rejected in ADR-0013 for metrics and traces; the same
  reasons hold.
