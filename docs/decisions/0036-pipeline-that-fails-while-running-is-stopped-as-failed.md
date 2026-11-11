# ADR-0036: A pipeline that fails while running is stopped in a new `failed` state

Status: accepted · Date: 2026-11-11 · Refines [ADR-0032](0032-accepted-reload-retries-failed-preflight-pipelines.md)

## Context
A pipeline can stop making progress after a successful start, in two ways the supervisor
did not notice:

- the record handler (`onBatch`: decode, route, submit) throws. The source catches it and
  pauses itself with the source reason `handler-failure`, which is not a supervisor
  reason, so the state stays `running`. No realistic throw is known; it would be a bug.
- a consumer of the Spring Kafka listener container stops on its own: an error the
  container does not retry (anything from `poll` other than the authentication and
  authorization errors it retries every 10s), or an `Error` from the listener.
  `CommonContainerStoppingErrorHandler` stops it rather than skip records.

In both cases the pipeline consumes nothing, but `kafkafim_pipeline_state` says `running`,
readiness counts it as started, and no alert fires. Recovery needed a pod restart, and
the runbook listed this as a known gap (#19).

## Decision
- **A new state, `failed`**: "was running, then failed". It is not `failed_preflight`,
  because preflight passed, and the cause is not a missing resource: an alert or a
  runbook entry for `failed_preflight` would send operators to the wrong checklist.
  `failed` is a stopped state: not started, so readiness treats it like
  `failed_preflight` (a pod whose every pipeline failed is out of service), and
  `kafkafim_pipeline_state{state="failed"}` is 1.
- **The source reports, the supervisor stops**: the source calls
  `SourceRecordHandler.onSourceFailed(cause)` when the handler throws, and when the
  container publishes a `ConsumerStoppedEvent` with any reason but `NORMAL`. The
  container gets an event publisher for this, and its child containers inherit it. The
  pipeline goes to `failed` at once, then is stopped like any other stop, on its own thread
  (the report comes on a consumer thread, which a container stop waits for): the source
  drains (up to the drain timeout), commits what completed, leaves the group, and the
  sinks close. Records still incomplete are re-delivered to the next owner. A stop
  that comes meanwhile (shutdown, reload) waits for this one, and nothing can start
  the pipeline before it is done. A report from the source of an earlier run is ignored,
  and so is one that comes while the pipeline is being stopped anyway.
- **No automatic retry: the ADR-0032 rule applies.** Every accepted reload starts once
  more each unchanged pipeline in `failed_preflight` **or `failed`**, one attempt per
  reload, and lists it under `retried`. A restarted pipeline starts anyway. A pipeline
  that fails again goes back to a failed state and waits for the next reload. The
  supervisor's `retryFailedPreflight` becomes `retryFailedPipelines`; the reload's INFO
  line says `retrying failed pipeline(s) [...]`. The reload response and
  `kafkafim_config_reload_total` do not change.
- **Reporting**: one ERROR line, `Pipeline '<p>': failed while running; stopping it until
  the next accepted reload or a restart: <cause>`, after the source's own ERROR line (the
  handler's exception or the container's). The INFO transition `running -> failed`
  follows as for every state.

## Alternatives
- **Reuse `failed_preflight`**: existing alerts and dashboards would cover it at once, but
  the label would be wrong, and so would the runbook it points to.
- **Restart the source automatically, with backoff**: recovers from a one-off error with no
  action. But a deterministic cause (a bug that throws on a record, an unsupported broker
  response) would restart the source in a loop, re-reading the same records and logging
  the same ERROR. This is the line ADR-0032 drew for configuration failures, and kafkafim
  stays light: the operator's reload (or the pod restart) is the retry.
- **Restart the pod (fail liveness)**: a restart may help with a one-off error, but it
  restarts every healthy pipeline of the pod, and DESIGN §10 keeps pipeline problems out
  of liveness.
- **Report `failed` but keep the source running**: nothing would release a handler
  failure's partitions to the group's other pods, and the paused consumer would hold them
  forever.

## Consequences
- A pipeline that stops consuming after start shows as `failed`, takes the pod out of
  readiness when it was the pod's last started pipeline, and frees its partitions for the
  other pods of the group.
- Operators recover it with a no-op `POST /actuator/reload` after reading the ERROR lines,
  the same step as for `failed_preflight`.
- `kafkafim_pipeline_state` has a sixth `state` value. Alert rules and dashboards that
  list states need it: the shipped rules in `deploy/grafana/` have no alert on `failed`
  yet (follow-up).
- `SourceRecordHandler` gains a default method, so existing handlers compile unchanged.
