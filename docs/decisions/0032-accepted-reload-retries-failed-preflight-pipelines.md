# ADR-0032: An accepted reload retries the pipelines it leaves in `failed_preflight`

Status: accepted · Date: 2026-11-10 · Refines [ADR-0028](0028-hot-reload-rereads-config-data-and-shares-building-blocks-per-cluster.md)

## Context
A pipeline whose preflight finds a configuration problem (missing topic, missing ACL,
rejected credential) stays in `failed_preflight` (DESIGN §4.6, ADR-0021). The cause is
almost always fixed outside kafkafim: the provisioning system creates the topic or the
ACL, or the identity's password is changed in Elasticsearch to match the Secret. Under
ADR-0028 such a pipeline started again only when a reload **changed** it, or on a pod
restart, because a reload touches only the pipelines its diff names. Recovering one
pipeline therefore meant a rollout of the whole group, a draining restart of every
healthy pipeline in the pod included.

## Decision
- **Every accepted reload retries**: a reload whose tree is valid (`applied` or
  `unchanged`), from the watcher or from `POST /actuator/reload`, starts once more every
  pipeline that it leaves unchanged and that is in `failed_preflight` at that moment.
  Restarted and added pipelines start anyway; a **rejected** reload retries nothing,
  because it changes nothing. No flag or parameter: a retry costs one preflight
  (one metadata fetch and one `_security/_authenticate` per identity), and an operator
  who fixed a resource wants exactly this.
- **One attempt per reload, never on its own**: the retried pipeline runs one ordinary
  start. A configuration failure sends it back to `failed_preflight`, where it waits for
  the next reload; a transient one keeps it `starting` with the usual backoff. A pipeline
  that is starting or stopping is not retried, so concurrent reloads (serialized anyway)
  cannot stack attempts. Nothing re-triggers a reload: the watcher fires only when the
  files' content changes. The retry rate is therefore the operator's reload rate.
- **Reporting**: the reload response gains `retried`, the names of the pipelines started
  once more; they are no longer listed under `unchanged`, so every pipeline appears in
  exactly one list. `result` and `generation` keep their meaning (a no-op tree that
  retried pipelines is still `unchanged`, generation unchanged), and
  `kafkafim_config_reload_total` keeps its two `result` values. The outcome of a retry is
  not in the response (the start runs in the background): it shows in
  `kafkafim_pipeline_state` (`failed_preflight` → `starting` → `running`, or back) and in
  the pipeline's usual log lines. The reload's INFO line names the retried pipelines.

## Alternatives
- **Only with an explicit flag** (`POST /actuator/reload?retry=true`): one more knob to
  document and remember; a `helm upgrade` after fixing the resource would still leave
  the pipeline stopped. Rejected: the retry is cheap and bounded without it.
- **Automatic retry with a long backoff** (e.g. every 5 minutes): recovers without any
  action, but turns a configuration error into a periodic stream of preflight requests
  and ERROR lines against a resource that may never appear, and blurs the line between
  configuration and transient failures that §4.6 draws.
- **A per-pipeline restart endpoint**: more precise, but another write endpoint to
  secure; a no-op reload already does the job without stopping anything that runs.
- **A new `retried` value of `result`, or a retry counter**: would change the
  meaning of existing alerts and dashboards for little gain over the state gauge.

## Consequences
- After fixing a resource, `POST /actuator/reload` (or any accepted reload) recovers a
  `failed_preflight` pipeline without a pod restart and without touching the others.
- A reload of an unrelated change also retries every `failed_preflight` pipeline of the
  pod; each one that still fails logs its ERROR line again.
- A script calling the endpoint in a loop retries at its own rate; at most one attempt
  per pipeline runs at a time.
