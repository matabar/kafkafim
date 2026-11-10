# ADR-0033: Consumer lag is read per pipeline in parallel under a 5s deadline; stale lag shows as a timestamp

Status: accepted · Date: 2026-11-10 · Refines [ADR-0029](0029-batch-spans-recorded-at-batch-end-failed-batches-always-sampled.md)

## Context
ADR-0029 has `telemetry` own the series and the supervisor feed it; `kafkafim_consumer_lag`
was refreshed every 10s by one poller thread that read every pipeline in turn, each read
bounded by 5s per broker request (two requests). Two problems:

- **Serial reads go stale together.** With N pipelines on a cluster that does not answer,
  one round takes up to 10s × N, so every pipeline's lag, also on healthy clusters, is
  refreshed only every few minutes.
- **Staleness was invisible.** A failed read kept the previous values, and nothing told a
  dashboard or an alert how old they were. Reporting 0 instead would be worse: "no lag"
  for a cluster nobody can reach.

## Decision
- **One read per pipeline, in parallel.** Every 10s the poller starts one read per
  pipeline on its own virtual thread and does not wait. A pipeline whose previous read
  still runs is skipped for that round, so a hanging cluster never piles up reads.
- **A deadline per read.** The 5s bound covers both broker requests of a read together
  (committed offsets, then end offsets with what is left), so a read normally ends
  before the next round.
- **Staleness as a timestamp.** A new gauge per pipeline,
  `kafkafim_consumer_lag_timestamp_seconds{pipeline,kafka_cluster}`, holds the Unix time
  of the last successful read (0 before the first). A failed or skipped read keeps both
  the lag values and the timestamp. `kafkafim_consumer_lag` itself keeps its meaning and
  its series.

## Consequences
- A cluster that does not answer delays only its own pipelines' lag.
- Stale lag is visible: `time() - kafkafim_consumer_lag_timestamp_seconds` is its age;
  an alert or panel can use that. The shipped alert rules and dashboards do not yet.
- One more gauge per pipeline. At most one blocked virtual thread per pipeline.
- A pipeline without a running source or without owned partitions still reads 0 on this
  pod, and that read counts as successful (the pod truthfully owns no lag).

## Alternatives
- **Remove the lag series while stale (gauge absent).** Stale shows as a gap, but a
  `lag > X` alert then resolves during an outage, dashboards lose the last value, and
  series known from the configuration no longer always exist (DESIGN §10).
- **A 0/1 stale flag.** Needs a threshold baked into kafkafim; a timestamp lets each
  alert choose its own and also shows how stale.
- **NaN while stale.** Prometheus stores NaN as a value; sums and comparisons over it
  give NaN or false, which hides more than it shows.
- **Only a deadline, still serial.** Bounds one read but a round still grows with the
  number of unreachable pipelines.
