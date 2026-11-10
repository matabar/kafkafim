# ADR-0034: Consumer lag is monitored outside kafkafim

Status: accepted · Date: 2026-11-10 · Supersedes [ADR-0033](0033-consumer-lag-read-per-pipeline-in-parallel-staleness-as-timestamp.md), and the lag-metric parts of [ADR-0014](0014-scaling-static-plus-cpu-hpa.md) and [ADR-0011](0011-dlq-on-source-kafka-cluster.md)

## Context
kafkafim read the consumer lag of every running pipeline itself and exported it as
`kafkafim_consumer_lag{topic}`, with `kafkafim_consumer_lag_timestamp_seconds` for its
age (ADR-0033). ADR-0014 exported it for a future lag-based autoscaler, and ADR-0011
named the replay pipeline's lag as the unresolved DLQ backlog. The shipped dashboard
had a lag panel and the alert rules a `KafkafimConsumerLagGrowing` alert.

The team that runs Kafka already monitors consumer lag for every consumer group,
kafkafim's included, with dashboards and alerts. Reading it again inside kafkafim
costs:

- **One extra consumer per running pipeline** (its identity and group, never joining),
  with its own broker connections, created and closed with the source.
- **Two broker requests per pipeline every 10s** (committed offsets, end offsets), on
  every pod, for numbers the brokers already hand to the Kafka team's monitoring.
- **Code and operations surface**: a poller, a virtual thread per pipeline read, a
  deadline, a staleness gauge and the stop/wake-up handling around the extra consumer
  (ADR-0033), all to recompute a solved number.

## Decision
- **kafkafim does not read or export consumer lag.** No lag consumer, no lag poller, no
  `kafkafim_consumer_lag` or `kafkafim_consumer_lag_timestamp_seconds` series. Each
  pipeline runs only the consumers of its listener container (plus the short-lived
  metadata consumer its source opens when it starts, to count partitions).
- **Lag lives in the Kafka team's consumer-lag monitoring**, per consumer group
  (`source.group-id`, default `kafkafim.<pipeline>`). The runbook, the dashboards and
  the alert rules point there; kafkafim ships no lag panel and no lag alert.
- **kafkafim's own "are we keeping up" signals** stay: `kafkafim_e2e_latency_seconds`
  (Elasticsearch ack time minus the record timestamp), the throughput counters
  (`kafkafim_records_consumed_total`, `kafkafim_docs_total`), and the state, breaker,
  in-flight and connectivity gauges that explain why a pipeline falls behind.
- **Lag-based autoscaling**, if it ever becomes available (ADR-0014), scales on the
  Kafka team's lag metrics, not on a kafkafim series.
- **A replay pipeline is removed** when its consumer group's lag on the DLQ topic, in
  the Kafka team's monitoring, reaches zero (ADR-0011's backlog number).

## Consequences
- One consumer per pipeline and its broker traffic fewer; less code in `source`,
  `telemetry` and `supervisor`. kafkafim stays a light pipeline service: concerns that
  are already solved outside it stay outside it.
- Lag and kafkafim's own metrics live in different places. An operator correlates the
  Kafka team's lag for the group with kafkafim's dashboards by pipeline name and group
  id.
- kafkafim alone cannot tell "behind" while nothing is consumed and nothing is
  acknowledged: `kafkafim_e2e_latency_seconds` records only acknowledged documents.
  The state, connectivity and assigned-partition alerts cover the stalled cases
  (`failed_preflight`, `paused`, Kafka disconnected, 0 assigned partitions); a stall
  while `running` and connected shows as a consumption rate of 0 and as the group's lag
  in the Kafka team's monitoring.
- Dashboards and alert rules that used the removed series must be changed; the shipped
  ones in `deploy/grafana/` are.

## Alternatives
- **Keep the lag series (ADR-0033).** Duplicates the Kafka team's monitoring and keeps
  the extra consumer and its broker requests on every pod.
- **Keep it behind a flag, off by default.** Keeps the code, its tests and its
  stop-path subtleties for a feature nobody turns on.
- **Read lag from the listener consumers' own kafka-clients metrics**
  (`records-lag-max`). Cheaper, but per fetching consumer, client-side and only while
  fetching; still a second lag number next to the authoritative one.
