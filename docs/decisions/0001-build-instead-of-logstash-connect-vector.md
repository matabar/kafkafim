# ADR-0001: Build kafkafim instead of keeping Logstash or adopting Kafka Connect / Vector

Status: accepted · Date: 2026-11-07

## Context
~200 Logstash pipelines ship JSON from 4 Kafka clusters (~100 topics) into ~20
Elasticsearch 8.x clusters. Pain: scaling, config sprawl, DLQ handling, disk queues,
consumer threads not bound to partitions, weak observability, no IaC story.

Production Kubernetes clusters allow plain workloads only: no CRDs, no operators,
no persistent storage.

## Options
- **Logstash**: the status quo; the listed pains are structural.
- **Kafka Connect + Elasticsearch sink**: one Connect cluster per Kafka cluster,
  REST-driven config, routing limited to SMTs, JMX metrics, scaling by `tasks.max`.
- **Vector**: good YAML + hot reload, but bulk item rejections are logged and dropped
  (no per-event DLQ), no fan-out control, no partition-bound workers.
- **Build**: own the consumer, the bulk sender and the routing model.

## Decision
Build kafkafim. Every requirement maps to a decision we control; the two hard parts
(Kafka consumer, ES bulk client) have mature JVM libraries.

## Consequences
We own correctness of offset commits, error classification and backpressure. These
are covered by integration and chaos tests (ADR-0019).
