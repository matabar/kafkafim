# ADR-0007: Deployment groups: plain Deployments, no CRDs, operators or storage

Status: accepted · Date: 2026-11-07

## Context
Production clusters permit only resources already present: `Deployment`,
`ConfigMap`, `Secret`, `Service`, `PodDisruptionBudget`, `HorizontalPodAutoscaler`.
Requirements: big topics get dedicated pods, small topics share pods but never
depend on one pod, workers never exceed partitions.

## Decision
A **group** is a set of pipelines deployed as one `Deployment`. Every replica runs
every pipeline of its group; Kafka consumer-group assignment spreads partitions
across replicas.

- Big topic → group with one pipeline, `replicas ≤ partitions`.
- Small topics → shared group, `replicas ≥ 2` (chart default), so each topic has
  consumers on at least two pods.
- Workers ≤ partitions by construction: a replica only processes assigned partitions.
- Chart defaults: `PodDisruptionBudget minAvailable: 1`, preferred anti-affinity
  across nodes and zones, `maxSurge: 1`, `maxUnavailable: 0`, graceful shutdown.

No local disk: Kafka is the only buffer (ADR-0010).

## Consequences
Rebalancing groups is a `values.yaml` edit. A pipeline moving between groups changes
nothing about its consumer group id, so offsets carry over.
