# ADR-0014: Scaling: static replicas per group, optional CPU HPA, lag metric exported for future KEDA

Status: accepted; the exported lag metric is superseded by [ADR-0034](0034-consumer-lag-is-monitored-outside-kafkafim.md) · Date: 2026-11-07

## Decision
v1 scales by `replicas` per group in `values.yaml`, with an optional core-API
`HorizontalPodAutoscaler` on CPU (`maxReplicas` capped by partition count). Lag-based
autoscaling needs KEDA (a CRD) and is not available in production; kafkafim exports
`kafkafim_consumer_lag` so switching is a chart toggle if that changes.

## Consequences
Scaling beyond partitions is wasted; repartitioning is a Kafka-side action.
