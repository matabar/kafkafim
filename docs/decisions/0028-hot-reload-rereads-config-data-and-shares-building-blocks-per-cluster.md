# ADR-0028: Hot reload re-runs Spring Boot's config data loading and shares building blocks per cluster definition

Status: accepted · Date: 2026-11-08 · Refines [ADR-0012](0012-spring-configuration-helm-configtree-hot-reload.md)

## Context
ADR-0012 decided hot reload with Spring's `Binder`, a directory watcher and
`POST /actuator/reload`, but not where the new tree comes from, nor how the
building blocks of the running pipelines (Kafka consumer factories, DLQ producers,
Elasticsearch clients and breakers) follow a new configuration. Those blocks are
built over one fixed configuration and cache by name: `DlqProducers` keeps a writer
per pipeline (with its topic), `ElasticsearchSinks` a client per
`(cluster, identity)`. Re-binding the already-loaded property sources would miss
every file change, since Spring Boot reads config files and `configtree` directories
once.

## Decision
- **Source of a reload**: a new environment gets every non-config-data property
  source of the running one in order (command line, system properties, environment
  variables, defaults), then Spring Boot's `ConfigDataEnvironmentPostProcessor` runs
  on it with the running profiles. Files and config trees are re-read from disk with
  startup's precedence; no own YAML loading or location resolution.
- **Validation** is startup's: Kubernetes guard, unknown keys, Bean Validation, the
  cross-reference rules and the inline-secret check, in one pass, against the fresh
  property sources (`config/ConfigLoader`). Invalid trees are rejected whole.
- **Diff by pipeline** (`config/ConfigDiff`): own definition, used clusters minus
  identities, referenced identities (secret values included).
- **Building blocks per cluster definition** (`supervisor/PipelineResources`): one
  current Elasticsearch registry per Elasticsearch cluster definition and one current
  DLQ producers + consumer factories per Kafka cluster definition, reference-counted
  by pipelines. A (re)started pipeline takes the current instance; a changed
  definition, or a pipeline the current DLQ producers do not know with its present
  DLQ settings, makes a new instance current; an instance closes with its last
  pipeline. Events (breaker transitions, DLQ blocked) are tagged with the instance
  that raised them and only apply to pipelines using it.
- **Endpoint in `supervisor`**: nothing depends on `supervisor` (ADR-0003), so the
  `@Endpoint` there cannot form a `telemetry` ↔ `supervisor` cycle.
- **Watcher settings** under `kafkafim.reload` (`enabled`, `poll-interval`,
  `debounce`), read at startup.

## Alternatives
- **Re-read only `application-kafkafim.yaml` and a new `ConfigTreePropertySource`
  ourselves**: duplicates Boot's location, profile and import rules, and drifts from
  them.
- **One full set of building blocks per configuration generation**: simplest, but a
  pipeline restarted for its own change would get new clients and producers and lose
  a blocked DLQ writer's state, and every generation would duplicate connections.
- **Evicting entries from `DlqProducers` / `ElasticsearchSinks`**: would share
  producers and clients exactly per `(cluster, identity)` across generations, but
  changes the `dlq` and `sink.es` building blocks; left as a follow-up.

## Consequences
- During and after a reload, an old and a new instance can coexist until the old
  one's pipelines restart: two producers or clients for the same
  `(cluster, identity)`, and a cluster's breaker split per instance.
- Adding a pipeline creates new DLQ producers for its Kafka cluster (the current
  ones cannot learn a new pipeline); untouched pipelines keep the old ones.
- A pipeline that moves to new DLQ producers leaves its old writer behind; that
  writer keeps retrying failed sends (and reporting them to telemetry under the
  pipeline's name) until its instance closes with the last pipeline still using it.
  The records are re-delivered to the new run regardless. Per-writer eviction in
  `dlq/` would end this.
- A reload waits for the drains it triggers; a SIGTERM during a reload waits for it.
