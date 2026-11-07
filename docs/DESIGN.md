# kafkafim — Design

Status: draft v0.4 (2026-11-07), MVP scope. Decisions are recorded one per file in
[decisions/](decisions/README.md); the stack is in [TECH_STACK.md](TECH_STACK.md);
CI/CD, registries and the air-gap procedure in [DELIVERY.md](DELIVERY.md); the Helm
chart in [HELM.md](HELM.md).

Replaces ~200 Logstash pipelines that ship JSON from 4 Kafka clusters (~100 topics)
into ~20 Elasticsearch 8.x clusters.

## 1. Goals and non-goals

### Goals (MVP)

- Ship JSON records from Kafka to Elasticsearch with at-least-once delivery and
  ES-side idempotence (document `_id` taken from a record field).
- Declarative per-event routing: choose ES cluster and index from field conditions,
  with Logstash-compatible index patterns (`orders-%{region}-%{+yyyy.MM}`) resolved
  from the event timestamp or ingest time. A pipeline may fan out to several targets
  and may read several topics.
- Sub-second end-to-end latency, configurable per pipeline.
- Horizontal scale on Kubernetes with plain `Deployment`s. No CRDs, no operators,
  no persistent storage. Kafka is the only buffer.
- Never run more consumer workers than partitions. Big topics get dedicated pods;
  small topics share pods but are never served by a single pod.
- DLQ topic for every event that cannot be indexed. A poison message never blocks a
  partition indefinitely. Replay is configuration, not tooling (section 9).
- Named identities per cluster, for Kafka and Elasticsearch alike: each consumer
  group, DLQ producer and route references an identity (SASL, API key, basic or
  mTLS) by name, so Kafka ACLs and ES roles can be scoped per pipeline and
  reader/writer roles can be separated.
- Configuration is **Spring Boot configuration** (`kafkafim.*` properties bound to
  typed `@ConfigurationProperties`, Bean-Validation checked) rendered by **Helm** into
  a ConfigMap, with secrets imported from mounted Kubernetes `Secret`s via Spring's
  `configtree`. Edited daily in git by one team; hot-reloaded without restarts.
- First-class OpenTelemetry (metrics, traces, logs) exported over OTLP to Elastic
  APM Server, Prometheus endpoint for free, Grafana dashboards and alert rules in
  the repo.
- Local demo stack (Kafka + ES + kafkafim) on Docker Compose and Docker Desktop
  Kubernetes; the production Helm chart deploys only kafkafim.
- kafkafim **never provisions resources**. Topics, consumer groups, credentials,
  indices, index templates, ingest pipelines, users and roles are created by the
  team's external provisioning systems before a pipeline is deployed. kafkafim
  verifies what it needs at startup and fails fast with a clear error.

### Non-goals (MVP)

- **Transformations.** Records are indexed as received (after optional JSON-array
  explosion). Enrichment and reshaping belong to the producing service or to ES
  ingest pipelines, which a route can name.
- **Expression language.** Routing is rules, not code. If a case cannot be expressed
  with the rule set, the answer is an ES ingest pipeline or a new rule type, not CEL.
- **CLI subcommands.** One run mode. Validation is fail-fast at startup and on
  reload; CI validates by booting with a `validate` profile (section 5.6). DLQ
  inspection uses standard Kafka tooling; replay is a pipeline (section 9).
- A UI. GitOps + Grafana is the UI.
- Non-JSON payloads, Avro, Schema Registry.
- `update`/`delete` operations (model leaves room; MVP implements `index`/`create`).
- Multi-tenancy, RBAC, audit log of config changes (git history is the audit log).
- Exactly-once. Idempotent `_id` gives the same observable result.

## 2. Alternatives considered

| Option | Why not |
|---|---|
| Keep Logstash | The stated pain: scale, config sprawl, DLQ handling, disk queues, threads ≠ partitions, weak observability. |
| Kafka Connect + ES sink | One Connect cluster per Kafka cluster (4), config via REST not git, routing limited to SMTs, JMX not OTel, scaling by `tasks.max` not per-topic pods. |
| Vector | Strong on YAML + hot reload, but ES sink has no per-event DLQ for bulk item rejections (logged and dropped), no fan-out control, no per-partition worker bound. |
| Build (this doc) | Every requirement maps to a design decision we control. Cost: we own a Kafka consumer loop and an ES bulk client, both on mature JVM libraries. |

## 3. Terminology

- **Cluster**: a named Kafka or ES connection (`kafkafim.kafka.clusters.*`,
  `kafkafim.elasticsearch.clusters.*`).
- **Identity**: a named credential set defined under a cluster. Kafka: SASL and/or
  client certificate, referenced from a pipeline's consumer and DLQ producer.
  Elasticsearch: API key, basic auth or client certificate, referenced from a route.
- **Pipeline**: one consumer group reading one or more topics from one Kafka
  cluster under one identity, routing each document to one or more (ES cluster,
  index) targets.
- **Route**: an ordered rule inside a pipeline: `when` conditions → `cluster`,
  `identity`, `index` pattern, optional ES ingest `pipeline`.
- **Group**: a set of pipelines deployed together as one Kubernetes `Deployment`.
  The unit of scaling and isolation.

## 4. Architecture

```
                 ┌────────────────────────── Kubernetes namespace ───────────────────────────┐
                 │  Deployment "orders" (replicas: 6)            Deployment "small" (2)       │
 Kafka A ──────▶ │  ┌──────────────┐ ┌──────────────┐           ┌──────────────┐             │
 Kafka B ──────▶ │  │ kafkafim pod │ │ kafkafim pod │ ...       │ kafkafim pod │ ...         │ ──▶ ES 1..20
 Kafka C ──────▶ │  │ pipelines:   │ │ pipelines:   │           │ pipelines:   │             │
 Kafka D ──────▶ │  │  orders      │ │  orders      │           │  audit, iam, │             │
                 │  └──────┬───────┘ └──────┬───────┘           │  billing ... │             │
                 │         │                │                   └──────┬───────┘             │
                 │   ConfigMap: application-kafkafim.yaml (Spring config, per group)         │
                 │   Secrets: mounted dirs → spring.config.import=configtree:...             │
                 │   OTLP ──▶ APM Server ──▶ Grafana                                         │
                 └────────────────────────────────────────────────────────────────────────────┘
```

One Spring Boot application, one mode: start, bind and validate `kafkafim.*`, start a
consumer container per pipeline, watch the config for changes.

### 4.1 Deployment model: groups

Every replica of a group runs every pipeline in that group. Kafka consumer-group
rebalancing spreads partitions across replicas. This gives:

- **HA for small topics**: a topic with 3 partitions in a group with 2 replicas has
  consumers on 2 pods; if one dies, partitions move within a session timeout.
- **Dedicated capacity for big topics**: a group with a single pipeline and
  `replicas ≤ partitions`.
- **Workers ≤ partitions by construction**: a replica only processes the partitions
  it is assigned. Container concurrency is capped at the partition count at startup
  (read from topic metadata), so misconfiguration cannot exceed it.

Chart defaults: `replicas ≥ 2` for every group, `PodDisruptionBudget minAvailable: 1`,
preferred anti-affinity across nodes and zones, `maxSurge: 1, maxUnavailable: 0`.

### 4.2 Pod internals

```
 for each pipeline in the group:
   spring-kafka listener container (batch listener, manual out-of-order acks,
   cooperative-sticky, concurrency ≤ partitions)
      │ batches of records (bounded by max in-flight bytes)
      ▼
   decoder (object | array of objects → N docs) ─▶ id/version/timestamp extraction ─▶ router (rules)
      │                                                                                 │
      │ per-partition offset tracker ◀───── acks ─────┐                                 ▼
      ▼                                               │          actions tagged with
   batcher (per ES cluster+identity): flush on        │          (cluster, identity, index, _id, op, pipeline)
     docs | bytes | age(ms)                           │
      ▼                                               │
   bulk sender (per ES cluster+identity): N in-flight, retry, breaker ──▶ ES (_bulk, NDJSON)
      │
      └─ non-retryable item errors ─▶ DLQ producer (same Kafka cluster as source)
```

Concurrency: decode/route runs on the consumer thread per batch; one batcher and a
bounded pool of bulk senders (virtual threads) per (ES cluster, identity) per
pipeline, because a bulk request carries exactly one `Authorization` header. A
pipeline reading 64 low-traffic partitions still produces full-size bulks because
batching is per target, not per partition.

### 4.3 Offsets and delivery guarantee

- Offsets are committed only after every document from that record has been acked by
  ES or written to the DLQ. A record that explodes into 50 documents is committed
  when all 50 are done.
- Per-partition tracker: completed offsets are recorded as they ack (bulks complete
  out of order); the container's out-of-order acknowledgement mode commits the
  highest offset such that all lower offsets are complete. Commit cadence: every
  `commit-interval` (default 1s) and on revoke/shutdown.
- Rebalance safety: cooperative-sticky assignor; on revoke we flush or abandon
  in-flight batches for the revoked partitions and commit what completed. Anything
  uncommitted is re-delivered to the new owner. Because `_id` is deterministic,
  re-delivery is harmless.
- `offset-reset`: `latest` by default (production migration), `earliest` configurable.

### 4.4 Idempotence details

- `_id` from `document.id-field`, a JSON pointer into each object (default `/id`).
  Ids always come from the JSON content, never from the Kafka key. If missing →
  DLQ with `error.type=missing_id`, unless `document.id-missing: auto` (ES-generated
  id; duplicates on retry, startup warning).
- Optional external versioning: `document.version-field`, `version_type: external`.
  On `version_conflict_engine_exception` the item is counted as `stale` and treated
  as success (`document.on-version-conflict: ignore|dlq`).
- Data streams require `op: create`. A retried item that already landed returns 409;
  for `op: create` a 409 is treated as success. No current pipeline targets a data
  stream; the path is supported and covered by an integration test from 0.3.

### 4.5 Decoding

A Kafka value is always a JSON object or a JSON array of objects. Decoding is not
configurable: an object yields one document, an array yields one document per
element, and the commit for the record waits for all of them. Anything else (invalid
JSON, a scalar, `null`, an array containing non-objects) goes to the DLQ with
`error.type=decode`; the DLQ record carries the original bytes and, for a bad array
element, `kafkafim.doc-index`.

Timestamps for `%{+...}` index patterns are read from `document.timestamp-field`
and parsed per `document.timestamp-format`: `iso-8601` (default, offset or `Z`
required), `epoch-millis`, `epoch-seconds`, or `pattern:<DateTimeFormatter>`. An
unparsable or missing value follows `on-bad-timestamp`: `dlq` (default, so a daily
index never silently receives misdated documents) or `ingest-time`.

### 4.6 No provisioning, fail-fast verification

kafkafim creates nothing in Kafka or Elasticsearch. At pipeline start it verifies
what the external provisioning systems should already have created, using only the
identities the pipeline references:

- source topics exist and are readable (`Describe` on topics, metadata fetch);
- the DLQ topic exists (`Describe`); kafkafim never relies on auto-create;
- the consumer group is permitted (first join is the check);
- for each route's ES cluster, the identity authenticates (one lightweight
  authenticated request); index templates, ingest pipelines and roles are assumed
  present and any gap surfaces as a classified bulk error at runtime.

A failed check keeps the pipeline stopped, records
`kafkafim_pipeline_state{pipeline,state="failed_preflight"}`, logs the exact
missing resource and identity, and leaves the other pipelines in the pod running.
Readiness stays green for the pod if at least one pipeline runs; the alert fires on
the metric.

## 5. Configuration model

Everything is Spring Boot configuration under the `kafkafim` prefix, bound to typed
records with `@ConfigurationProperties` and checked with Bean Validation plus a
cross-reference validator (every `cluster` and `identity` name must exist, exactly
one default route unless `route-mode: all`, patterns must parse). The same model is read from
`application.yaml` locally and from a Helm-rendered ConfigMap in production.

### 5.1 Clusters

```yaml
kafkafim:
  kafka:
    clusters:
      prod-a:
        bootstrap-servers: [kafka-a-1:9093, kafka-a-2:9093]
        security-protocol: SASL_SSL
        ssl:                                 # cluster-wide trust (CA), shared by all identities
          truststore-location: /etc/kafkafim/secrets/kafka/prod-a/truststore.p12
          truststore-password: ${kafka.prod-a.truststore-password}
        identities:                          # named credential sets, referenced by pipelines
          orders-reader:
            sasl:
              mechanism: SCRAM-SHA-256
              username: ${kafka.prod-a.orders-reader.username}   # from configtree
              password: ${kafka.prod-a.orders-reader.password}
          dlq-writer:
            sasl:
              mechanism: SCRAM-SHA-256
              username: ${kafka.prod-a.dlq-writer.username}
              password: ${kafka.prod-a.dlq-writer.password}
          platform-cert:                     # mTLS identity
            ssl:
              keystore-location: /etc/kafkafim/secrets/kafka/prod-a.platform-cert/keystore.p12
              keystore-password: ${kafka.prod-a.platform-cert.keystore-password}
        default-identity: platform-cert      # optional; used when a pipeline names none
        properties: {}                       # passthrough to kafka-clients
  elasticsearch:
    clusters:
      search-eu:
        uris: [https://es-eu-1:9200, https://es-eu-2:9200]
        ssl:                                 # cluster-wide trust, shared by all identities
          ca-certificate: /etc/kafkafim/secrets/elasticsearch/search-eu/ca.crt
        identities:                          # named credential sets, referenced by routes
          orders-writer:                     # production default: basic auth over TLS
            username: ${elasticsearch.search-eu.orders-writer.username}
            password: ${elasticsearch.search-eu.orders-writer.password}
          ingest-api-key:
            api-key: ${elasticsearch.search-eu.ingest-api-key.api-key}
          platform-cert:                     # mTLS identity (PKI realm), optional
            ssl:
              keystore-location: /etc/kafkafim/secrets/elasticsearch/search-eu.platform-cert/keystore.p12
              keystore-password: ${elasticsearch.search-eu.platform-cert.keystore-password}
        default-identity: orders-writer      # optional; used when a route names none
        bulk:
          max-docs: 5000
          max-bytes: 5MB
          max-age: 200ms
          in-flight: 4
        compression: true
```

### 5.2 Pipelines

```yaml
kafkafim:
  pipelines:
    orders:
      source:
        cluster: prod-a
        topics: [orders.v2, orders.backfill]
        group-id: kafkafim.orders            # default: kafkafim.<name>
        identity: orders-reader               # consumer identity; default: cluster.default-identity
        offset-reset: latest
        max-record-bytes: 8MB                 # value is a JSON object or an array of objects
      document:
        id-field: /order_id                   # JSON pointer into each object; default /id
        version-field: /version               # optional → version_type external
        timestamp-field: /@timestamp          # for %{+...} patterns; default: ingest time
        timestamp-format: iso-8601            # iso-8601 (default) | epoch-millis | epoch-seconds | pattern:<DateTimeFormatter>
        on-bad-timestamp: dlq                 # dlq (default) | ingest-time
        op: index                             # index | create
      routes:                                 # evaluated in order
        - when:
            - { field: /region, equals: eu }
          cluster: search-eu
          identity: orders-writer             # ES identity; default: cluster.default-identity
          index: "orders-eu-%{+yyyy.MM}"
          pipeline: orders-enrich             # ES ingest pipeline (optional)
        - when:
            - { field: /region, in: [us, ca] }
            - { field: /tier, exists: true }
          cluster: search-us
          index: "orders-%{region}-%{+yyyy.MM.dd}"   # no identity → search-us default-identity
        - default: true
          cluster: search-eu
          identity: orders-writer
          index: orders-unrouted
      route-mode: first                       # first | all (fan-out to every match)
      dlq:
        topic: orders.kafkafim-dlq            # default: <first topic>.kafkafim-dlq
        identity: dlq-writer                  # producer identity; default: the source identity
        max-attempts: 10                      # per document, retryable errors only
        max-elapsed: 10m
      latency:
        max-age: 200ms                        # overrides cluster bulk.max-age
```

### 5.2.1 Identities (Kafka and Elasticsearch)

An **identity** is a named credential set defined under a cluster. Trust material
(truststore / CA) and the transport security settings belong to the cluster and are
shared by all its identities. Every reference to an identity is validated at bind
time against the named cluster; identity names are unique per cluster; a cluster may
declare a `default-identity`.

**Kafka identity**: a `sasl` block (mechanism, username, password; production uses
`SCRAM-SHA-256`; GSSAPI or OAUTHBEARER via `properties`), an `ssl` keystore block
for mTLS, or both.

- `pipelines.<name>.source.identity`: the consumer group's identity → falls back to
  the cluster's `default-identity` → validation fails if the security protocol
  requires authentication and none resolves.
- `pipelines.<name>.dlq.identity`: the DLQ producer's identity → falls back to the
  resolved source identity. A read-only account can consume while a shared writer
  account owns every DLQ topic, or the reverse.
- Consumer and producer factories are keyed by `(cluster, identity)`; each container
  still owns its own `KafkaConsumer`; one DLQ producer per `(cluster, identity)`.
- ACLs per identity: consumer → `Read`/`Describe` on topics and group; DLQ writer →
  `Write`/`Describe` on the DLQ topics it serves.

**Elasticsearch identity**: exactly one of `username`+`password` (production
default), `api-key`, or an `ssl` keystore block (PKI realm); a keystore may be
combined with basic or `api-key` when the cluster requires client certificates on
top of user auth. Trust (CA) is per cluster. Production today is basic auth with the
cluster CA for trust only; client certificates are modelled but not exercised in
the MVP tests.

- `routes[].identity`: the identity used for that route's bulk items → falls back to
  the cluster's `default-identity` → validation fails if the cluster is `https` with
  security enabled and none resolves (plain `http` with no identity is allowed and
  warned, for local development).
- One `RestClient` per `(cluster, identity)`, since the low-level client fixes the
  `Authorization` header (or client certificate) at construction. Batching and bulk
  senders are therefore per `(cluster, identity)`; a pipeline whose routes use two
  identities on one cluster produces two bulk streams to it.
- The circuit breaker stays per **cluster**: a 5xx or connection failure says
  something about the cluster, not the identity.
- `401`/`403` on a bulk request is a **configuration failure, not a data failure**:
  the affected pipeline pauses, retries with backoff, raises
  `kafkafim_auth_failures_total{es_cluster,identity}` and an alert. Documents are
  never sent to the DLQ because of a bad credential.
- Privileges per identity: `write` (or `create_doc` for `op: create`) and
  `auto_configure`/`create_index` as needed on the index patterns it serves; data
  streams additionally need `create_index` on the stream; a route naming an ingest
  `pipeline` needs `read_pipeline` on it. No cluster-level privileges; kafkafim
  never calls cluster health.

**Common consequences**

- Secrets are mounted one per identity; the configtree path is free-form and the
  placeholders inside the identity decide which files are used.
- Metrics carry `kafka_cluster`/`es_cluster` and `pipeline`; `identity` is an optional
  low-cardinality label on connection and auth metrics. Credentials never appear in
  labels or logs.
- Reload: changing an identity restarts every pipeline that references it (as
  consumer, DLQ producer or route) and nothing else.

### 5.3 Routing rules (no expression language)

A route matches when **all** of its `when` conditions hold. Condition types:

| Condition | Meaning |
|---|---|
| `equals: v` | field is a string/number/bool equal to `v` |
| `in: [a, b]` | field value is one of the list |
| `exists: true|false` | field present (and not null) / absent |
| `prefix: s` | string field starts with `s` |
| `matches: regex` | string field matches (anchored, precompiled) |
| `header: name, equals: v` | Kafka record header equality |
| `topic: name` | source topic equality (for multi-topic pipelines) |

`field` is a JSON pointer (`/a/b/0`). Index patterns use the Logstash syntax the
team already knows: `%{field}` (a JSON pointer or a top-level name) and
`%{+yyyy.MM.dd}` (Java `DateTimeFormatter` pattern over `document.timestamp-field`,
or ingest time if unset). Resolved index names are lower-cased; a pattern whose field
is missing routes the document to the DLQ with `error.type=unresolved_index`.

This covers: static indices, daily/monthly indices from event time, per-region or
per-tenant clusters, topic-to-index merging, and fan-out. It deliberately cannot do
arithmetic, string manipulation or nested logic.

### 5.4 Helm rendering

Full chart design in [HELM.md](HELM.md). Essentials: **one release per Kubernetes
cluster**; `values.yaml` carries `config.kafkafim.{kafka,elasticsearch,pipelines}`,
a `secrets[]` list of existing `Secret`s to mount, and a `groups` map. Per group the
chart renders a `Deployment`, a `ConfigMap` with `application-kafkafim.yaml`
(shared cluster definitions + only that group's pipelines), a `PodDisruptionBudget`,
a `Service` and an optional CPU `HorizontalPodAutoscaler`. Pods start with:

```
SPRING_CONFIG_ADDITIONAL_LOCATION=/etc/kafkafim/config/
SPRING_CONFIG_IMPORT=configtree:/etc/kafkafim/secrets/
```

The pod template carries **no** config checksum annotation, so a `helm upgrade`
that only changes pipelines updates the ConfigMap and pods hot-reload. Image,
resource and mount changes still roll the pods normally. The chart creates no
`Secret`, has no subcharts, and exposes `image.registry` for the air-gapped
registry.

### 5.5 Secrets

Spring's `configtree` import turns each file in a mounted `Secret` directory into a
property named after its path relative to the import root:
`/etc/kafkafim/secrets/kafka/prod-a.orders-reader/password` becomes
`kafka.prod-a.orders-reader.password`. The top-level `kafka.*` and
`elasticsearch.*` property namespaces are therefore reserved for secrets; all
application properties live under `kafkafim.*`. Cluster trust settings and identity
definitions reference those properties with `${...}` placeholders, so the rendered
ConfigMap never contains a secret. Startup validation refuses literal values in `password`,
`api-key` and `*-password` fields unless `kafkafim.allow-inline-secrets=true`
(local development only). Secret provisioning stays outside the chart
(`existingSecret` names): plain Secrets, Sealed Secrets, External Secrets or a Vault
agent all work. A changed secret file is picked up by the same reload path.

### 5.6 Validation and hot reload

- **Startup**: binding failure or validation failure stops the pod with a clear
  message; readiness never goes green.
- **CI**: the image is run with `--spring.profiles.active=validate` and the
  candidate ConfigMap mounted; the profile binds and validates, logs the pipeline
  summary, and exits 0/1 without connecting anywhere. This replaces a CLI.
- **Reload**: a watcher on the config and secret directories (Java `WatchService` +
  10s hash poll, because kubelet updates volumes by symlink swap with up to ~1 min
  delay) plus `POST /actuator/reload`. The new tree is loaded with Spring's
  `Binder` into a fresh model and validated as a whole. On success the supervisor
  diffs by pipeline: changed pipelines are drained (stop fetching, flush in-flight,
  commit, leave group) and restarted; added start; removed drain and stop; unchanged
  are untouched. Cluster definition changes restart every pipeline using that
  cluster; an identity change restarts only the pipelines referencing it. An invalid tree is rejected whole; the running config stays, a metric and
  log record the failure.

## 6. Scaling

- **Static**: `replicas` per group in `values.yaml`, capped by the smallest partition
  count in the group (startup warning).
- **HPA on CPU** (core Kubernetes): CPU tracks throughput closely for JSON decode and
  gzip. Optional per group.
- **Lag-based**: needs KEDA (a CRD) or a custom-metrics adapter, not available in
  production. `kafkafim_consumer_lag` is exported so this is a chart toggle later.
- Scaling beyond partitions is wasted; repartitioning is a Kafka-side action.
- Per-cluster knobs: `bulk.max-docs/max-bytes/max-age/in-flight`; per pipeline
  `source.max-poll-records`, `max-in-flight-bytes` (memory bound).

## 7. Backpressure and failure handling

| Situation | Behaviour |
|---|---|
| ES returns 429 / 503 / connection error | Exponential backoff with jitter per cluster; the pipeline's container is paused when in-flight bytes hit the budget. Kafka lag grows. No local buffering. |
| Repeated cluster-level failures | Circuit breaker per ES cluster (Resilience4j). Pipelines routing to that cluster pause; others proceed. |
| Bulk request rejected with 401 / 403 | Configuration failure, not data failure: the pipeline pauses, retries with backoff, `kafkafim_auth_failures_total` and an alert fire. Nothing goes to the DLQ. |
| Bulk partial failure | Only failed items are retried; successful items are acked immediately. |
| Non-retryable item error (400 family: `mapper_parsing_exception`, `strict_dynamic_mapping_exception`, `illegal_argument_exception`, `document_parsing_exception`) | DLQ immediately, partition continues. |
| Retryable item error exceeding `max-attempts`/`max-elapsed` | DLQ, partition continues. This is the poison-message bound. |
| Oversized document (413) | DLQ. |
| Undecodable record, non-object element, missing `_id`, bad timestamp, no matching route, unresolved index pattern | DLQ with the matching `error.type` (`decode`, `missing_id`, `bad_timestamp`, `no_route`, `unresolved_index`). |
| Missing topic, DLQ topic, or failed authentication at pipeline start | Pipeline stays stopped (`failed_preflight`), clear log naming resource and identity, alert. Other pipelines unaffected. kafkafim never creates the resource. |
| DLQ produce fails | Block the partition and alert. Losing data is worse than lag. |
| Kafka unavailable | Reconnect with backoff; `kafkafim_kafka_connected` gauge drops, alert. |
| Pod SIGTERM | Spring graceful shutdown: stop fetching, flush in-flight (bounded by `terminationGracePeriodSeconds`, default 60s), commit, leave group. |

Error classification lives in one table (`sink/es/ErrorClassifier`) and is
unit-tested against captured ES responses, because it decides data loss versus stall.

## 8. No transformations

Documents are indexed byte-for-byte as decoded (a JSON array record is split into
its elements). Field extraction for `_id`, version, timestamp and routing is
read-only. Reshaping and enrichment happen in the producer or in an ES ingest
pipeline named on the route. This keeps kafkafim CPU-light, which is what lets a
small pod drain a big backlog, and keeps the MVP honest.

## 9. DLQ

DLQ records are produced to a topic on the **same Kafka cluster as the source**, so
no extra credentials or network paths. Record layout:

- key: original key
- value: original value, byte-for-byte
- headers: `kafkafim.pipeline`, `kafkafim.error.type`, `kafkafim.error.reason`
  (ES item error JSON, truncated to 4 KiB), `kafkafim.source.topic`,
  `kafkafim.source.partition`, `kafkafim.source.offset`, `kafkafim.target.cluster`,
  `kafkafim.target.index`, `kafkafim.attempts`, `kafkafim.first-failure-ts`,
  `kafkafim.doc-index` (position inside a JSON array record); original headers kept.

**Replay is configuration.** After fixing a mapping, add a pipeline whose source is
the DLQ topic and whose routes mirror the original (or point at a repair index):

```yaml
kafkafim:
  pipelines:
    orders-replay:
      source: { cluster: prod-a, topics: [orders.kafkafim-dlq], offset-reset: earliest }
      document: { id-field: /order_id }
      routes: [ { default: true, cluster: search-eu, index: "orders-eu-%{+yyyy.MM}" } ]
      dlq: { topic: orders.kafkafim-dlq-2 }
```

Remove it when `kafkafim_consumer_lag{pipeline="orders-replay"}` is zero. Inspection
uses standard Kafka tooling (`kafka-console-consumer` with headers, Kafka UI).
Purge is retention. A dedicated DLQ tool is a post-MVP item.

Metrics: `kafkafim_dlq_records_total{pipeline,error_type}`; the DLQ topic's consumer
lag under a replay pipeline is the unresolved-backlog number.

## 10. Observability

Spring Boot's OpenTelemetry starter: Micrometer → OTLP (APM Server) plus
`/actuator/prometheus`; Micrometer Tracing with the OTel bridge → OTLP. APM Server
(or later an OpenTelemetry Collector) pre-exists in production; switching between
them is only the `OTEL_EXPORTER_OTLP_ENDPOINT` value in the chart.

### Metrics (all with `pipeline`, `kafka_cluster`; `topic`/`es_cluster`/`route` where it applies)

- `kafkafim_records_consumed_total`, `kafkafim_bytes_consumed_total`
- `kafkafim_docs_total{outcome=indexed|stale|dlq}`
- `kafkafim_bulk_requests_total{status}`, `kafkafim_bulk_latency_seconds`,
  `kafkafim_bulk_docs`, `kafkafim_bulk_bytes` (histograms)
- `kafkafim_item_errors_total{error_type,retryable}`
- `kafkafim_retries_total`, `kafkafim_breaker_state{es_cluster}`,
  `kafkafim_auth_failures_total{es_cluster,identity}`
- `kafkafim_consumer_lag{topic}` (owned partitions, end offsets polled every 10s)
- `kafkafim_e2e_latency_seconds` (ack time minus Kafka record timestamp)
- `kafkafim_batch_flush_total{reason=docs|bytes|age}`
- `kafkafim_in_flight_bytes`, `kafkafim_assigned_partitions`
- `kafkafim_pipeline_state{pipeline,state=running|paused|failed_preflight|stopped}`
- `kafkafim_config_reload_total{result}`, `kafkafim_config_generation`
- `kafkafim_dlq_records_total`

Label discipline: `route` is the route's position or `name`, never the resolved
index (daily indices would explode cardinality). Partition labels are opt-in.

### Traces

One span per bulk request (`es.bulk`: cluster, docs, bytes, status, retries) under a
`pipeline.batch` span. Incoming `traceparent` Kafka headers are attached as **span
links**, not parents. Head sampling 1% of batches; errors always. No per-document spans.

### Logs

ECS-structured JSON on stdout (Spring Boot structured logging); OTLP log export
optional. One line per bulk failure (first N item errors), per DLQ record, per
reload. Document bodies are never logged by default.

### Health

- `/actuator/health/liveness`: process alive, supervisor running.
- `/actuator/health/readiness`: config bound and valid, Kafka clients connected.
  Deliberately **not** gated on ES health.

### Shipped with the repo

`deploy/grafana/`: overview dashboard (lag, throughput, e2e latency, DLQ rate per
pipeline), per-ES-cluster dashboard (bulk latency, 429s, breaker), alert rules: lag
growing 5m, DLQ rate > 0 for 10m, bulk reject ratio > 1%, e2e p99 above target,
breaker open, auth failures > 0, reload failed, pod with 0 assigned partitions for a
pipeline it serves.

## 11. Security

- Secrets only via mounted `Secret`s imported as `configtree`; inline secret values
  fail validation outside local development.
- TLS everywhere, custom CA per cluster. Named identities (Kafka: SASL/mTLS;
  Elasticsearch: API key, basic, mTLS) referenced per consumer group, DLQ producer
  and route, so Kafka ACLs and ES roles can be scoped per pipeline and per role.
- Container: layered jar on a minimal JRE image, non-root, read-only root filesystem,
  no capabilities.
- `/actuator/reload` is reachable only in-cluster via the Service; a
  `NetworkPolicy` in the chart (opt-in).

## 12. Technology choices

Summary; see [TECH_STACK.md](TECH_STACK.md) and the ADRs.

- **Java 27 on Spring Boot 4.1** (ADR-0002): configuration binding and validation,
  Actuator probes, OpenTelemetry starter, Kafka integration, graceful shutdown,
  Docker Compose and Testcontainers support.
- **spring-kafka programmatic listener containers** (ADR-0004): one per pipeline,
  batch listener, manual out-of-order acks, cooperative-sticky, pause/resume.
- **Elasticsearch low-level REST client** (ADR-0005): HTTP only, version-agnostic
  across the 8.x fleet; we build `_bulk` bodies and parse per-item results.
- **Resilience4j** core for breaker and retry (ADR-0016). **Jackson 3** for JSON.
- Tests: JUnit 5 + Testcontainers (Kafka, Elasticsearch, Grafana LGTM); chaos cases
  (ADR-0019).

## 13. Repository layout

```
pom.xml, mvnw                    Maven build (Spring Boot parent)
compose.yaml                     local Kafka + Elasticsearch + Grafana LGTM
src/main/java/io/github/matabar/kafkafim/
  config/         @ConfigurationProperties model, validators, watcher, Binder reload, diff
  supervisor/     pipeline lifecycle, hot reload orchestration
  source/kafka/   consumer factories per cluster, container per pipeline, offset tracker
  decode/         object / array-of-objects decoding, field extraction (JSON pointer), timestamp parsing
  route/          condition evaluation, index pattern resolution
  sink/es/        RestClient per cluster, bulk builder, item parser, error table, breaker
  dlq/            producer, record format
  telemetry/      meters, spans, health indicators, reload endpoint
src/main/resources/application.yaml   defaults + local-dev sample pipeline
src/test/java/...                     unit + Testcontainers integration tests
charts/kafkafim/                      production Helm chart
deploy/local/                         Kubernetes demo stack (Kafka + ES operators, local only)
deploy/grafana/                       dashboards + alert rules
docs/                                 DESIGN.md, TECH_STACK.md, decisions/, runbook
```

## 14. Migration from Logstash

Idempotent `_id`s make this low-risk:

1. Translate one Logstash pipeline into a `kafkafim.pipelines.<name>` block. Index
   patterns carry over almost verbatim (`%{+YYYY.MM.dd}` → `%{+yyyy.MM.dd}`).
2. Deploy with a **new** consumer group at `offset-reset: latest` while Logstash
   keeps running. Same `_id`s → identical documents. Compare counts and DLQ rate.
3. Stop the Logstash pipeline. No gap, no duplicates.
4. Pipelines with ES-generated ids: stop Logstash first, then start kafkafim with
   `source.group-id` set to Logstash's group so it continues from its offsets.

## 15. Delivery and Helm

See [DELIVERY.md](DELIVERY.md) and [HELM.md](HELM.md). In one paragraph: GitHub
Actions CI runs unit, integration (Testcontainers), architecture (ArchUnit), image
and chart checks on every PR; a `v*` tag publishes a signed multi-arch image to
`ghcr.io/matabar/kafkafim`, a signed OCI chart to `ghcr.io/matabar/charts/kafkafim`
and an air-gap bundle (amd64 image archive, chart, SBOM, checksums, import
instructions) on the GitHub Release. Production is air-gapped; the team imports the
bundle into the internal registry and installs one Helm release per Kubernetes
cluster. The build is a single Maven module with CI-friendly `${revision}`
versioning (ADR-0003, ADR-0022, ADR-0023).

## 16. Roadmap

- **0.1** single pipeline end to end: bind config, preflight checks, consume, route,
  bulk, commit, metrics, DLQ. SCRAM-SHA-256 and basic auth identities. Compose stack.
- **0.2** groups, Helm chart, configtree secrets, hot reload, `validate` profile.
- **0.3** traces/logs, Grafana dashboards + alerts, breaker, versioning, data streams.
- **1.0** runbook, load-test results, first production pipelines, Logstash
  config translator for the common 80%.
- **Later**: DLQ tooling, transformations (if ever, as ES ingest pipelines first),
  `update`/`delete` ops, KEDA where allowed, read-only UI over the same config.

## 17. Open questions

- Elasticsearch TLS: confirm the certificate in production is a CA for trust only
  (assumed), not a client certificate presented with basic auth.
- GitOps tool used to apply the per-cluster Helm release inside the air gap (Argo CD,
  Flux, plain `helm upgrade`)? Affects documentation only.
