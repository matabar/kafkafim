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
  partition indefinitely. Replay is configuration, not tooling (section 9). The only
  documents that reach neither Elasticsearch nor the DLQ are those a `drop: true` route
  selects, and each one is counted (§5.3, ADR-0037).
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
- Local demo stack (Kafka + ES + kafkafim) on Docker Compose and on a local Kubernetes
  cluster (kind or Docker Desktop, `deploy/local/`); the production Helm chart deploys
  only kafkafim.
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
  reload; the infrastructure repository's CI can validate rendered configuration by
  booting the image with the `validate` profile (section 5.6). DLQ inspection uses
  standard Kafka tooling; replay is a pipeline (section 9).
- A UI. The infrastructure repository (git) + Grafana is the UI.
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
   spring-kafka listener container (batch listener, own out-of-order offset tracker,
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

The sink (`sink/es/`) takes `BulkAction`s (index, `_id` or none, op, external version,
ingest pipeline, source bytes, an opaque completion handle) and reports one outcome per
action through a callback: `indexed`, `stale`, or `dlq` with an `error.type` and the ES
reason. The supervisor maps decoded and routed documents to actions, and outcomes to
offset acks or DLQ records; the sink knows nothing about Kafka.

The supervisor (`supervisor/`) owns one runtime per pipeline: the compiled decoder and
router, one bulk sink per `(cluster, identity)` its routes use, its DLQ writer and its
Kafka source. Per record it plans every part before dispatching any and declares their
number (`SourceRecord.expectParts`): one part per routed copy of a decoded document
(`route-mode: all` fan-out makes several), one per document that fails decoding or
routing (`missing_id`, `bad_timestamp`, `decode` with `doc-index`, `no_route`,
`unresolved_index` with the route's `target.cluster`), one per document a drop route
selected (§5.3), and exactly one for a record that fails as a whole (oversize, never
decoded; undecodable; tombstone). Each parsed object is routed before its id, version and
timestamp problems apply: decoding extracts them and keeps any error with the document,
and the error becomes a part only for a document that routing sends to Elasticsearch,
that is, every document a drop route did not select, including one no route selected (it
reports its id, version or timestamp error rather than `no_route`). It stays one part per
document, with `route="none"` (§10), however many copies `route-mode: all` would have
made. The timestamp is still parsed before any `%{+...}` pattern is resolved. A part
completes on `indexed` or `stale`, when the broker acknowledged its DLQ record (a sink
`dlq` outcome or a decode/route failure), or, for a dropped document, as soon as it is
dispatched; a DLQ send failed by shutdown leaves the record uncommitted. A dropped
document gets no source byte copy and no index resolution, so a record whose documents
all drop commits at once, like an empty array, and holds its `max-in-flight-bytes` share
only until then. Array elements drop one by one while the rest go ahead.
Outcomes for a record whose partition was revoked are ignored and not dead-lettered (the
next owner re-reads it). On revoke the supervisor seals the open batches and waits for
nothing. Ingest time is the record's Kafka timestamp (the wall clock for a record
without one), except that a pipeline without `document.timestamp-field` reading a record
that carries `kafkafim.source.timestamp` (a DLQ record being replayed, ADR-0027) uses that
original timestamp. The supervisor also refuses to start a pipeline whose configuration
would make Elasticsearch reject whole bulk requests (`op: create` with
`document.version-field`, a blank ingest `pipeline`); config validation rejects both
already (§5.7), so this guards a tree that bypassed it.

Pause reasons (`es-breaker:<cluster>`, `es-auth:<cluster>/<identity>`, `dlq-blocked`)
belong to the pipeline, not to its source. Auth events are hints: the supervisor re-reads
the current sink's auth state, so a late event from a closed sink cannot leave a pause
behind. When a pipeline is restarted (reload), breaker and auth reasons are dropped,
because only the old run's senders were probing the breaker or retrying the identity; the
new sinks report them again if the cluster is still down, and the in-flight budget bounds
what is fetched meanwhile. `dlq-blocked` stays until the shared DLQ writer's retries
succeed, unless the restart gives the pipeline a new DLQ writer (§5.6), whose events are
then the only ones that count. A pipeline that is stopping cannot be started until the stop returns.

- **Batching**: a batch is sealed at `bulk.max-docs`, at `bulk.max-bytes` (an action
  that would push it over seals it first, so only a single action larger than
  `max-bytes` makes a larger request), when its oldest action is `max-age` old
  (`latency.max-age` when the pipeline sets it, else `bulk.max-age`), or on an explicit
  flush. Sizes count the uncompressed NDJSON body byte for byte.
- **Sending**: each sealed batch gets a virtual thread; at most `bulk.in-flight`
  requests are outstanding per stream, and batches start in the order they were sealed.
- **Backpressure**: `submit` never blocks, because the consumer thread must keep
  polling. Everything the sink holds is an action without an outcome, so it is already
  inside the pipeline's `max-in-flight-bytes` budget, at which the container pauses
  (ADR-0010). While ES is down or the breaker is open, outcomes stop, the budget
  fills, and the pipeline pauses; nothing is dropped.
- **Lifecycle**: `flush()` seals the open batch; `close(timeout)` rejects new actions,
  sends what is batched (retries are sealed at once from then on) and waits for every
  outcome. Actions still unresolved at the timeout normally get no outcome (one whose
  response arrives at that moment may still get it), so their offsets are not committed
  and the records are re-delivered (idempotent by `_id`).
- **Auth state**: the stream is failing from a request-level 401/403 until a request
  (any, even one sent earlier) is next answered with a bulk response or a
  single-document 413, and while any
  action waits to retry after an item-level 401/403; a 429, 5xx, transport error or other
  unexpected status leaves the state as it was. The listener hears one `onAuthFailure`
  per answered request that carries a 401/403 (request or items; never one per item, and
  again on every rejected retry, so `kafkafim_auth_failures_total` counts responses
  carrying a 401/403) and one `onAuthRecovered` when the stream leaves that state, so the
  supervisor can pause and resume without flapping. Both come on the sender thread, once
  `authFailing()` reflects the response and before that response's outcomes. Senders run
  concurrently, so the two events of one stream can arrive out of order: they are hints,
  and the supervisor re-reads the current sink's `authFailing()` (pause reasons, above).

### 4.3 Offsets and delivery guarantee

- Offsets are committed only after every document from that record has been acked by
  ES, written to the DLQ, or dropped by a drop route (§5.3). A record that explodes
  into 50 documents is committed when all 50 are done.
- Per-partition tracker (`source/kafka/OffsetTracker`, ADR-0025): completed records are
  recorded as they ack (bulks complete out of order); the committed offset is the lowest
  incomplete offset of the partition, or one past the highest delivered offset when none
  is incomplete. spring-kafka's `asyncAcks` is not used: it pauses the consumer after
  every poll until that poll is fully acked. A multi-document record declares its part
  count (`SourceRecord.expectParts`) and is complete when every part is; an empty array
  declares zero parts and is committable at once.
- Commit cadence: before a poll once `commit-interval` (default 1s) elapsed, also while
  paused or idle (the poll timeout is at most `commit-interval`); synchronously on
  revoke; and on shutdown. Commits run only on the consumer thread that owns the
  partition.
- Rebalance safety: cooperative-sticky assignor; on revoke the supervisor is told first
  (to flush or abandon in-flight batches for the revoked partitions), then what completed
  is committed and the partition's remaining records are abandoned: their late
  completions are ignored. On partition loss nothing is committed. Tracker state belongs to
  one consumer's assignment, so a late loss callback on a consumer that was replaced by
  another consumer of the same pod never drops the new owner's state. Anything
  uncommitted is re-delivered to the new owner. Because `_id` is deterministic,
  re-delivery is harmless.
- Records whose value exceeds `max-record-bytes` are handed over flagged as oversize and
  are never decoded; the supervisor sends them to the DLQ.
- `offset-reset`: `latest` by default (production migration), `earliest` configurable.

### 4.4 Idempotence details

- `_id` from `document.id-field`, a JSON pointer into each object (default `/id`).
  Ids always come from the JSON content, never from the Kafka key. If missing →
  DLQ with `error.type=missing_id`, unless `document.id-missing: auto` (ES-generated
  id; duplicates on retry, startup warning).
  A JSON string (1 to 512 UTF-8 bytes, ES's `_id` limit) is used as is; a JSON integer
  of any size becomes its decimal digits (canonical, since JSON has no leading zeros or
  `+`; `-0` is `0`). A number with a fraction or exponent, a boolean, an object or an
  array is `missing_id` in both modes: `auto` covers only an absent or `null` id.
- Optional external versioning: `document.version-field`, `version_type: external`.
  The version is a JSON integer or a string of ASCII digits in `[0, 2^63-1]`; anything
  else, including an absent or `null` version, is `error.type=decode`, because an
  unversioned write could overwrite a newer document.
  On `version_conflict_engine_exception` the item is counted as `stale` and treated
  as success (`document.on-version-conflict: ignore|dlq`).
- Data streams require `op: create`, which takes no `version-field` (§5.7). A retried
  item that already landed returns 409; for `op: create` a 409 is treated as success.
  No current pipeline targets a data stream; the path is supported and covered by an
  integration test from 0.3.

### 4.5 Decoding

A Kafka value is always a JSON object or a JSON array of objects. Decoding is not
configurable: an object yields one document, an array yields one document per
element, and the commit for the record waits for all of them. Anything else (invalid
JSON, a scalar, `null`, an array containing non-objects) goes to the DLQ with
`error.type=decode`; the DLQ record carries the original bytes and, for a bad array
element, `kafkafim.doc-index`.

- **Granularity.** A value that is not one valid JSON value fails as one record, with
  no `doc-index`: a tombstone, an empty or whitespace-only value, a value over
  `max-record-bytes` (rejected before parsing), invalid JSON or UTF-8 (including
  UTF-16/32, detected by a byte order mark or a NUL in the first four bytes), a scalar or
  `null`, trailing content, nesting deeper than 500, or a duplicate key at any depth.
  Duplicates are rejected because ES rejects them too, and because routing and `_id`
  would otherwise read whichever duplicate the parser kept. Inside a valid array each
  element stands alone: a non-object element (including a nested array), or an id,
  version or timestamp problem, fails only that element (`doc-index` set), and the
  other elements are indexed. A non-object element fails at once; an id, version or
  timestamp problem is held with the parsed object and applies after routing (§4.2).
  An empty array is zero documents, so its offset is committable at once.
- **Source bytes.** Each document is indexed from its own bytes in the value (the
  whole value for an object, the element's slice for an array), not from a
  re-serialised tree, so number spelling and key order survive. One leading UTF-8 BOM
  is skipped. A slice containing a line feed between tokens is copied once with the
  whitespace outside strings removed, because the bulk body is NDJSON.
- **Limits.** `max-record-bytes` bounds every string, number and token count before
  parsing. Nesting is capped at 500, number literals at 1000 digits and keys at
  50,000 UTF-8 bytes. Parsed trees are kept for routing and can take far more heap than
  the value: an 8 MiB array of empty objects retains about 237 MB (~85 bytes per
  element, ~28× the value), while `max-in-flight-bytes` counts raw bytes.

Timestamps for `%{+...}` index patterns are read from `document.timestamp-field`
and parsed per `document.timestamp-format`: `iso-8601` (default, offset or `Z`
required), `epoch-millis`, `epoch-seconds`, or `pattern:<DateTimeFormatter>`. An
unparsable or missing value follows `on-bad-timestamp`: `dlq` (default, so a daily
index never silently receives misdated documents) or `ingest-time`.

`epoch-millis` takes an integer, as a number or a digit string; `epoch-seconds` also
takes up to nine fraction digits (truncated to nanoseconds; a JSON number with a
fraction passes through a `double`, which is exact to the millisecond, so send a
string for more). `pattern:` parses with `Locale.ROOT` and strict resolution (the era
defaults to CE, so `yyyy` works and `02-30` fails). A pattern without an offset or
zone is read as **UTC**, one without any time field as midnight; a time that cannot
resolve (`hh` without `a`) or a pattern that yields no date fails. Era text is
`CE`/`BCE` under `Locale.ROOT`. `k` is the clock hour 1 to 24, whose `24` is the first
hour of the day (`24:30` is `00:30` of the same date), as java.time defines it; `HH`
rejects ISO-8601's end-of-day `24:00`, so such a value is a bad timestamp rather than
misdated. Every format rejects instants outside years 1 to 9999. "Ingest time" is the
Kafka record timestamp (for a replayed DLQ record, its `kafkafim.source.timestamp`; §9,
ADR-0027).

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

The local Compose stack follows the same rule: its broker has topic auto-creation
disabled, and a one-shot `kafka-init` service in `compose.yaml` stands in for the
provisioning systems by creating the `local` profile's `demo` topics
([README, Develop](../README.md#develop)).

How the supervisor runs it (`supervisor/Preflight`, `PipelineRuntime`): every pipeline
starts on its own thread, so one slow check never delays another pipeline or the
application context. The DLQ topic is described with the DLQ identity
(`DlqProducers.checkTopic`); each `(cluster, identity)` sends one `GET
/_security/_authenticate` (no privilege needed) through a client built like the sink's;
then the source starts, which fails for a missing source topic. All DLQ and
Elasticsearch checks run before a verdict, so one ERROR line names every problem.
Missing or undescribable topics, a 401/403, an unknown identity or unreadable trust
material, and a Kafka authentication or authorization error are **configuration**
failures (`failed_preflight` until the next accepted reload or a restart, §5.6). An unreachable broker or cluster
(timeout, connection error, 5xx) is **transient**: the pipeline stays `starting` and
retries (1s doubling to 30s), so a pod that starts during a short outage needs no
restart. Elasticsearch answers 405 on that path when security is disabled; any status
other than 401, 403 and 5xx passes. The consumer group's permission is not checked
before the first join; a denied join is retried by the source every 10s and logged.

Readiness (`pipelines` health contributor in the readiness group, next to `kafka`,
§10): up while at least one pipeline is `running` or `paused`; out of service while every pipeline is still
`starting` or has failed, so a rollout of a broken configuration stops at the first
pod; up with zero pipelines configured (valid, with a startup warning). A breaker or
auth pause never affects readiness.

## 5. Configuration model

Everything is Spring Boot configuration under the `kafkafim` prefix, bound to typed
records with `@ConfigurationProperties` and checked with Bean Validation plus a
cross-reference validator (every `cluster` and `identity` name must exist, exactly
one default route unless `route-mode: all`, patterns must parse). The same model is
read from the `local` profile locally (`application-local.yaml`, the default profile
via `spring.profiles.default: local`) and from a Helm-rendered
`application-kafkafim.yaml` in production, where `SPRING_PROFILES_ACTIVE=kafkafim`
both activates that file and disables the default profile, so the local sample never
binds in production. The packaged `application.yaml` holds only profile-independent
defaults (Actuator, OTLP, logging).

The model lives in `config/` (`KafkafimProperties` and its records); defaults are
applied while binding, so the bound tree already carries the effective values. Unknown
keys under `kafkafim.*` fail startup, so a typo (`idenity:`) never silently falls back to
a default; only a Kafka cluster's `properties` map accepts arbitrary keys. Section 5.7
lists every rule.

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
        properties: {}                       # passthrough to kafka-clients (keys kafkafim owns are rejected)
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
        client:                              # HTTP client timeouts (§7, ADR-0031)
          connect-timeout: 5s
          response-timeout: 60s              # a max-bytes bulk must be answered within it
```

A Kafka cluster's `properties` are passed to kafka-clients under kafkafim's own
consumer settings (`source/kafka/ConsumerSettings`). A key kafkafim owns is rejected when
the pipeline's consumer factory is built, naming the key but never its value:
`bootstrap.servers`, `security.protocol`, `group.id`, `group.instance.id`,
`group.protocol` (always `classic`, which the cooperative-sticky assignor needs),
`client.id` (`kafkafim-<pipeline>-<n>`), `enable.auto.commit` (always `false`),
`auto.offset.reset` and `max.poll.records` (from the `source` block), the key and value
deserializers (always byte arrays), `partition.assignment.strategy` (always
`CooperativeStickyAssignor`) and `allow.auto.create.topics` (always `false`, ADR-0021).
The security keys kafkafim derives from the cluster's `ssl` block and the identity's
`sasl`/`ssl` blocks (`ssl.truststore.*`, `ssl.keystore.*`, `ssl.key.password`,
`sasl.mechanism`, and `sasl.jaas.config` for `PLAIN`/`SCRAM-*`) are rejected only when
that block sets them, so GSSAPI or OAUTHBEARER can still bring their JAAS settings
through `properties`. Everything else (fetch sizes, timeouts, `isolation.level`, …)
passes through. Consumer factories are cached per `(cluster, identity)`; every
pipeline's container creates its own consumers.

The DLQ producer (`dlq/ProducerSettings`, one per `(cluster, identity)`, `client.id`
`kafkafim-<cluster>-<identity>-dlq`) takes the same map in three layers: kafkafim's
defaults (`compression.type=lz4`, `max.request.size` 16 MiB, `max.block.ms` 10s), then the
passthrough restricted to keys a producer knows (consumer-only keys such as
`fetch.min.bytes` are dropped; keys neither client knows, such as plugin settings, pass),
then kafkafim's own settings. It owns, and rejects in the passthrough,
`bootstrap.servers`, `security.protocol`, `client.id`, `acks` (always `all`),
`enable.idempotence` (always `true`), `retries` (`MAX_VALUE`),
`max.in.flight.requests.per.connection` (5, the idempotent bound), `delivery.timeout.ms`
(120s; kafkafim resends after it), the key and value serializers (byte arrays),
`transactional.id` (never set) and `interceptor.classes` (an interceptor could change DLQ
records), plus the same derived security keys as the consumer. A
DLQ record of an oversize source record can exceed `max.request.size`; raise it through
`properties` and the DLQ topic's `max.message.bytes` together.

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
        max-poll-records: 500
        max-in-flight-bytes: 64MB             # memory bound per pipeline; fetching pauses above it
        commit-interval: 1s
        max-record-bytes: 8MB                 # value is a JSON object or an array of objects
      document:
        id-field: /order_id                   # JSON pointer into each object; default /id
        id-missing: dlq                       # dlq (default) | auto (ES-generated id, duplicates on retry)
        version-field: /version               # optional → version_type external
        on-version-conflict: ignore           # ignore (default, counted as stale) | dlq
        timestamp-field: /@timestamp          # for %{+...} patterns; default: ingest time
        timestamp-format: iso-8601            # iso-8601 (default) | epoch-millis | epoch-seconds | pattern:<DateTimeFormatter>
        on-bad-timestamp: dlq                 # dlq (default) | ingest-time
        op: index                             # index | create
      routes:                                 # evaluated in order
        - name: eu                            # metric label; default: the zero-based position
          when:
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
        max-attempts: 10                      # per document, item-level retryable errors only
        max-elapsed: 10m                      # request-level failures (ES down) never count
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
top of user auth. Trust (CA) is per cluster. Production uses basic auth (or an API
key) with the cluster CA for trust only; client certificates stay modelled but are
not used in production and not exercised in the MVP tests.

- `routes[].identity`: the identity used for that route's bulk items → falls back to
  the cluster's `default-identity` → validation fails if the cluster uses `https`
  and none resolves; plain `http` with no identity binds with a startup warning (local
  development only). Whether the cluster actually enforces security is not knowable
  at bind time; a 401 at runtime is handled as a configuration failure (section 7).
- One low-level client (`Rest5Client`) per `(cluster, identity)`, since it fixes the
  `Authorization` header (`Basic` or `ApiKey`, sent preemptively) or client certificate
  at construction. It round-robins over the cluster's `uris`, skips nodes that just
  failed and tries the next node on a connection error or 502/503/504. With
  `ssl.ca-certificate` the cluster CA is the only trust anchor (hostnames are
  verified); without it the JVM default trust applies. `compression: true` gzips
  request bodies. Clients are shared by every pipeline using the same pair. Batching and bulk
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

`field` is a JSON pointer (`/a/b/0`). Comparison semantics:

- `equals`/`in`: a JSON string equals `v` when the text is identical; a JSON number
  equals `v` when `v` parses as a decimal of the same value (`1`, `1.0`, `1e0` are
  equal; a double compares by its shortest decimal form, so `0.1` equals `0.1`; NaN
  and infinities equal nothing); a JSON boolean equals `true`/`false` exactly. A
  string never equals a number (`"1"` in the document does not match `equals: 1.0`);
  objects, arrays, `null` and a missing field match nothing.
- `prefix`/`matches` apply to string fields only; any other type does not match. A
  value long enough to overflow `java.util.regex`'s stack (constructs like `(a|b)*`
  recurse per repetition) counts as no match instead of failing the record.
- `header` compares the UTF-8 bytes of `v` with the value of the **last** header of
  that name (Kafka allows repeats); a missing header or a null value does not match.

Route selection: in `route-mode: first` the first non-default route (in order) whose
conditions all hold, else the default route. In `all`, every non-default route whose
conditions hold, in order; the default route only when none did. No match and no
default (possible only in `all`) sends the document to the DLQ with
`error.type=no_route`.

**Drop routes** (ADR-0037). A route with `drop: true` discards the documents it selects:
they are neither indexed nor dead-lettered. `drop: true` takes the place of `cluster`,
`identity`, `index` and `pipeline`, and validation rejects a drop route that sets any of
them; `name`, `when` and `default` work as on any route.

```yaml
routes:
  - name: debug-logs
    when: [ { field: /level, equals: debug } ]
    drop: true
  - { default: true, cluster: logs-eu, index: "logs-%{+yyyy.MM.dd}" }
```

- In `route-mode: first`, order decides: a drop route after an indexing route that
  also matches never sees the document.
- In `route-mode: all`, a selected drop route **cancels every other copy**: the
  document goes nowhere, and it is counted under the first matching drop route.
- `default: true, drop: true` turns the routes into an allowlist: whatever no other
  route selected is dropped. A pipeline whose routes all drop is valid but logs a
  startup warning.
- A dropped document skips the id, version and timestamp checks (§4.2), so a drop
  route can discard documents that would otherwise be `missing_id` or
  `bad_timestamp`. Decode failures (invalid JSON, a scalar, a non-object element, an
  oversize record, a tombstone) are not documents a route can see, and still go to the
  DLQ.
- Each dropped document counts once in `kafkafim_docs_total{route,outcome="dropped"}`
  (§10). Nothing is logged per document.
- Preflight and the Elasticsearch auth check skip drop routes, which name no cluster;
  a pipeline whose routes all drop opens no bulk sink.
- Changing a drop route on reload restarts only its pipeline (§5.6) and applies to
  records not yet committed. Documents already in Elasticsearch stay.

A route's optional `name` is its `route` label in metrics (§10) and names it in log
and error messages; an unnamed route is labelled by its zero-based position (`0`, `1`,
…). So that no two labels of a pipeline collide, a name is not blank, is unique within
the pipeline, and is not reserved: `none` labels documents that failed before a route
was selected, and digits-only names are the positions of unnamed routes (so `1` would
merge with the unnamed second route; `01` is rejected too, for simplicity).

Index patterns use the Logstash syntax the team already knows: `%{field}` (a JSON
pointer or a top-level name, taken literally, so `%{a.b}` is the property `"a.b"`)
and `%{+yyyy.MM.dd}` (Java `DateTimeFormatter` pattern over `document.timestamp-field`,
or ingest time if unset, formatted in UTC with `Locale.ROOT`). A reference runs from
`%{` to the next `}`; everything else is literal, with no escapes. A string field
contributes its text, a number or boolean Jackson's text form (`42`, `1.5`,
`1.0e20`, `true`). Resolved index names are lower-cased with `Locale.ROOT`. A
pattern whose field is missing, `null`, an object or an array, or whose result
Elasticsearch would reject (forbidden character, leading `-`/`_`/`+`, `.`/`..`,
empty, an unpaired UTF-16 surrogate, more than 255 UTF-8 bytes), routes the
document to the DLQ with `error.type=unresolved_index` and the cause in
`error.reason`.

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
SPRING_PROFILES_ACTIVE=kafkafim
SPRING_CONFIG_ADDITIONAL_LOCATION=/etc/kafkafim/config/
SPRING_CONFIG_IMPORT=configtree:/etc/kafkafim/secrets/
```

`application-kafkafim.yaml` is a profile-specific file, so the `kafkafim` profile
must be active for Spring Boot to load it; activating it also switches off the
default `local` profile that carries the development sample. Two guards back this:
the chart's unit tests assert that every rendered `Deployment` sets
`SPRING_PROFILES_ACTIVE=kafkafim` and the non-optional config location, and the
application refuses to start with the `local` profile active (as the default or
explicitly) when it detects Kubernetes (`CloudPlatform.KUBERNETES`, which reads
`KUBERNETES_SERVICE_HOST`/`_PORT` and honours `spring.main.cloud-platform` for tests;
`config/LocalProfileGuard`, which runs before any bean is created).
Without the guard, a pod that lost its profile env would silently run the compose
sample with inline secrets allowed; with it, such a pod fails at startup with a message
naming the fix. As a second application-side check, the same guard refuses
`kafkafim.allow-inline-secrets=true` on Kubernetes in any profile (for example set by hand next to `kafkafim`): there, secrets
must come from mounted `Secret` files imported with `configtree` (§5.5).

Config location failure mode: the image's default is
`SPRING_CONFIG_ADDITIONAL_LOCATION=optional:/etc/kafkafim/config/` so that local runs
and the CI smoke test start without a mount; the chart overrides it with the
non-optional form, so a pod whose ConfigMap mount is missing fails at startup with
`ConfigDataLocationNotFoundException` instead of running without pipelines.

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
`api-key` and `*-password` fields (and in passthrough `properties` keys ending in
`password`, containing `secret`, `sasl.jaas.config`, `ssl.keystore.key` or
`user.info`) unless `kafkafim.allow-inline-secrets=true`. That switch is for local
development outside Kubernetes (`./mvnw spring-boot:run` with Compose): on
`CloudPlatform.KUBERNETES` the flag itself stops startup (§5.4), so the local Kubernetes
demo (`deploy/local/`, §13) mounts its secrets as files, like production. A
secret is accepted when it is read straight from a `configtree` file, or when its raw,
unresolved text is exactly one `${name}` placeholder (no `:default`, which would be an
inline fallback) whose chain of references ends in a `configtree` file. The check looks
at the property source that wins for each name, before placeholder resolution, and
reports the origin but never the value; likewise the bound configuration records print
every secret (passwords, API keys, keystore and key passwords, secret passthrough
`properties` values) as `******` in `toString()`, while usernames stay readable. A
literal in YAML, an environment variable or a system property is rejected, also when
reached through a placeholder. A credential
placeholder (secret or username) that nothing defines always fails startup, even with
inline secrets allowed: Spring leaves unresolvable placeholders as literal text, so a
missing secret file would otherwise become the password. A value with a placeholder embedded in
other text (for example a `sasl.jaas.config` string containing `password="${...}"`)
counts as a literal, so such a value is mounted whole as one secret file. Secret provisioning stays outside the chart
(`existingSecret` names): plain Secrets, Sealed Secrets, External Secrets or a Vault
agent all work. A changed secret file is picked up by the same reload path. No Bean
Validation constraint sits on a secret component (a reflection test enforces it): Boot's
bind-failure report prints the raw `Value:` of a failed constraint, so secret rules live
in `ConfigValidator`, whose messages never include values.

### 5.6 Validation and hot reload

- **Startup**: binding failure or validation failure stops the pod with a clear
  message that lists every violation at once (Bean Validation and the rules of section
  5.7 run in the same pass), each as `<property path>: <problem>`; readiness never goes
  green. The `local` profile active, or `kafkafim.allow-inline-secrets=true` set, on
  `CloudPlatform.KUBERNETES` stops startup before binding (section 5.4).
- **`validate` profile** (application feature, roadmap 0.2): started with
  `--spring.profiles.active=validate,kafkafim` and a rendered ConfigMap mounted, the
  application binds and validates, logs the pipeline summary, and exits 0/1 without
  connecting anywhere. It is meant for the infrastructure repository's CI, where the
  per-cluster values live. This repository's CI does not use it; it builds the image
  and smoke-tests it with a liveness probe ([DELIVERY.md](DELIVERY.md)).
- **Reload** (ADR-0028): a watcher on the config and secret directories (Java
  `WatchService` + 10s hash poll, because kubelet updates volumes by symlink swap with
  up to ~1 min delay) plus `POST /actuator/reload`. The new tree is loaded with
  Spring's `Binder` into a fresh model and validated as a whole. On success the
  supervisor diffs by pipeline: changed pipelines are drained (stop fetching, flush
  in-flight, commit, leave group) and restarted; added start; removed drain and stop;
  unchanged are untouched. Cluster definition changes restart every pipeline using
  that cluster; an identity change restarts only the pipelines referencing it. An
  invalid tree is rejected whole; the running config stays, a metric and log record
  the failure. In detail:
  - **What is re-read.** A new Spring environment receives every property source of
    the running one that did not come from config data, in the same order
    (command-line arguments, `SPRING_APPLICATION_JSON`, system properties, environment
    variables, random values, `SpringApplication` default properties, test
    properties); then Spring Boot's own config data processing
    (`ConfigDataEnvironmentPostProcessor`) runs on it, with the running application's
    active profiles pinned. It reads `spring.config.location`,
    `spring.config.additional-location` and `spring.config.import` from those sources
    and loads, from disk, exactly what startup loaded: `application.yaml`, the
    profile-specific files (`application-kafkafim.yaml` from the ConfigMap mount) and
    every import (the `configtree:` secret directories, file by file). Precedence is
    therefore startup's, highest first: command-line arguments, system properties,
    environment variables, profile-specific files outside the jar, plain files outside
    the jar, profile-specific files in the jar, `application.yaml` in the jar,
    `SpringApplication` defaults; the `configtree` imports land where startup put them
    (their names, `kafka.*` and `elasticsearch.*`, never overlap `kafkafim.*`).
    Environment variables and system properties cannot change in a running process,
    so a reload only sees file changes.
  - **Validation**: the Kubernetes guard (§5.4: `kafkafim.allow-inline-secrets=true`
    on Kubernetes), unknown keys under `kafkafim`, Bean Validation, the rules of §5.7
    and the inline-secret check of §5.5 against the fresh property sources, all in
    one pass (`config/ConfigLoader`). A secret file that disappeared leaves its
    placeholder unresolved, which rejects the tree.
  - **Diff** (`config/ConfigDiff`): a pipeline restarts when its own definition
    changed (including its resolved group id and identities, so a new
    `default-identity` restarts the pipelines that inherit it), when a cluster it
    uses changed in anything but its identities and `default-identity` (its source
    Kafka cluster serves both the consumer and the DLQ producer; every route's
    Elasticsearch cluster, including its `client` timeouts), or when an identity it
    references changed (source identity, DLQ identity, every route's identity).
    Secret values are part of the bound identities, so a rotated secret file is an
    identity change. A change to a cluster no pipeline uses restarts nothing.
  - **Building blocks** (`supervisor/PipelineResources`): one Elasticsearch registry
    (clients per `(cluster, identity)`, one breaker) and one set of DLQ producers and
    consumer factories per *cluster definition*. A restarted pipeline gets the current
    instance of each cluster it uses, so a pipeline restarted for its own change keeps
    its clients, producer and DLQ writer as long as they are still the current ones (an
    earlier reload that added a pipeline on its Kafka cluster made new DLQ producers
    current, and the restart then moves it to them); a changed cluster or identity makes a new
    instance current; a pipeline that is new to the current DLQ producers, or whose
    DLQ settings changed, also gets new DLQ producers (they cache each pipeline's
    writer with its topic). An instance closes when its last pipeline stopped.
    Meanwhile old and new instances coexist: briefly two producers or clients for the
    same `(cluster, identity)`, and a breaker per instance; each pipeline follows only
    the breaker and DLQ writer it uses, and `dlq-blocked` is dropped when a restart
    gives the pipeline a new writer. When a pipeline leaves an instance that other
    pipelines still use (it moved to a new one, or was removed), its DLQ writer there is
    evicted (ADR-0035): its unacknowledged sends complete with `DlqClosedException` and
    stop retrying, and their offsets stay uncommitted (re-delivered if the pipeline runs
    again).
  - **One reload at a time**: watcher and endpoint share one lock; a second caller
    waits. The watcher reloads once the content hash has been stable for the debounce
    (2s), so a kubelet update of several files is read once, and it does not retry a
    rejected tree until the files change again. A reload waits for the drains it
    causes (up to about 50s per pipeline, in parallel; drains are never cut short);
    a SIGTERM during a reload waits for it before stopping the pipelines, starts
    nothing the reload would have started, and may then exceed the 60s shutdown
    phase, after which Spring moves on: records not yet committed are re-delivered
    after the restart, nothing is lost.
  - **`POST /actuator/reload`** returns `{result, generation, added, removed,
    restarted: [{pipeline, reasons}], retried, unchanged, errors}`: 200 for `applied` or
    `unchanged`, 400 with every violation for `rejected`, 503 while the supervisor is
    not running. Names and violations only, never configuration values. It is
    defined in `supervisor` (nothing depends on `supervisor`, so it cannot create a
    `telemetry` ↔ `supervisor` cycle).
  - **Metrics and logs**: `kafkafim_config_reload_total{result=success|failure}`
    (`unchanged` counts as success) and `kafkafim_config_generation` (1 at startup,
    +1 per applied change); one INFO line per applied reload with the plan, one ERROR
    line per rejected reload listing every violation. `kafkafim_pipeline_state` gains
    and loses series with added and removed pipelines.
  - **Settings**: `kafkafim.reload.enabled` (default `true`), `poll-interval` (10s),
    `debounce` (2s), read at startup only. The watched paths are the file and
    `configtree:` locations of the three properties above; `classpath:` locations are
    not watched. `POST /actuator/reload` works with the watcher disabled.
  - Not reloaded: the `KafkafimProperties` bean stays the startup tree (the supervisor
    holds the one in effect); OTLP export headers (§10) and everything outside
    `kafkafim.*` need a restart.
  - **Retry of `failed_preflight`** (ADR-0032): every accepted reload (`applied` or
    `unchanged`, watcher or endpoint) starts once more each pipeline it leaves unchanged
    that is in `failed_preflight`, so after fixing a missing topic, ACL or credential a
    no-op `POST /actuator/reload` recovers it without a restart. They are listed under
    `retried` (not `unchanged`); `result`, `generation` and
    `kafkafim_config_reload_total` are unaffected, and the reload's INFO line names
    them. One ordinary start per reload, in the background: a pipeline that still fails
    goes back to `failed_preflight` and waits for the next reload (no automatic retry,
    no attempt while one is starting or stopping); the outcome shows in
    `kafkafim_pipeline_state`. A rejected reload retries nothing.

### 5.7 Defaults and validation rules

Defaults not shown inline in 5.1/5.2: Kafka `security-protocol: PLAINTEXT`; ES
`bulk` 5000 docs / 5MB / 200ms / 4 in flight, `compression: true`, `client`
`connect-timeout: 5s` / `response-timeout: 60s` (both must be positive); `source`
`offset-reset: latest`, `max-poll-records: 500`, `max-in-flight-bytes: 64MB`,
`commit-interval: 1s`, `max-record-bytes: 8MB`; `dlq` `max-attempts: 10`,
`max-elapsed: 10m`; `route-mode: first`; `latency.max-age` unset (the cluster's
`bulk.max-age` applies).

Every rule below is checked on every bind (`config/ConfigValidator`), and all violations
are reported together:

- **Names** of clusters, identities and pipelines are lower-case kebab-case
  (`[a-z0-9-]`, no leading/trailing `-`): they appear in group ids, metric labels and
  configtree paths (`kafka/<cluster>.<identity>/password`).
- **References**: every `cluster` and `identity` (including `default-identity`) names an
  existing entry; identity names are unique per cluster (duplicate YAML keys fail). An
  invalid `default-identity` is reported once, on the cluster, not again on every
  pipeline or route that would have used it.
- **Kafka**: `bootstrap-servers` are `host:port` (IPv6 in brackets; an optional
  `PROTOCOL://` prefix is tolerated, as kafka-clients does). `SASL_PLAINTEXT`/`SASL_SSL` require a
  resolved identity with a `sasl` block for the consumer and the DLQ producer; `SSL` and
  `PLAINTEXT` need none (client certificates are the broker's choice). A `sasl` block is
  only allowed on a `SASL_*` cluster, a keystore and cluster `ssl` trust only on a TLS
  one. `PLAIN`/`SCRAM-*` need `username` and `password`.
- **Elasticsearch**: `uris` are `http(s)://host[:port]`, one scheme per cluster;
  `ssl` trust and keystores only with `https`. An identity is exactly one of
  `username`+`password` or `api-key`, optionally with a keystore, or a keystore alone.
  A route on an `https` cluster without a resolvable identity fails; on `http` it binds
  with a startup warning.
- **Pipelines**: `topics` are non-empty and distinct; the DLQ topic is not one of the
  source topics; `max-record-bytes ≤ max-in-flight-bytes`; sizes and durations are
  positive. A consumer group belongs to exactly one pipeline: two pipelines with the same
  `(source.cluster, group-id)` are rejected (the same topic in two groups is fine).
- **Routes**: in `route-mode: first` exactly one `default: true` route, and every other
  route has at least one `when` condition (an unconditional route would hide the rest);
  in `all` at most one default, and unconditional routes are allowed (fan-out). The
  default route is last and has no conditions. A route `name`, when set, is not blank,
  is unique within the pipeline, and is neither `none` nor digits-only (both are
  reserved `route` labels, §5.3). A route's ingest `pipeline` is optional
  but, when set, not blank: `pipeline: ""`, whitespace, or a bare `pipeline:` (which binds
  as an empty string) would make Elasticsearch reject the whole bulk request; omit the key
  instead. A route needs `cluster` and `index` unless it has `drop: true`, which takes
  none of `cluster`, `identity`, `index` or `pipeline` (§5.3).
- **Conditions**: exactly one subject (`field`, `header` or `topic`). A `field` takes
  exactly one of `equals`, `in` (non-empty), `exists`, `prefix`, `matches` (the regex
  must compile); a `header` takes `equals`; a `topic` takes nothing and must be one of
  the pipeline's source topics. Every `field` and document pointer is an RFC 6901 JSON
  pointer starting with `/`.
- **Index patterns**: `%{field}` (JSON pointer or top-level name) and
  `%{+<DateTimeFormatter>}` must be terminated and parse; literal parts, and what each
  date pattern produces (so `%{+yyyy/MM}` or `%{+HH:mm}` fail), must not contain
  characters Elasticsearch rejects (`\ / * ? " < > | , # :` and space); a name must not
  start with `-`, `_` or `+`, nor be `.` or `..`. A name starting with `.` (a literal, or
  a leading `%{+...}` whose output does) is a warning: Elasticsearch reserves those for
  hidden and system indices and deprecates creating them; a leading `%{field}` cannot be
  known at bind time and is not checked. Date patterns are checked, and resolved, with
  `Locale.ROOT`. An unquoted `Y` (week-based year) in a
  date pattern is a warning.
- **Timestamps**: `timestamp-format` is `iso-8601`, `epoch-millis`, `epoch-seconds` or
  `pattern:<DateTimeFormatter>`. A pattern is checked with the formatter decode uses
  (`Locale.ROOT`, strict, era CE) by formatting a sample and parsing it back: one that
  never yields a date (`yyyy-MM`, `HH:mm`, `YYYY-MM-dd`) or a complete time (`hh`
  without `a`, minutes without hours) is an error, since every document would fail, as
  is a day period `B` without a time (it would read every value as midnight). A
  pattern without an offset or zone (read as UTC; a quoted `'Z'` literal does not warn),
  one using `k`, or one that cannot format and parse back the sample is a warning.
- **Document**: `op: create` takes no `version-field`; Elasticsearch accepts external
  versions only on `index` ("create operations only support internal versioning", §4.4).
- **Warnings** (bind, log at `WARN`): no pipelines, `id-missing: auto`, an http route
  without identity, an index name starting with `.`, week-based year, a timestamp
  `pattern:` without an offset or zone, using `k` or failing its self-check,
  `allow-inline-secrets=true`, a pipeline whose routes all have `drop: true`.

## 6. Scaling

- **Static**: `replicas` per group in `values.yaml`, capped by the smallest partition
  count in the group (startup warning).
- **HPA on CPU** (core Kubernetes): CPU tracks throughput closely for JSON decode and
  gzip. Optional per group.
- **Lag-based**: needs KEDA (a CRD) or a custom-metrics adapter, not available in
  production. If it ever is, it scales on the consumer-lag metrics of the Kafka team's
  monitoring for the group's consumer groups; kafkafim exports no lag (ADR-0034).
- Scaling beyond partitions is wasted; repartitioning is a Kafka-side action.
- Per-cluster knobs: `bulk.max-docs/max-bytes/max-age/in-flight`,
  `client.connect-timeout/response-timeout`; per pipeline
  `source.max-poll-records`, `source.max-in-flight-bytes` (memory bound), both shown
  in section 5.2.

## 7. Backpressure and failure handling

| Situation | Behaviour |
|---|---|
| ES returns 429 / 5xx / connection error or timeout for a bulk request | Retried with the same documents **forever**, exponential backoff with jitter (100ms doubling to 30s, ±50%); counts as a breaker failure. The pipeline's container is paused when in-flight bytes (key + value of every delivered, incomplete record) reach `max-in-flight-bytes`, and resumes once they drop to half of it. A poll that crosses the budget is delivered whole, so the overshoot is at most one poll per consumer. Kafka lag grows (visible in the Kafka team's consumer-lag monitoring). No local buffering. Never DLQ. |
| Repeated cluster-level failures | Circuit breaker per ES cluster (Resilience4j, shared by all identities and pipelines): count window of 20 requests, opens at ≥50% failures after at least 10, stays open 10s, then lets 2 probes through. While it is open every sender for that cluster waits; pipelines routing to that cluster pause (supervisor reason `es-breaker:<cluster>`, from open until the breaker is closed again; half-open keeps the pause), others proceed. |
| Bulk request rejected with 401 / 403 | Configuration failure, not data failure: retried with backoff forever and reported through the sink listener (the supervisor pauses the pipeline; `kafkafim_auth_failures_total` and an alert fire). Not a breaker failure. Nothing goes to the DLQ. The log line and the listener event keep the security error's type and `reason` (user, roles, action, index), never the raw body (ADR-0030). An **item** with 401/403 (the identity may not write that index) is handled the same way. |
| Bulk request with any other unexpected status (a 400 for the whole request, a 404 from a proxy or a wrong path, a 3xx) or a 2xx body that is not a bulk response | Retried forever with backoff and logged with Elasticsearch's `error.type` and `root_cause[].type`s, never its `reason`, which can quote field values (ADR-0030; a body that is not JSON is logged by its size, an unparsable 2xx body by what was wrong with it; the body itself only at DEBUG on the `BulkSink` logger): it cannot be attributed to one document, so it never reaches the DLQ. An unexpected status is not a cluster-health signal and does not open the breaker; an unparsable 2xx body does. |
| An action Elasticsearch would reject for the whole request (`op: create` with an external version, an empty ingest `pipeline` name, an `_id` outside 1 to 512 UTF-8 bytes) | Rejected when the action is built (`BulkAction`), because a whole-request 400 could only be retried forever. Config validation (§5.7) therefore rejects `op: create` together with `document.version-field` (ES answers "create operations only support internal versioning") and a blank route ingest `pipeline` (`""`, whitespace, or a bare `pipeline:`). |
| Bulk partial failure | Only failed items are retried; successful items are acked immediately. |
| Non-retryable item error: the 400 family (`mapper_parsing_exception`, `document_parsing_exception`, `strict_dynamic_mapping_exception`, `illegal_argument_exception` such as a missing ingest pipeline) and any other 4xx item not listed here | DLQ immediately with `error.type=es_error`, partition continues. |
| Item 409 `version_conflict_engine_exception` | With an external version: `stale` (success) under `on-version-conflict: ignore`, DLQ `version_conflict` under `dlq`. For `op: create` without a version: an earlier attempt landed, so success. Otherwise `es_error`. |
| Retryable **item** error (item 429 `es_rejected_execution_exception`, item 5xx, item 404 `index_not_found_exception`, any other unexpected item status) exceeding `max-attempts`/`max-elapsed` | The item goes back into the batcher after a backoff and travels in a later bulk; once it has failed `max-attempts` times, or `max-elapsed` after its first retryable failure, DLQ with `error.type=es_retries_exhausted`, partition continues. This is the poison-message bound. A missing index is a provisioning gap (ADR-0021) or a race with whoever creates the index, so it gets the bounded retry rather than an immediate DLQ or an unbounded stall. Request-level failures (connection errors, 5xx, request-level 429, breaker open, 401/403, unexpected statuses) are retried indefinitely and never count toward the bound; an ES outage produces lag, never DLQ records. |
| Bulk request rejected with 413 | The request is split in halves and each half retried (recursively); a single-document bulk that still gets 413 sends that document to the DLQ with `error.type=too_large`. |
| Undecodable record, non-object element, unusable version, missing `_id`, bad timestamp, no matching route, unresolved index pattern | DLQ with the matching `error.type` (`decode`, `missing_id`, `bad_timestamp`, `no_route`, `unresolved_index`). A document a drop route selected skips the version, `_id` and timestamp checks and is never dead-lettered for them (§5.3); undecodable records and non-object elements still are. |
| Missing topic, DLQ topic, or failed authentication at pipeline start | Pipeline stays stopped (`failed_preflight`) until the next accepted reload or a restart (§5.6), clear log naming resource and identity, alert. Other pipelines unaffected. kafkafim never creates the resource. |
| DLQ produce fails | Block the partition and alert. Losing data is worse than lag. The send is retried forever (100ms doubling to 30s, ±25% jitter) and never completes the source offset until the broker acknowledged it (`acks=all`), so the committed offset stays put. The writer is *blocked* from its first failure until every failed send was acknowledged; the supervisor pauses the pipeline on `onBlocked` and resumes on `onUnblocked`. A missing DLQ topic, failed authentication or authorization, an invalid topic or a record larger than the producer or topic allows is a **configuration** failure (logged at `ERROR`, alert); anything else is **transient**. Both kinds are retried; neither drops the record. A producer error that kafka-clients can make fatal even for an idempotent producer (cluster authorization, authentication, unsupported version, a fenced producer or an invalid producer ID mapping) replaces the shared producer, so fixing the cause unblocks the next attempt without a restart. A missing `Write` ACL is usually not fatal, though: in kafka-clients 4.2 a `CLUSTER_AUTHORIZATION_FAILED` answer to `InitProducerId` is *abortable*: the producer fails the pending batches, goes back to uninitialized and retries `InitProducerId` on its own. The producer becomes fatal only when the error reaches its error state through a failed batch (a produce response carrying it). kafkafim replaces the producer on `ClusterAuthorizationException` either way: replacing it is cheap and covers the fatal path. On the abortable path kafka-clients still logs `Aborting producer batches due to fatal error` at `ERROR`, and kafkafim logs that the producer *may be* in a fatal state and replaces it; both mean only that the pending sends failed and will be retried. A send that fails only because of the replacement (closing the producer aborts its batch, or it reached the producer after the close began) is reported by the failure that triggered the replacement, so a missing ACL never shows up as a transient `Producer is closed forcefully`. Each send attempt is reported once, although kafka-clients can both throw from `send` and later fail the batch it had already appended. |
| Kafka unavailable | Reconnect with backoff (kafka-clients); `kafkafim_kafka_connected` gauge drops, alert. A Kafka authentication or authorization failure after start is retried every 10s instead of stopping the consumer. |
| Pod SIGTERM | Spring graceful shutdown, higher phases first: the pipeline supervisor (phase MAX-100) stops all pipelines in parallel; each pipeline's Kafka source pauses fetching, gives in-flight records up to 30s to complete (its bulk sinks and the DLQ writer are still open, so outcomes keep arriving), then stops its container (10s), which commits what completed on revoke and leaves the group; records still incomplete are re-delivered to the next owner; then the pipeline's bulk sinks close (up to 10s; outcomes after the revoke are ignored); then the DLQ producers (phase MAX-1000, so they outlive the source drain; closed by `supervisor/PipelineResources`, which owns every configuration generation's producers) flush for up to 10s and close, failing any send still unacknowledged so that its record is not committed; then the web server drains (phase MAX-1024, instant for actuator-only traffic). `spring.lifecycle.timeout-per-shutdown-phase` (60s, `application.yaml`) bounds each phase; the consumer phase must fit the pod's `terminationGracePeriodSeconds` (90s, chart default). |

Error classification lives in one table (`sink/es/ErrorClassifier`) and is
unit-tested against captured ES 8.19 responses (`src/test/resources/sink/es/`), because
it decides data loss versus stall. The retry loop around it is the sink's own
(ADR-0026); Resilience4j provides the breaker and the backoff intervals.

**HTTP client settings** (per `(cluster, identity)` client; ADR-0031): the cluster's
`client.connect-timeout` (default 5s) and `client.response-timeout` (socket inactivity,
default 60s), so a `max-bytes` bulk must be answered within it, and a timeout is a
request-level retry. The pool is a code default: 30s to lease a pooled connection; 32
connections per node, 128 per client. The breaker's half-open safety net (a half-open
breaker whose probes never report back reopens) is its open time plus twice the
response timeout. Bulk requests carry
`filter_path=errors,items.*.status,items.*.error`, so a successful response is a few
bytes per item and parsing stops at `"errors": false`.

**Ordering.** Retries can reorder writes of the same `_id` (an item retried after a
later write of the same id succeeded, or two bulks in flight). Pipelines that need
last-writer-wins use `document.version-field` (external versions).

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
- headers: `kafkafim.pipeline`, `kafkafim.error.type` (`decode`, `missing_id`,
  `bad_timestamp`, `no_route`, `unresolved_index`, `es_error`, `es_retries_exhausted`,
  `version_conflict`, `too_large`; section 7), `kafkafim.error.reason`
  (ES item error JSON, truncated to 4 KiB), `kafkafim.source.topic`,
  `kafkafim.source.partition`, `kafkafim.source.offset`, `kafkafim.target.cluster`,
  `kafkafim.target.index`, `kafkafim.attempts`, `kafkafim.first-failure-ts`,
  `kafkafim.doc-index` (position inside a JSON array record), `kafkafim.source.timestamp`;
  original headers kept except incoming `kafkafim.*` ones (ADR-0027).

Record details (`dlq/DlqRecordFormat`):

- A `null` key or value (tombstone) stays `null`. Header values are UTF-8: numbers in
  decimal, instants ISO-8601 UTC (`2026-11-08T10:15:30.123Z`). A header without a value
  (no target, no `doc-index`, no reason) is omitted, not sent empty.
- `kafkafim.error.reason` is cut to 4096 UTF-8 bytes on a code-point boundary; a cut ES
  error JSON is no longer valid JSON.
- Original headers come first, in their order, duplicates included, **except** those in
  the reserved `kafkafim.` namespace (case-insensitive). kafkafim's headers follow, so a
  record replayed from a DLQ topic and dead-lettered again carries exactly one, current
  set: a stale `kafkafim.doc-index` of an earlier failure never survives. The earlier
  failure stays readable at the coordinates the new `kafkafim.source.*` headers name.
- The record timestamp is the send time, not the original: with the original, retention
  would delete a record dead-lettered after a long backlog as soon as it lands, and a
  broker rejects timestamps outside its `message.timestamp.*` window, which would block
  the partition for a value kafkafim cannot change. The original timestamp travels in
  `kafkafim.source.timestamp` (omitted when the record had none). Because ingest time is
  the Kafka record timestamp (§4.5), a pipeline consuming a DLQ topic uses
  `kafkafim.source.timestamp` as the ingest time when `document.timestamp-field` is
  unset, so replayed documents land in the original date's index (ADR-0027; the
  supervisor reads it, §4.2). Records without the header, or with one that is not an
  ISO-8601 instant, keep the Kafka record timestamp.
- No partition is set: the default partitioner spreads by key.

Writer contract (`dlq/DlqWriter`): `send` never blocks and returns a future that completes
only after the broker acknowledged the DLQ record; the caller completes the source record
(or one part of a multi-document record, ADR-0025) there and nowhere else. Failures are
retried forever and reported to a `DlqListener` (§7), never as an exceptional completion;
the future fails only with `DlqClosedException` at shutdown, leaving the offset
uncommitted. Before the first record to a topic the writer describes it
(`Describe`, already required): the producer always lets the broker auto-create a topic
it sends to, so producing blind would create a missing DLQ topic on a broker with
`auto.create.topics.enable=true` (ADR-0021) and would otherwise only surface as a metadata
timeout. A topic once seen is only checked again after a send timeout, so a DLQ topic
deleted while kafkafim runs can still be re-created by the producer's metadata refresh on
such a broker; brokers should keep auto-creation off, and the writer identity never has
`Create` (ADR-0021). A resend after `delivery.timeout.ms` can duplicate a DLQ record and resends are
not ordered; both are harmless, since replay is idempotent by `_id`.

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

Remove it when its consumer group's lag (`source.group-id`, in the Kafka team's
consumer-lag monitoring; kafkafim exports none, ADR-0034) reaches zero. Inspection
uses standard Kafka tooling (`kafka-console-consumer` with headers, Kafka UI).
Purge is retention. A dedicated DLQ tool is a post-MVP item.

Metrics: `kafkafim_dlq_records_total{pipeline,error_type}`; the replay pipeline's
consumer-group lag on the DLQ topic, in the Kafka team's monitoring, is the
unresolved-backlog number.

## 10. Observability

Spring Boot's OpenTelemetry starter: Micrometer → OTLP (APM Server) plus
`/actuator/prometheus`; Micrometer Tracing with the OTel bridge → OTLP. APM Server
(or later an OpenTelemetry Collector) pre-exists in production; switching between
them is only the `OTEL_EXPORTER_OTLP_ENDPOINT` value in the chart. Export headers reach
both exporters: non-secret ones through `OTEL_EXPORTER_OTLP_HEADERS` (mapped by Spring
Boot), secret ones (the APM Server token) as `configtree` files the chart mounts at the
exporters' `headers` property paths (`otel.secretHeaders`, [HELM.md](HELM.md)). Header
secrets are read at startup; rotating one needs a pod restart.

### Metrics (all with `pipeline`, `kafka_cluster`; `topic`/`es_cluster`/`route` where it applies)

Exact label sets (`telemetry/TelemetryNames`, `telemetry/PipelineMeters`):

| Series | Labels besides `pipeline`, `kafka_cluster` | Fed by |
|---|---|---|
| `kafkafim_records_consumed_total`, `kafkafim_bytes_consumed_total` (key + value) | `topic` | every record the source delivers |
| `kafkafim_docs_total` | `route`, `outcome=indexed\|stale\|dlq\|dropped` | one per routed copy or failed document, when it is done (ES ack, or DLQ record acknowledged), and one per document a drop route discarded (`dropped`, under the drop route's label; §5.3) |
| `kafkafim_bulk_requests_total` | `es_cluster`, `status` (HTTP code, or `io_error` without a response) | every bulk request attempt |
| `kafkafim_bulk_latency_seconds`, `kafkafim_bulk_docs`, `kafkafim_bulk_bytes` (histograms) | `es_cluster` | every answered bulk request |
| `kafkafim_item_errors_total` | `es_cluster`, `error_type` (Elasticsearch error type, `status_<n>` without one), `retryable` | every failed item: retried, or dead-lettered as `es_error` / `version_conflict` |
| `kafkafim_retries_total` | `es_cluster`, `scope=request\|item` | every retryable failure of a whole request, and of an item (an item whose poison bound is reached also counts its last failure) |
| `kafkafim_auth_failures_total` | `es_cluster`, `identity` (`none` for an unauthenticated cluster) | every response carrying a 401/403, for the request or for any item (one per response, not per item) |
| `kafkafim_batch_flush_total` | `es_cluster`, `reason=docs\|bytes\|age\|explicit` | every sealed batch (`explicit`: revoke, stop) |
| `kafkafim_e2e_latency_seconds` (histogram) | — | every Elasticsearch ack: ack time minus the Kafka record timestamp (DLQ'd documents are not counted) |
| `kafkafim_in_flight_bytes`, `kafkafim_assigned_partitions`, `kafkafim_kafka_connected` (1/0) | — | the pipeline's running source, read on scrape |
| `kafkafim_dlq_records_total` | `error_type` | every DLQ record the broker acknowledged |
| `kafkafim_breaker_state` | only `es_cluster`: 0 closed, 1 half-open, 2 open | the breakers the pipelines send through (the worst one while a reload leaves two generations alive) |
| `kafkafim_pipeline_state` | only `pipeline`, `state=starting\|running\|paused\|failed_preflight\|stopped` | 1 for the current state, 0 for the others; `paused` means a supervisor reason: breaker open, identity rejected or DLQ blocked, not the byte-budget backpressure |
| `kafkafim_config_reload_total{result=success\|failure}`, `kafkafim_config_generation` | none | §5.6 (1 at startup, +1 per applied reload) |

Label discipline: `route` is the route's `name`, or its zero-based position, never the
resolved index (daily indices would explode cardinality); `route="none"` counts
documents that failed before a route was selected (`decode`, `missing_id`,
`bad_timestamp`, `no_route`). Validation reserves `none` and digits-only names and
rejects duplicate names (§5.3), so each `route` value means exactly one route or the
pre-routing failures. There are no partition labels. Series known from the
configuration (per topic, per route and outcome, the bulk histograms per cluster, the
gauges) exist from startup with 0; status and error-type series appear with their first
event. On a reload (§5.6, ADR-0028) a removed pipeline loses every series, an added one
gains them, a pipeline whose labels changed (source cluster, topics, routes, clusters)
gets fresh series, and an unchanged one keeps its counters across a restart.

Consumer lag is not a kafkafim metric (ADR-0034): the Kafka team's consumer-lag
monitoring already shows and alerts on it per consumer group (`source.group-id`), and
reading it here would cost one extra consumer per pipeline and two broker requests per
pipeline every 10s. kafkafim's own "are we keeping up" signals are
`kafkafim_e2e_latency_seconds` and the throughput counters, read next to
`kafkafim_pipeline_state` and `kafkafim_breaker_state`.

`kafkafim_kafka_connected` is 1 while every consumer of the pipeline holds a broker
connection (kafka-clients' `connection-count`); a stopped pipeline reads 0, as do its
in-flight bytes and assigned partitions.

### Traces

One `pipeline.batch` span per sealed bulk batch (kind consumer, from sealing to the end
of its last request; attributes `kafkafim.pipeline`, `kafkafim.kafka_cluster`,
`kafkafim.es_cluster`, `kafkafim.es_identity`, `kafkafim.flush_reason`,
`kafkafim.docs`, `kafkafim.bytes`, `kafkafim.requests`) with one `es.bulk` child per
bulk request, retries and 413 halves included (kind client; `kafkafim.es_cluster`,
`kafkafim.docs`, `kafkafim.bytes`, `http.response.status_code` when answered,
`kafkafim.retries` = failed attempts of that request before it, `kafkafim.item_errors`).
A failure sets `error.type` (`io_error`, `malformed_response`, the HTTP status, or
`item_errors`) and the span status. Incoming `traceparent` Kafka headers of the batch's
records are attached as **span links** (distinct, at most 128), not parents. Head
sampling: 1% of batches (`management.tracing.sampling.probability`); a failed batch is
always kept (`telemetry/BatchSampler`), retryable item errors such as a 429 included, so
trace volume rises while Elasticsearch pushes back. Spans are recorded when the batch ends, with
their real timestamps, which is what lets the sampler see the failure (ADR-0029); a
batch still retrying has no span yet. No per-document spans; no trace context is sent
to Elasticsearch. Dropped documents (§5.3) never join a batch, so they appear in no span;
`kafkafim_docs_total{outcome="dropped"}` counts them (ADR-0037).

### Logs

ECS-structured JSON on stdout (Spring Boot structured logging); OTLP log export
optional. One WARN line per bulk response with failed items, naming the first 5 by
position, status and error type (not Elasticsearch's reason, which quotes field values
of the document; it travels in the DLQ record), written before that response's outcomes;
one WARN line per request-level retry (429, 5xx, 401/403, unexpected status) with the
status and Elasticsearch's error types, plus the security `reason` for 401/403, or, for
an unparsable 2xx body, what was wrong with it; never other response text, which can
quote field values (the response body, cut to 1024 characters, only at DEBUG on the
`io.github.matabar.kafkafim.sink.es.BulkSink` logger, ADR-0030); one WARN line per DLQ
record (pipeline, source coordinates, error type, attempts, target, where the DLQ
record landed; never the reason or the record);
one INFO or ERROR line per reload (§5.6). Document bodies are never
logged by default.

### Health

- `/actuator/health/liveness`: `livenessState` and `supervisor` (up while the pipeline
  supervisor runs, and while a graceful shutdown drains it). Nothing about pipelines,
  Kafka or Elasticsearch: a restart would not fix them.
- `/actuator/health/readiness`: `readinessState` (config bound and valid: an invalid
  configuration never finishes startup), `pipelines` (§4.6) and `kafka` (Kafka clients
  connected: up while at least one started pipeline's consumers hold broker
  connections, or while none is started; out of service when every started pipeline
  lost its brokers). Deliberately **not** gated on ES health.

### Shipped with the repo

[`deploy/grafana/`](../deploy/grafana/README.md), written against the
`/actuator/prometheus` names above:

- `dashboards/kafkafim-overview.json`: per pipeline: state, throughput, dropped
  documents per drop route, e2e latency, DLQ rate, in-flight bytes and heap, assigned
  partitions, Kafka connectivity, config generation and reloads.
- `dashboards/kafkafim-elasticsearch.json`: per ES cluster: breaker, bulk requests by
  status and the 429 ratio, bulk latency, docs and bytes per request, flush reasons,
  retries, item errors, auth failures.
- `alerts/kafkafim-alerts.yaml` (Prometheus rules, with `promtool test rules` tests):
  DLQ rate > 0 for 10m, bulk reject (429) ratio > 1%, e2e p99 above
  target (10s by default), breaker open, auth failures > 0, reload failed, pod with 0
  assigned partitions for a pipeline it runs; plus `failed_preflight`, `paused` for
  5m, `starting` for 10m and Kafka disconnected for a running pipeline.

The compose demo provisions both dashboards into its Grafana LGTM.

## 11. Security

- Secrets only via mounted `Secret`s imported as `configtree`; inline secret values
  fail validation outside local development. This includes the OTLP export token
  (`otel.secretHeaders`, §10).
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
  batch listener, cooperative-sticky, pause/resume; kafkafim's own out-of-order offset
  tracker (ADR-0025).
- **Elasticsearch low-level REST client** (ADR-0005): HTTP only, version-agnostic
  across the 8.x fleet; we build `_bulk` bodies and parse per-item results.
- **Resilience4j** core for the breaker and backoff intervals (ADR-0016, ADR-0026).
  **Jackson 3** for JSON.
- Tests: JUnit 5 + Testcontainers (Kafka, Elasticsearch, Grafana LGTM); chaos cases
  (ADR-0019).

## 13. Repository layout

```
pom.xml, mvnw                    Maven build (Spring Boot parent)
compose.yaml                     local Kafka + Elasticsearch + Grafana LGTM; one-shot kafka-init creates the demo topics
src/main/java/io/github/matabar/kafkafim/
  config/         @ConfigurationProperties model, validators, watcher, Binder reload, diff
  supervisor/     pipeline lifecycle, hot reload orchestration, reload endpoint
  source/kafka/   consumer factories per cluster, container per pipeline, offset tracker
  decode/         object / array-of-objects decoding, field extraction (JSON pointer), timestamp parsing
  route/          condition evaluation, index pattern resolution
  sink/es/        Rest5Client per (cluster, identity), batcher, bulk sender, body builder, item parser, error table, breaker
  dlq/            producer, record format
  telemetry/      meters, spans, health indicators
src/main/resources/application.yaml   profile-independent defaults (default profile: local)
src/main/resources/application-local.yaml   local-dev sample config: clusters + demo pipeline
src/test/java/...                     unit + Testcontainers integration tests
charts/kafkafim/                      production Helm chart
deploy/local/                         local Kubernetes demo (kind or Docker Desktop): plain Kafka + ES manifests, demo chart values, demo.sh up/smoke/down
deploy/grafana/                       Grafana dashboards, Prometheus alert rules + promtool tests, compose provisioning
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
and an air-gap bundle (the signed multi-arch image saved by digest with `cosign
save`, its digest file, chart, SBOM, checksums, import instructions) on the GitHub
Release. Production is air-gapped; the team imports the
bundle into the internal registry and applies one Helm release per Kubernetes
cluster with plain `helm upgrade --install`, run from the infrastructure repository's
CI inside the air gap (no Argo CD or Flux). The build is a single Maven module with
CI-friendly `${revision}` versioning (ADR-0003, ADR-0022, ADR-0023).

## 16. Roadmap

- **0.1** single pipeline end to end: bind config, preflight checks, consume, route,
  bulk, commit, metrics, DLQ. SCRAM-SHA-256 and basic auth identities. Compose stack.
- **0.2** groups, Helm chart, configtree secrets, hot reload, `validate` profile for
  the infrastructure repository's config checks, refusal of the `local` profile on
  Kubernetes.
- **0.3** traces/logs, Grafana dashboards + alerts, breaker, versioning, data streams.
- **1.0** runbook, load-test results, first production pipelines (each Logstash
  pipeline translated by hand, §14 and ADR-0017; there is no config translator).
- **Later**: DLQ tooling, transformations (if ever, as ES ingest pipelines first),
  `update`/`delete` ops, KEDA where allowed, read-only UI over the same config.

## 17. Open questions

None open. The questions listed in v0.4 were answered on 2026-11-08: production
Elasticsearch uses the cluster CA for trust only, with basic auth or an API key
(§5.2.1), and the per-cluster release is applied with plain `helm upgrade --install`
from the infrastructure repository (§15).
