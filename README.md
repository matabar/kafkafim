# kafkafim

Production-grade Kafka → Elasticsearch ingest for Kubernetes. Replaces a fleet of
Logstash pipelines with one service that routes JSON records from many Kafka
clusters to many Elasticsearch clusters, with at-least-once delivery, declarative
per-event routing, a real DLQ, hot-reloaded Spring configuration rendered by Helm,
and OpenTelemetry built in. MVP scope: a shipper, not a pipeline engine (no
transformations, no expression language, no CLI).

Status: design and scaffolding. Nothing ships yet.

## Documents

- [Design](docs/DESIGN.md): goals, architecture, configuration model, failure
  handling, observability, migration, roadmap.
- [Decision records](docs/decisions/README.md): one ADR per decision.
- [Tech stack](docs/TECH_STACK.md): every library, its version and the reason.
- [Delivery](docs/DELIVERY.md): CI, releases, registries, importing into the
  air-gapped environment.
- [Helm chart](docs/HELM.md): one release per Kubernetes cluster, groups, secrets.
- [Runbook](docs/RUNBOOK.md): deploy, observe and recover kafkafim in production;
  incidents as symptom → check → action.

## Develop

Requires Docker (for the local stack and tests). No local Maven or Java install is
required beyond a JDK 27 on `PATH`.

```bash
./mvnw spring-boot:run      # starts Kafka, Elasticsearch and Grafana via compose.yaml
```

The stack also runs a one-shot `kafka-init` service that creates the topics the `local`
profile's `demo` pipeline needs (`demo.events`, `demo.events.kafkafim-dlq`; 3 partitions,
replication 1). kafkafim itself never creates topics ([ADR-0021](docs/decisions/0021-no-resource-provisioning.md)),
and the local broker has topic auto-creation disabled to mirror production. Smoke test once
the `demo` pipeline is `running`
(`curl -s localhost:8080/actuator/prometheus | grep 'kafkafim_pipeline_state{pipeline="demo",state="running"}'`):

```bash
echo '{"id":"smoke-1","region":"eu","@timestamp":"2026-11-08T10:00:00Z"}' | \
  docker compose run --rm -T --no-deps --entrypoint /opt/kafka/bin/kafka-console-producer.sh \
  kafka-init --bootstrap-server kafka:19092 --topic demo.events
curl -s 'localhost:9200/demo-*/_search?pretty'     # the document appears in demo-eu-<date>
docker compose down -v                             # reset the stack and its data
```

(The native broker image ships no Kafka CLI scripts, hence the JVM `apache/kafka` image of the
`kafka-init` service for topic and producer commands.)

```bash
./mvnw verify               # unit + integration tests (Testcontainers)
```

```bash
docker build -t kafkafim:dev .   # runtime image from target/*.jar
```

```bash
./mvnw spotless:apply       # format to Google Java Style before committing
```

Code follows the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html);
`./mvnw verify` fails on unformatted code or Checkstyle violations.

Actuator: `http://localhost:8080/actuator/health`, `/actuator/prometheus`.
Grafana (local stack): `http://localhost:3000`.

## License

[MIT](LICENSE).
