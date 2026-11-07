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

## Develop

Requires Docker (for the local stack and tests). No local Maven or Java install is
required beyond a JDK 27 on `PATH`.

```bash
./mvnw spring-boot:run      # starts Kafka, Elasticsearch and Grafana via compose.yaml
```

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
