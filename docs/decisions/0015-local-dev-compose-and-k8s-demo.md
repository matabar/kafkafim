# ADR-0015: Local development with Docker Compose; demo on Docker Desktop Kubernetes; production chart deploys only kafkafim

Status: accepted · Date: 2026-11-07

## Decision
- `compose.yaml` (Kafka, Elasticsearch, Grafana LGTM) starts automatically with
  `./mvnw spring-boot:run` via Spring Boot Docker Compose support.
- Integration tests use Testcontainers (Kafka, Elasticsearch, LGTM).
- `deploy/local/` holds a Kubernetes demo (Kafka + Elasticsearch operators are fine
  **locally**) that installs the real chart against them.
- `charts/kafkafim` never deploys Kafka or Elasticsearch; production values point at
  existing clusters.
