# ADR-0012: Configuration is Spring Boot configuration rendered by Helm; secrets via `configtree`; hot reload with `Binder`

Status: accepted · Date: 2026-11-07

## Context
Config is edited daily by one team, deployed to 2–3 clusters, must be IaC, must
reload without restarts, and must not carry secrets inline. No CRDs or operators in
production.

## Decision
- All settings live under `kafkafim.*`, bound to typed `@ConfigurationProperties`
  records, checked with Bean Validation plus a cross-reference validator.
- Helm renders `application-kafkafim.yaml` into one `ConfigMap` per group (shared
  cluster definitions + only that group's pipelines) and starts pods with
  `SPRING_PROFILES_ACTIVE=kafkafim` and `SPRING_CONFIG_ADDITIONAL_LOCATION` pointing
  at the mount. The profile is what makes Spring Boot load the profile-specific file,
  and it disables the default `local` profile (`spring.profiles.default: local`) that
  holds the development sample in `application-local.yaml`. Guards: the chart tests
  assert the env var; the application refuses the `local` profile when
  `KUBERNETES_SERVICE_HOST` is set; the chart passes the non-optional config
  location so a missing mount fails startup (the image default is `optional:` for
  local and CI runs).
- Secrets: Kubernetes `Secret`s mounted as directories and imported with
  `spring.config.import=configtree:...`; cluster definitions reference them with
  `${...}` placeholders. Literal secret values fail validation outside the local
  profile. Provisioning of Secrets stays outside the chart (`existingSecret`).
- Credentials are **named identities** defined per cluster, for Kafka and
  Elasticsearch alike. Kafka (`kafka.clusters.<c>.identities.<name>`: SASL and/or
  mTLS keystore) is referenced by `source.identity` and `dlq.identity` (default: the
  source identity). Elasticsearch (`elasticsearch.clusters.<c>.identities.<name>`:
  API key, basic or mTLS keystore) is referenced by `routes[].identity`. Each cluster
  may set a `default-identity`; trust material stays per cluster; every reference is
  validated at bind time; an identity change restarts only the pipelines using it.
- No config checksum annotation on the pod template: pipeline edits hot-reload,
  everything else rolls pods.
- Hot reload: watcher on config and secret directories (`WatchService` + 10s hash
  poll) and `POST /actuator/reload`. The tree is re-read through Spring's
  `Binder` into a fresh model, validated whole, then diffed by pipeline; changed
  pipelines drain and restart, others are untouched. Invalid trees are rejected
  whole.
- `validate` profile: the image started with `--spring.profiles.active=validate,kafkafim`
  binds, validates, prints a summary and exits 0/1 without connecting anywhere. It
  serves the infrastructure repository's CI (where the values live); this
  repository's CI smoke-tests the image with a liveness probe instead.

## Alternatives
- Own YAML format + JSON Schema + CLI validator: a second config system next to
  Spring's; rejected with the CLI (ADR-0018).
- Spring Cloud Kubernetes config reload: needs API RBAC and the Spring Cloud BOM for
  what a file watcher does.

## Consequences
The Spring property model is the public API; a future UI edits the same values.
