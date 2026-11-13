# ADR-0040: The `validate` profile accepts secrets that are not mounted

Status: accepted; refined by [ADR-0043](0043-validate-profile-fails-on-an-empty-configuration.md): a tree that defines no cluster and no pipeline fails under the profile, while clusters without pipelines stay valid · Date: 2026-11-13 · Refines [ADR-0012](0012-spring-configuration-helm-configtree-hot-reload.md) and [ADR-0018](0018-no-cli-single-run-mode.md)

## Context
ADR-0012 and ADR-0018 give the infrastructure repository's CI a `validate` profile: the
image binds and validates a rendered configuration and exits 0/1 without connecting
anywhere. They do not say what happens to secrets. The rendered ConfigMap refers to
them as `${kafka.<cluster>.<identity>.password}`, and at startup a credential
placeholder that nothing defines is always an error, because Spring would otherwise
bind the literal text `${...}` as the password (DESIGN §5.5). The CI has the values
file but usually not the Secrets, which live in the cluster (plain, Sealed or External
Secrets, or a Vault agent). Under the startup rule every real configuration would fail
validation there.

## Decision
(Matan, 2026-11-13, issue #95.)

- Under the `validate` profile a credential placeholder (secret or username) that
  nothing defines is not an error. Every other check is the one a normal start runs.
  Literal secrets are still rejected, also when reached through a placeholder.
- A valid tree logs one summary: totals, each pipeline with its clusters, identities,
  topics and routes, and the secret names the tree expects from mounted files, each
  marked `(not mounted)` when missing. It never contains a secret value.
- The profile creates neither the pipeline supervisor nor kafkafim's telemetry beans
  (`@Profile("!validate")` on their configurations), runs without a web server and with
  metric, trace and log export off (`application-validate.yaml`). `main` exits with the
  context's exit code once the summary is logged; an invalid tree fails startup, which
  exits 1.
- A tree without pipelines stays valid, as at startup.

## Consequences
- The check runs in CI with only the rendered file: no network, no Secrets.
- A misnamed or missing secret passes validation and is found when the pod starts. The
  summary's list is there to be compared with the chart's `secrets[]`.
- Trust and keystore files are not opened and preflight does not run, so reachability,
  topics, ACLs and credentials are still checked only by a real start.
- The relaxed rule exists only under the profile, and a process started with the
  profile exits once the summary is logged, so it never applies to a running pipeline.

## Alternatives
- Keep the startup rule and have CI mount dummy secret files: no code, but CI must
  build a dummy tree per cluster, and it proves nothing about the real secrets.
- The relaxed rule plus a strict switch for environments that do mount secrets: one
  more setting for a case nobody has; mounting the secrets (with
  `SPRING_CONFIG_IMPORT=configtree:/etc/kafkafim/secrets/`, as the chart's Deployment
  sets it) already marks them as mounted in the summary.
