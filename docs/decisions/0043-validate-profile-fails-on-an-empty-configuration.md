# ADR-0043: The `validate` profile fails on a configuration that defines nothing

Status: accepted · Date: 2026-11-13 · Refines [ADR-0040](0040-validate-profile-accepts-unmounted-secrets.md)

## Context
ADR-0040 kept one startup rule under the `validate` profile: a tree without pipelines
is valid (a normal start only warns about it). In the infrastructure repository's CI
that also covered a mistake in the check itself. If `application-kafkafim.yaml` is not
where the image reads it, or the `kafkafim` profile is not active, nothing is bound,
and the check passed with `0 pipeline(s)` in its summary for a configuration that was
never read (issue #177).

The options were:

1. Keep it: the profile accepts exactly what a start accepts, and CI has to read the
   summary or grep the pipeline count.
2. Exit 1 under the profile when there are no pipelines: catches the wrong mount, but
   a deliberately empty group fails the check.
3. Exit 1 only when no `kafkafim.*` configuration is set at all: catches the wrong
   mount and still allows an empty group, at the price of one more special case.

## Decision
(Matan, 2026-11-13, issue #177: option 3.)

- Under the `validate` profile a tree that defines no Kafka cluster, no Elasticsearch
  cluster and no pipeline fails startup, so the process exits 1. The message says
  `No kafkafim configuration found` and names the mount path and the profile.
- "Nothing set" is judged on the bound tree, not on property names: the image's own
  `application.yaml` always sets `kafkafim.reload.*`, so some `kafkafim.*` property
  always exists. `kafkafim.reload.*` or `kafkafim.allow-inline-secrets` alone are
  therefore not configuration; one cluster is.
- Clusters without pipelines stay valid and show `0 pipeline(s)`: that is a
  deliberately empty group, which the chart renders with the shared cluster maps.
- A normal start is unchanged: no pipelines stays a warning there.
- Binding and validation errors come first. The check runs only on a tree that bound
  and validated.

## Consequences
- A wrong mount path, a missing or empty file, or a missing `kafkafim` profile no
  longer passes the check.
- The profile no longer accepts exactly what a start accepts. A pod started normally
  with the same empty tree starts and warns.
- A file that was read but sets only reload settings or `allow-inline-secrets` fails
  with a message about the mount path. The message also says the file may define no
  clusters or pipelines.
- The failure is not a validation violation, so it is not in Boot's "APPLICATION FAILED
  TO START" report. It is the exception of the `Application run failed` line
  ([HELM.md](../HELM.md)).

## Alternatives
- Options 1 and 2 above.
- Checking where properties come from (anything outside the classpath counts as
  "found"): more code, and any stray `KAFKAFIM_*` environment variable would count as
  configuration.
