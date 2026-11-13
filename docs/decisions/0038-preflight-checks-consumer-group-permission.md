# ADR-0038: Preflight checks the consumer group permission with one `DescribeGroups`

Status: accepted · Date: 2026-11-11 · Refines [ADR-0021](0021-no-resource-provisioning.md)

## Context
DESIGN §4.6 listed the consumer group permission among the preflight checks, with "the
first join is the check". That never reached `failed_preflight`. The listener container
retries authentication and authorization errors every `authRetryInterval` (10s), so a
denied join only publishes a `ConsumerRetryAuthEvent` and logs. It never publishes a
`ConsumerStoppedEvent`, so the `failed` state of ADR-0036 does not see it either. The
pipeline shows `running`, readiness counts it as started, and no alert fires. It was a
known gap in the runbook (#75).

The broker allows a join only with `Read` on the group (`Read` implies `Describe`). With
kafka-clients 4.2.2 against a 4.1 `StandardAuthorizer` broker, `describeConsumerGroups`
with `includeAuthorizedOperations(true)` reports, by the identity's group ACLs:

| Group ACLs | Group exists | Result |
|---|---|---|
| none, or the ACL names another group | either | `GroupAuthorizationException` (FindCoordinator needs `Describe`) |
| `Describe` only | yes | authorized operations `[DESCRIBE]` |
| `Read` (with or without `Describe`) | yes | `[READ, DESCRIBE]` |
| `Describe` or `Read` | no (never joined) | `GroupIdNotFoundException` |
| no authorizer on the broker | yes | every group operation |

## Decision
- Preflight sends one `DescribeGroups` for the pipeline's group. It uses a short-lived
  admin client built from the source identity's consumer settings (all of them except
  the consumer-only keys, so config providers and custom handler keys still apply),
  bounded by the source's metadata timeout (30s). It runs with the DLQ and
  Elasticsearch checks, so one ERROR line still names every problem.
- **Configuration** (`failed_preflight`): a group or other authorization error, an
  authentication error, an existing group whose authorized operations lack `READ`
  (and `ALL`), or a group id another protocol already uses (such as Connect).
- **Passes**: `READ` or `ALL` listed; no operations listed (a broker that cannot say);
  a group that does not exist yet; a broker that does not support the request.
- **Transient** (stays `starting`, retried with backoff): a timeout or connection error.
  An interrupt from a stop also counts as transient, and the stop discards the verdict.

## Alternatives
- **Join the group from preflight.** This is exact, but a join rebalances the live group
  of every other pod on each pipeline start and reload.
- **`describeAcls`.** It needs `Describe` on the cluster, which a consumer identity does
  not have. It also misses ACLs that are granted by other means, such as prefixed
  patterns, groups or a custom authorizer.
- **An `OffsetCommit` probe.** The broker checks `Read` on the group before it looks at
  the topics, so a commit to a topic that does not exist would test exactly `Read`. But
  it depends on the broker checking things in that order, and commits are not something
  preflight should send.

## Consequences
- The common mistakes now reach `failed_preflight` and its alert, with the group, the
  identity and the cluster named: no group ACL at all, an ACL on the wrong group name
  or prefix, and `Describe` without `Read` on a group that has joined before (every
  restart and reload after the first deployment).
- One case stays undetected: `Describe` without `Read` on a group that has never been
  joined. The broker has no description to report operations for. The first join is
  still the check there, and the runbook lists it as a known gap.
- Each pipeline start makes one more admin connection to the source cluster. On a
  broker without an authorizer the check always passes.
- Against a cluster that does not answer, each start attempt now also waits out this
  check's timeout (the source's metadata timeout, 30s) after the DLQ check, so retries
  in `starting` are spaced further apart.
