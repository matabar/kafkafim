# ADR-0039: Preflight checks `Read` on the source topics with the group check's admin client

Status: accepted · Date: 2026-11-13 · Refines [ADR-0038](0038-preflight-checks-consumer-group-permission.md)

## Context
The source's start check is a metadata fetch (`partitionsFor`), which the broker answers
with `Describe` alone. An identity with `Describe` but no `Read` on a source topic
therefore passed every check. Its fetches then failed with a topic authorization error,
which the listener container retries every `authRetryInterval` (10s) without stopping.
The pipeline showed `running`, readiness counted it as started, nothing was consumed
and no alert fired. The runbook listed it as a known gap (#142). It is the topic-side
twin of the group gap that ADR-0038 closed.

With kafka-clients 4.2.2 against a 4.1 `StandardAuthorizer` broker, `describeTopics`
with `includeAuthorizedOperations(true)` reports, by the identity's topic ACLs:

| Topic ACLs | Topic exists | Result |
|---|---|---|
| none | yes | the describe fails for that topic |
| `Describe` only | yes | authorized operations `[DESCRIBE]` |
| `Read` (which implies `Describe`) | yes | `[READ, DESCRIBE]` |
| `Read` | no | the describe fails for that topic |
| no authorizer on the broker | yes | operations that include `READ` |

Unlike a group, a source topic must exist before the pipeline starts (ADR-0021), so the
case ADR-0038 could not decide (a resource with nothing to describe yet) does not arise
here: a missing topic already fails the start.

## Decision
- The group check's admin client also sends one `describeTopics` for the pipeline's
  source topics, asking for the authorized operations. Both requests are in flight
  together and share the check's one deadline (the source's metadata timeout), so the
  topics add no connection and no waiting time.
- **Configuration** (`failed_preflight`): an existing topic whose authorized operations
  lack `READ`. The ERROR line names the topic, the identity and the cluster.
- **Passes**: `READ` listed; no operations listed (a broker that cannot say).
- **Left to the source's start**: any failure of the topic request. A topic that does
  not exist or that the identity cannot see fails the start (`MissingTopicsException`),
  and a broker that does not answer fails the source's metadata fetch. The topic check
  passes these, so each problem keeps its one message and its one owner.

## Alternatives
- **Fetch from the topic in preflight.** This is exact, but it needs a consumer with an
  assignment, reads data the pipeline then reads again, and takes far longer than a
  describe.
- **Report missing and invisible topics from this check too.** One ERROR line would
  then name them together with the other problems. It would also give the same problem
  two messages, depending on which check saw it first, and move a decision
  `source/kafka/` owns into the supervisor.
- **Leave it to the first fetch** and make the source stop on a topic authorization
  error. The container retries those on purpose, so that an ACL that arrives late or is
  restored needs no restart; that behaviour is worth keeping for a running pipeline.

## Consequences
- `Describe` without `Read` on a source topic now ends in `failed_preflight` and its
  alert, instead of a `running` pipeline that consumes nothing.
- No new connection and no extra timeout: the request rides on the group check.
- The check is best effort where the topic request fails but the source's metadata
  fetch then succeeds (a timeout on the describe alone, for example): a topic with
  `Describe` only still ends `running` in that case, as before this ADR.
- An ACL that arrives after the start attempt no longer heals by itself: the pipeline
  is in `failed_preflight` and needs a reload or a restart (runbook §5.4), as for the
  group and the DLQ topic.
- `Read` revoked after the start is still not detected: consumers retry every 10s and
  the state stays `running` (runbook §5.4), as for the group.
- A custom authorizer or an old broker that lists no operations passes, and the first
  fetch stays the check there.

## Correction (2026-11-14, #213)

This record first listed `ALL` as a passing operation. kafka-clients 4.2 never reports `ALL`, `ANY` or `UNKNOWN` among the authorized operations (`AdminUtils.validAclOperations` filters them out), so only `READ` can pass; the text above is corrected and the dead test removed.
