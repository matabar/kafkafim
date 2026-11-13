# ADR-0041: A reload that fails while it is applied is rolled back

Status: accepted · Date: 2026-11-13 · Refines [ADR-0028](0028-hot-reload-rereads-config-data-and-shares-building-blocks-per-cluster.md) and [ADR-0035](0035-dlq-writer-evicted-when-its-pipeline-leaves-the-instance.md)

## Context
A valid tree can still fail while the supervisor applies it: `PipelineResources.acquire`
throws when a client or producer cannot be built, and `PipelineRuntime.reconfigure`
throws when something started the pipeline after its drain. Until now `apply` stopped
where it failed (#161). The contexts acquired for pipelines earlier in the tree were
never released, so their Elasticsearch registries and DLQ producers stayed open until
the pod stopped, and each further attempt could add more. Their per-pipeline use counts
stayed too, which keeps ADR-0035 from evicting a DLQ writer later. The building blocks
of the running tree had already lost their place as the current ones, so later restarts
got new instances for clusters that had not changed. Pipelines reconfigured before the
failure ran the new tree while `properties()` reported the old one, and the pipelines
stopped for the reload stayed stopped until a later reload succeeded.

## Decision
- **Acquire first, change afterwards.** After the drains, `apply` acquires the contexts
  of every added and restarted pipeline, then reconfigures the restarted ones, and only
  then releases what the running tree used (previous contexts, removed pipelines). A
  restarted pipeline still takes its new use before it gives back the old one, so it
  keeps its DLQ writer as in ADR-0035.
- **A failure in either step is rolled back as a whole**, and the exception is rethrown:
  - pipelines already reconfigured get their previous context back;
  - every context acquired for the new tree is released, which closes the instances
    created for it;
  - `PipelineResources` makes the instances that were current before the reload current
    again (`checkpoint()` before the acquires, `rollback(checkpoint, contexts)` after
    the failure);
  - the pipelines the drains stopped while they were running, paused or starting are
    started again on the running tree. That includes pipelines the new tree removes:
    it did not take effect. A pipeline that was stopped or failed before the reload
    stays as it was.
- **`acquire` itself takes nothing when it throws**: instances it created are closed and
  the ones they replaced are current again.
- If a pipeline cannot get its previous context back, because something started or
  stopped it during the rollback, it keeps the new context, that context stays in use,
  and one WARN line names the pipeline. It stays so until a reload restarts that
  pipeline or the pod restarts: the next diff is taken against the running tree, so a
  reload that leaves the pipeline unchanged does not correct it. No production code
  starts or stops single pipelines today.
- Nothing is started again when a shutdown began during the drains.

## Alternatives
- **Only release what was acquired, and leave the pipelines stopped**: fixes the leak,
  but the pod keeps a tree in effect that it does not run, until someone changes the
  files again (the watcher does not retry the same content).
- **Keep going past the failing pipeline and apply the rest**: the pod would run a mix
  of two trees that neither `properties()` nor the reload result describes.
- **`startPipeline` takes the lifecycle lock**, so `reconfigure` cannot fail: removes
  one cause only, and `acquire` can still throw.

## Consequences
- After a failed apply the pod runs the tree it ran before, on the same clients,
  producers, breakers and DLQ writers. The restarted pipelines went through one drain
  and one start (a new preflight, a consumer group rebalance); nothing is lost, as with
  any reload restart.
- `kafkafim_config_reload_total{result=failure}` and the ERROR line are unchanged; the
  generation does not move. One WARN line lists the pipelines started again.
- A reload holds the old and the new building blocks of all its restarted pipelines at
  once for the time of the reconfigure, a little longer than before. One effect: when
  the last pipeline of an Elasticsearch registry restarts away from it and another
  pipeline of the same reload joins that cluster with the same definition, the registry
  (and its breaker state) is now shared instead of closed and built again.
- Known limit: a pipeline that had moved to new DLQ producers before a later
  reconfigure failed, and whose previous writer was blocked, comes back without its
  `dlq-blocked` pause, because the writer reports only the change to blocked. This
  needs the same concurrent start or stop as above (#207).
- An `Error` (not a `RuntimeException`) while a client is built is not rolled back (#208).
