package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.ConfigDiff;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.sink.es.ElasticsearchSinks;
import io.github.matabar.kafkafim.source.kafka.KafkaPipelineSource;
import io.github.matabar.kafkafim.telemetry.KafkafimMeters;
import io.github.matabar.kafkafim.telemetry.PipelineMeters;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs every configured pipeline of the pod (DESIGN §4): starts each one with its own preflight
 * when the application starts, turns sink and DLQ events into pause reasons, applies reloaded
 * configurations pipeline by pipeline, and stops all of them gracefully on shutdown.
 *
 * <p><b>Lifecycle</b> (DESIGN §7, SIGTERM): a {@link SmartLifecycle} in the sources' phase ({@link
 * KafkaPipelineSource#PHASE}, {@code MAX_VALUE - 100}), so Spring stops it first. Every pipeline
 * stops in parallel: its source drains (up to 30s) and leaves the group (up to 10s), then its bulk
 * sinks close (up to {@link #SINK_CLOSE_TIMEOUT}); together within the 60s {@code
 * timeout-per-shutdown-phase}. The DLQ producers ({@link PipelineResources}, phase {@code MAX_VALUE
 * - 1000}) close after that, because draining records may still dead-letter documents; then the web
 * server. A shutdown that arrives during a reload waits for the reload to finish.
 *
 * <p><b>Pausing</b>: an Elasticsearch cluster's breaker opening pauses every pipeline routing to it
 * through that breaker until it closes again; an identity rejected with 401/403 pauses the pipeline
 * owning that bulk stream until a request succeeds; a blocked DLQ writer pauses its pipeline until
 * every failed send was acknowledged. Nothing is dropped: paused pipelines keep committing what
 * completes. Events from building blocks a pipeline no longer uses (a previous configuration
 * generation's breaker or DLQ writer) are ignored for it.
 *
 * <p><b>Reload</b> ({@link #apply}, DESIGN §5.6): removed and changed pipelines drain and stop in
 * parallel; changed ones get a new context and start again, added ones start, unchanged ones are
 * not touched, except that an unchanged one in {@code failed_preflight} or {@code failed} is
 * started once more ({@link #retryFailedPipelines}). An apply that fails is undone as a whole
 * (ADR-0041).
 */
public final class PipelineSupervisor implements SmartLifecycle {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineSupervisor.class);

  /** Phase: the Kafka sources' phase, stopped before the DLQ producers and the web server. */
  public static final int PHASE = KafkaPipelineSource.PHASE;

  /** How long a stopping pipeline waits for its sinks' outstanding requests after the source. */
  public static final Duration SINK_CLOSE_TIMEOUT = Duration.ofSeconds(10);

  private final PipelineResources resources;
  private final List<SupervisorListener> listeners;
  private final List<Runnable> topologyListeners = new CopyOnWriteArrayList<>();

  /** Held by start, stop, apply and retryFailedPipelines: one of them at a time. */
  private final Object lifecycle = new Object();

  private final Function<String, PipelineMeters> meters;
  private volatile Map<String, PipelineRuntime> pipelines;
  private volatile KafkafimProperties properties;
  private volatile boolean running;
  private volatile boolean stopRequested;

  /**
   * Creates a supervisor over the startup configuration; nothing starts until {@link #start()}.
   *
   * @param properties the validated startup configuration
   * @param resources where pipelines get their building blocks
   * @param listeners state hooks (telemetry)
   */
  PipelineSupervisor(
      KafkafimProperties properties,
      PipelineResources resources,
      List<SupervisorListener> listeners) {
    this(properties, resources, listeners, name -> PipelineMeters.NOOP);
  }

  /**
   * Creates a supervisor whose pipelines record the §10 meters.
   *
   * @param meters the meters of a pipeline by name ({@link KafkafimMeters#pipeline}), looked up
   *     whenever a pipeline starts
   */
  PipelineSupervisor(
      KafkafimProperties properties,
      PipelineResources resources,
      List<SupervisorListener> listeners,
      Function<String, PipelineMeters> meters) {
    this.resources = resources;
    this.listeners = List.copyOf(listeners);
    this.properties = properties;
    this.meters = meters;
    Map<String, PipelineRuntime> runtimes = new LinkedHashMap<>();
    for (String name : properties.pipelines().keySet()) {
      runtimes.put(name, newRuntime(name, resources.acquire(name, properties)));
    }
    this.pipelines = Collections.unmodifiableMap(runtimes);
  }

  private PipelineRuntime newRuntime(String name, PipelineRuntime.Context context) {
    return new PipelineRuntime(name, context, this::stateChanged, () -> meters.apply(name));
  }

  /** Starts every pipeline in the background; each one runs its own preflight. */
  @Override
  public void start() {
    synchronized (lifecycle) {
      running = true;
      if (pipelines.isEmpty()) {
        LOG.warn("No pipelines configured; kafkafim is idle");
      }
      pipelines.values().forEach(PipelineRuntime::start);
    }
  }

  @Override
  public void stop() {
    stopRequested = true;
    running = false;
    synchronized (lifecycle) {
      stopAll(pipelines.values());
      LOG.info("All pipelines stopped");
    }
  }

  /** Stops every pipeline in parallel without blocking the lifecycle thread. */
  @Override
  public void stop(Runnable callback) {
    Thread.ofVirtual()
        .name("kafkafim-supervisor-stop")
        .start(() -> {
          try {
            stop();
          } finally {
            callback.run();
          }
        });
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /** Whether a shutdown stopped, or is stopping, the supervisor (liveness stays up meanwhile). */
  public boolean isShuttingDown() {
    return stopRequested;
  }

  @Override
  public int getPhase() {
    return PHASE;
  }

  /** The configuration the pipelines were last applied from. */
  public KafkafimProperties properties() {
    return properties;
  }

  /** Pipeline names, in configuration order. */
  public Set<String> pipelineNames() {
    return pipelines.keySet();
  }

  /** Every pipeline's state, in configuration order. */
  public Map<String, PipelineState> states() {
    Map<String, PipelineState> states = new LinkedHashMap<>();
    pipelines.forEach((name, pipeline) -> states.put(name, pipeline.state()));
    return states;
  }

  /**
   * One pipeline's state.
   *
   * @throws IllegalArgumentException if there is no such pipeline
   */
  public PipelineState state(String pipeline) {
    return runtime(pipeline).state();
  }

  /** One pipeline's state, or {@code null} if there is no such pipeline (any more). */
  public @Nullable PipelineState stateOrNull(String pipeline) {
    PipelineRuntime runtime = pipelines.get(pipeline);
    return runtime == null ? null : runtime.state();
  }

  /** Why a pipeline failed (preflight, or while running), or {@code null}. */
  public @Nullable String failure(String pipeline) {
    return runtime(pipeline).failure();
  }

  /** The supervisor's active pause reasons of a pipeline. */
  public Set<String> pauseReasons(String pipeline) {
    return runtime(pipeline).pauseReasons();
  }

  /**
   * Starts one pipeline in the background; no-op while starting or started.
   *
   * @throws IllegalStateException while the pipeline is being stopped
   */
  public void startPipeline(String pipeline) {
    runtime(pipeline).start();
  }

  /**
   * Stops one pipeline gracefully and waits for it. A pipeline in {@code failed_preflight} or
   * {@code failed} keeps that state, so the next accepted reload retries it ({@link
   * #retryFailedPipelines}).
   */
  public void stopPipeline(String pipeline) {
    runtime(pipeline).stop();
  }

  /** The source of a started pipeline, or {@code null}; for tests and telemetry. */
  @Nullable KafkaPipelineSource source(String pipeline) {
    return runtime(pipeline).source();
  }

  /** The source of a started pipeline, or {@code null}, also for an unknown pipeline. */
  @Nullable KafkaPipelineSource sourceOrNull(String pipeline) {
    PipelineRuntime runtime = pipelines.get(pipeline);
    return runtime == null ? null : runtime.source();
  }

  /**
   * The worst state of the circuit breakers of an Elasticsearch cluster that the pipelines send
   * through (open over half-open over closed), or {@code null} if no pipeline routes to it. After a
   * reload, pipelines may use different configuration generations' breakers (DESIGN §5.6).
   */
  CircuitBreaker.@Nullable State breakerState(String cluster) {
    Map<ElasticsearchSinks, Boolean> seen = new IdentityHashMap<>();
    CircuitBreaker.State worst = null;
    for (PipelineRuntime runtime : pipelines.values()) {
      ElasticsearchSinks sinks = runtime.context().sinks().get(cluster);
      if (sinks == null || seen.put(sinks, Boolean.TRUE) != null) {
        continue;
      }
      CircuitBreaker.State state = sinks.breaker(cluster).getState();
      if (worst == null
          || KafkafimMeters.breakerValue(state) > KafkafimMeters.breakerValue(worst)) {
        worst = state;
      }
    }
    return worst;
  }

  /**
   * Runs {@code listener} once a reload is applied (every applied reload, before the restarted and
   * added pipelines start), so that metrics follow added, removed and relabelled pipelines.
   */
  void onTopologyChange(Runnable listener) {
    topologyListeners.add(listener);
  }

  /**
   * Applies a validated configuration (DESIGN §5.6) and waits for the drains: removed and restarted
   * pipelines stop in parallel (each drains its in-flight records and commits what completed);
   * restarted ones get the building blocks of {@code next} and start in the background, as do added
   * ones; unchanged ones keep running on what they have.
   *
   * <p>If acquiring a context or reconfiguring a pipeline fails, nothing of {@code next} takes
   * effect (ADR-0041): what was acquired is released, and the pipelines stopped for it that were
   * running or starting start again on the running configuration, including those {@code next}
   * removes. The exception is rethrown.
   *
   * @param next the new configuration, valid as a whole
   * @param diff {@code ConfigDiff.between(properties(), next)}
   * @return the unchanged pipelines in {@code failed_preflight} or {@code failed} that were started
   *     once more
   * @throws IllegalStateException if the supervisor is not running
   */
  List<String> apply(KafkafimProperties next, ConfigDiff diff) {
    synchronized (lifecycle) {
      if (!running) {
        throw new IllegalStateException("The pipeline supervisor is not running");
      }
      Map<String, PipelineRuntime> current = pipelines;
      List<PipelineRuntime> toStop = new ArrayList<>();
      for (String name : diff.removed()) {
        toStop.add(current.get(name));
      }
      for (String name : diff.restarted().keySet()) {
        toStop.add(current.get(name));
      }
      // What the drains stop while it runs or starts: a failed apply starts those again. Read
      // before the drains, so a pipeline that fails in between is started again too (a retry).
      List<PipelineRuntime> active = new ArrayList<>();
      for (PipelineRuntime runtime : toStop) {
        PipelineState state = runtime.state();
        if (state.isStarted() || state == PipelineState.STARTING) {
          active.add(runtime);
        }
      }
      stopAll(toStop);

      // Everything is acquired before any pipeline changes, and nothing of the running
      // configuration is released before every pipeline changed: a failure can be undone.
      PipelineResources.Checkpoint checkpoint = resources.checkpoint();
      Map<String, PipelineRuntime.Context> acquired = new LinkedHashMap<>();
      Map<PipelineRuntime, PipelineRuntime.Context> replaced = new LinkedHashMap<>();
      Map<String, PipelineRuntime> updated = new LinkedHashMap<>();
      List<PipelineRuntime> toStart = new ArrayList<>();
      try {
        for (String name : next.pipelines().keySet()) {
          if (!current.containsKey(name) || diff.restarted().containsKey(name)) {
            acquired.put(name, resources.acquire(name, next));
          }
        }
        for (String name : next.pipelines().keySet()) {
          PipelineRuntime runtime = current.get(name);
          PipelineRuntime.Context context = acquired.get(name);
          if (runtime == null) {
            runtime = newRuntime(name, context);
            toStart.add(runtime);
          } else if (context != null) {
            replaced.put(runtime, runtime.reconfigure(context));
            toStart.add(runtime);
          }
          updated.put(name, runtime);
        }
      } catch (RuntimeException e) {
        rollBack(checkpoint, acquired.values(), replaced, active);
        throw e;
      }
      pipelines = Collections.unmodifiableMap(updated);
      replaced.values().forEach(resources::release);
      for (String name : diff.removed()) {
        resources.release(current.get(name).context());
        lastStates.remove(name);
        lastReasons.remove(name);
      }
      resources.retireStale(next);
      properties = next;
      topologyListeners.forEach(PipelineSupervisor::runQuietly);
      if (running) {
        toStart.forEach(PipelineRuntime::start);
        return retryFailed(diff.unchanged());
      }
      // A shutdown began during the drains: stop() runs next and would stop them again.
      LOG.info("Shutdown in progress; {} pipeline(s) of the reload not started", toStart.size());
      return List.of();
    }
  }

  /**
   * Undoes an apply that failed after the drains (ADR-0041), so that the running configuration
   * stays in effect as a whole: pipelines already reconfigured get their previous context back,
   * everything acquired for the new configuration is released, the building blocks that were
   * current are current again, and the pipelines the drains stopped while running or starting start
   * again. Pipelines the new configuration removes were not released yet and are among those.
   *
   * @param checkpoint the building blocks that were current before the first acquire
   * @param acquired every context acquired for the new configuration
   * @param replaced the reconfigured pipelines, each with its previous context
   * @param active the pipelines to start again
   */
  private void rollBack(
      PipelineResources.Checkpoint checkpoint,
      Collection<PipelineRuntime.Context> acquired,
      Map<PipelineRuntime, PipelineRuntime.Context> replaced,
      List<PipelineRuntime> active) {
    List<PipelineRuntime.Context> toRelease = new ArrayList<>(acquired);
    replaced.forEach((runtime, previous) -> {
      try {
        runtime.reconfigure(previous);
        runtime.restoreDlqBlocked();
      } catch (RuntimeException e) {
        // Something started or is stopping it meanwhile (startPipeline and stopPipeline do not
        // take the lifecycle lock): it has the new context, so that one stays in use.
        PipelineRuntime.Context kept = runtime.context();
        toRelease.removeIf(context -> context == kept); // by identity: contexts are records
        toRelease.add(previous);
        LOG.warn(
            "Pipeline '{}' was started or stopped while a failed reload was undone; it keeps"
                + " the new configuration until a reload restarts it or the pod restarts",
            runtime.name(),
            e);
      }
    });
    resources.rollback(checkpoint, toRelease);
    if (!running) {
      return; // a shutdown began during the drains: stop() runs next
    }
    List<String> started = new ArrayList<>();
    for (PipelineRuntime runtime : active) {
      try {
        runtime.start();
        started.add(runtime.name());
      } catch (RuntimeException e) {
        LOG.warn("Pipeline '{}' could not be started again", runtime.name(), e);
      }
    }
    LOG.warn(
        "Applying the configuration failed; the running configuration stays in effect."
            + " Pipelines started again after their drain: {}",
        started);
  }

  /**
   * Starts once more those of {@code candidates} that failed, in preflight or while running (DESIGN
   * §5.6, ADR-0032, ADR-0036): an accepted reload that leaves a pipeline unchanged retries it,
   * since its cause (a missing topic, an ACL, a credential, a consumer error) is usually fixed
   * outside kafkafim. One attempt per call, none while the pipeline is starting or stopping;
   * nothing while the supervisor is not running.
   *
   * @param candidates pipeline names, typically {@code ConfigDiff.unchanged()}
   * @return the pipelines started, in configuration order
   */
  List<String> retryFailedPipelines(Collection<String> candidates) {
    synchronized (lifecycle) {
      return running ? retryFailed(candidates) : List.of();
    }
  }

  private List<String> retryFailed(Collection<String> candidates) {
    Set<String> wanted = Set.copyOf(candidates);
    List<String> retried = new ArrayList<>();
    pipelines.forEach((name, runtime) -> {
      if (wanted.contains(name) && runtime.retryIfFailed()) {
        retried.add(name);
      }
    });
    return List.copyOf(retried);
  }

  private static void runQuietly(Runnable listener) {
    try {
      listener.run();
    } catch (RuntimeException e) {
      LOG.warn("Topology listener {} failed", listener, e);
    }
  }

  /**
   * Stops pipelines in parallel and waits for every drain, also when interrupted (the flag is kept
   * for the caller): returning early would let a reload reconfigure or release a pipeline that is
   * still draining.
   */
  private static void stopAll(Collection<PipelineRuntime> runtimes) {
    List<Thread> stoppers = new ArrayList<>(runtimes.size());
    for (PipelineRuntime pipeline : runtimes) {
      stoppers.add(
          Thread.ofVirtual().name("kafkafim-stop-" + pipeline.name()).start(pipeline::stop));
    }
    boolean interrupted = false;
    for (Thread stopper : stoppers) {
      while (true) {
        try {
          stopper.join();
          break;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private PipelineRuntime runtime(String pipeline) {
    PipelineRuntime runtime = pipelines.get(pipeline);
    if (runtime == null) {
      throw new IllegalArgumentException("Unknown pipeline '" + pipeline + "'");
    }
    return runtime;
  }

  // ---- events from the sinks and the DLQ writers (ListenerRelay) ----

  /**
   * An Elasticsearch cluster's breaker changed state; applies to the pipelines that send to that
   * cluster through {@code owner} (the {@code ElasticsearchSinks} that owns the breaker).
   */
  void onBreakerStateChange(@Nullable Object owner, String cluster, CircuitBreaker.State to) {
    String reason = PipelineRuntime.BREAKER_REASON_PREFIX + cluster;
    boolean open = PipelineRuntime.isOpen(to);
    boolean closed =
        to == CircuitBreaker.State.CLOSED
            || to == CircuitBreaker.State.DISABLED
            || to == CircuitBreaker.State.METRICS_ONLY;
    if (!open && !closed) {
      return; // HALF_OPEN keeps the pause: only the probes decide whether the cluster is back.
    }
    for (PipelineRuntime pipeline : pipelines.values()) {
      pipeline.pauseIfOwner(p -> p.usesSinks(cluster, owner), reason, open);
    }
  }

  /**
   * A bulk stream's identity was rejected (401/403) or accepted again: the runtime re-reads the
   * current sink's auth state, so stale or reordered events cannot leave a pause behind.
   */
  void onAuthChanged(String pipeline, String cluster, @Nullable String identity) {
    PipelineRuntime runtime = pipelines.get(pipeline);
    if (runtime != null) {
      runtime.authChanged(cluster, identity);
    }
  }

  /**
   * A pipeline's DLQ writer became blocked or unblocked; ignored unless the pipeline still writes
   * through {@code owner} (the {@code DlqProducers} that owns the writer).
   */
  void onDlqBlocked(@Nullable Object owner, String pipeline, boolean blocked) {
    PipelineRuntime runtime = pipelines.get(pipeline);
    if (runtime != null) {
      runtime.pauseIfOwner(r -> r.usesDlq(owner), PipelineRuntime.DLQ_BLOCKED_REASON, blocked);
    }
  }

  private final Map<String, PipelineState> lastStates = new ConcurrentHashMap<>();
  private final Map<String, Set<String>> lastReasons = new ConcurrentHashMap<>();

  /** Called by a runtime, with its lock held, after its state or pause reasons may have changed. */
  private void stateChanged(PipelineRuntime pipeline) {
    PipelineState to = pipeline.state();
    PipelineState previousState = lastStates.put(pipeline.name(), to);
    PipelineState from = previousState == null ? PipelineState.STOPPED : previousState;
    if (from != to) {
      LOG.info("Pipeline '{}': {} -> {}", pipeline.name(), from.label(), to.label());
      notify(l -> l.onStateChange(pipeline.name(), from, to));
    }
    Set<String> reasons = pipeline.pauseReasons();
    Set<String> previous = lastReasons.put(pipeline.name(), reasons);
    if (!reasons.equals(previous == null ? Set.of() : previous)) {
      notify(l -> l.onPauseReasonsChange(pipeline.name(), reasons));
    }
  }

  private void notify(Consumer<SupervisorListener> event) {
    for (SupervisorListener listener : listeners) {
      try {
        event.accept(listener);
      } catch (RuntimeException e) {
        LOG.warn("Supervisor listener {} failed", listener, e);
      }
    }
  }

  /** For tests: the runtimes. */
  Collection<PipelineRuntime> runtimes() {
    return pipelines.values();
  }
}
