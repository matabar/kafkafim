package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.source.kafka.KafkaPipelineSource;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs every configured pipeline of the pod (DESIGN §4): starts each one with its own preflight
 * when the application starts, turns sink and DLQ events into pause reasons, and stops all of them
 * gracefully on shutdown.
 *
 * <p><b>Lifecycle</b> (DESIGN §7, SIGTERM): a {@link SmartLifecycle} in the sources' phase ({@link
 * KafkaPipelineSource#PHASE}, {@code MAX_VALUE - 100}), so Spring stops it first. Every pipeline
 * stops in parallel: its source drains (up to 30s) and leaves the group (up to 10s), then its bulk
 * sinks close (up to {@link #SINK_CLOSE_TIMEOUT}); together within the 60s {@code
 * timeout-per-shutdown-phase}. The DLQ producers (phase {@code MAX_VALUE - 1000}) stop after that,
 * because draining records may still dead-letter documents; then the web server.
 *
 * <p><b>Pausing</b>: an Elasticsearch cluster's breaker opening pauses every pipeline routing to it
 * until it closes again; an identity rejected with 401/403 pauses the pipeline owning that bulk
 * stream until a request succeeds; a blocked DLQ writer pauses its pipeline until every failed send
 * was acknowledged. Nothing is dropped: paused pipelines keep committing what completes.
 *
 * <p><b>Per-pipeline control</b> ({@link #startPipeline}, {@link #stopPipeline}) is the hook for
 * hot reload (DESIGN §5.6), which builds on it in a later slice.
 */
public final class PipelineSupervisor implements SmartLifecycle {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineSupervisor.class);

  /** Phase: the Kafka sources' phase, stopped before the DLQ producers and the web server. */
  public static final int PHASE = KafkaPipelineSource.PHASE;

  /** How long a stopping pipeline waits for its sinks' outstanding requests after the source. */
  public static final java.time.Duration SINK_CLOSE_TIMEOUT = java.time.Duration.ofSeconds(10);

  private final Map<String, PipelineRuntime> pipelines;
  private final List<SupervisorListener> listeners;
  private volatile boolean running;

  /**
   * Creates a supervisor over one bound configuration; nothing starts until {@link #start()}.
   *
   * @param context what the pipelines share
   * @param listeners state hooks (telemetry)
   */
  PipelineSupervisor(PipelineRuntime.Context context, List<SupervisorListener> listeners) {
    this.listeners = List.copyOf(listeners);
    Map<String, PipelineRuntime> runtimes = new LinkedHashMap<>();
    for (String name : context.properties().pipelines().keySet()) {
      runtimes.put(name, new PipelineRuntime(name, context, this::stateChanged));
    }
    this.pipelines = java.util.Collections.unmodifiableMap(runtimes);
  }

  /** Starts every pipeline in the background; each one runs its own preflight. */
  @Override
  public void start() {
    running = true;
    if (pipelines.isEmpty()) {
      LOG.warn("No pipelines configured; kafkafim is idle");
    }
    pipelines.values().forEach(PipelineRuntime::start);
  }

  @Override
  public void stop() {
    running = false;
    List<Thread> stoppers = new ArrayList<>(pipelines.size());
    for (PipelineRuntime pipeline : pipelines.values()) {
      stoppers.add(
          Thread.ofVirtual().name("kafkafim-stop-" + pipeline.name()).start(pipeline::stop));
    }
    for (Thread stopper : stoppers) {
      try {
        stopper.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    LOG.info("All pipelines stopped");
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

  @Override
  public int getPhase() {
    return PHASE;
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

  /** Why a pipeline failed preflight, or {@code null}. */
  public @Nullable String failure(String pipeline) {
    return runtime(pipeline).failure();
  }

  /** The supervisor's active pause reasons of a pipeline. */
  public Set<String> pauseReasons(String pipeline) {
    return runtime(pipeline).pauseReasons();
  }

  /**
   * Starts one pipeline in the background (reload hook); no-op while starting or started.
   *
   * @throws IllegalStateException while the pipeline is being stopped
   */
  public void startPipeline(String pipeline) {
    runtime(pipeline).start();
  }

  /** Stops one pipeline gracefully and waits for it (reload hook). */
  public void stopPipeline(String pipeline) {
    runtime(pipeline).stop();
  }

  /** The source of a started pipeline, or {@code null}; for tests and telemetry. */
  @Nullable KafkaPipelineSource source(String pipeline) {
    return runtime(pipeline).source();
  }

  private PipelineRuntime runtime(String pipeline) {
    PipelineRuntime runtime = pipelines.get(pipeline);
    if (runtime == null) {
      throw new IllegalArgumentException("Unknown pipeline '" + pipeline + "'");
    }
    return runtime;
  }

  // ---- events from the sinks and the DLQ writers (ListenerRelay) ----

  /** An Elasticsearch cluster's breaker changed state. */
  void onBreakerStateChange(String cluster, CircuitBreaker.State to) {
    String reason = PipelineRuntime.BREAKER_REASON_PREFIX + cluster;
    boolean open = PipelineRuntime.isOpen(to);
    for (PipelineRuntime pipeline : pipelines.values()) {
      if (pipeline.clusters().contains(cluster)) {
        if (open) {
          pipeline.pause(reason);
        } else if (to == CircuitBreaker.State.CLOSED
            || to == CircuitBreaker.State.DISABLED
            || to == CircuitBreaker.State.METRICS_ONLY) {
          // HALF_OPEN keeps the pause: only the probes decide whether the cluster is back.
          pipeline.resume(reason);
        }
      }
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

  /** A pipeline's DLQ writer became blocked or unblocked. */
  void onDlqBlocked(String pipeline, boolean blocked) {
    PipelineRuntime runtime = pipelines.get(pipeline);
    if (runtime == null) {
      return;
    }
    if (blocked) {
      runtime.pause(PipelineRuntime.DLQ_BLOCKED_REASON);
    } else {
      runtime.resume(PipelineRuntime.DLQ_BLOCKED_REASON);
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

  private void notify(java.util.function.Consumer<SupervisorListener> event) {
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
