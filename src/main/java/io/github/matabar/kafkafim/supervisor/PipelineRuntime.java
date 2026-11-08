package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
import io.github.matabar.kafkafim.dlq.DlqProducers;
import io.github.matabar.kafkafim.dlq.DlqWriter;
import io.github.matabar.kafkafim.sink.es.BulkSink;
import io.github.matabar.kafkafim.sink.es.ElasticsearchSinks;
import io.github.matabar.kafkafim.sink.es.SinkKey;
import io.github.matabar.kafkafim.sink.es.SinkSettings;
import io.github.matabar.kafkafim.source.kafka.ConsumerFactories;
import io.github.matabar.kafkafim.source.kafka.KafkaPipelineSource;
import io.github.matabar.kafkafim.source.kafka.MissingTopicsException;
import io.github.matabar.kafkafim.source.kafka.SourceOptions;
import io.github.matabar.kafkafim.source.kafka.SourceRecord;
import io.github.matabar.kafkafim.source.kafka.SourceRecordHandler;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One pipeline at runtime: its preflight, its Kafka source, its bulk sinks (one per {@code
 * (cluster, identity)}) and its DLQ writer, wired through a {@link RecordProcessor}. It can be
 * started and stopped on its own, which is what a reload of one pipeline needs (DESIGN §5.6).
 *
 * <p><b>Start</b> runs on its own virtual thread: compile the configuration, run the {@link
 * Preflight} checks, open the sinks, then start the source (which fails with {@link
 * MissingTopicsException} for a missing source topic). A configuration problem leaves the pipeline
 * in {@link PipelineState#FAILED_PREFLIGHT}; an unreachable broker or cluster is retried with
 * backoff (1s doubling to 30s) in {@link PipelineState#STARTING}.
 *
 * <p><b>Stop</b> (graceful, DESIGN §7): the source pauses, drains its in-flight records (whose
 * outcomes still need the sinks and the DLQ writer) and leaves the group, committing what
 * completed; then the sinks close. The DLQ writer is shared and closes with {@link DlqProducers}.
 *
 * <p><b>Pause reasons</b> set by the supervisor (breaker, auth, DLQ blocked) are kept here, so they
 * survive a restart of the source and are applied to a source created later.
 */
final class PipelineRuntime {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineRuntime.class);

  /** Pause reason prefix while an Elasticsearch cluster's breaker is open. */
  static final String BREAKER_REASON_PREFIX = "es-breaker:";

  /** Pause reason prefix while an Elasticsearch identity is rejected (401/403). */
  static final String AUTH_REASON_PREFIX = "es-auth:";

  /** Pause reason while the pipeline's DLQ writer is blocked. */
  static final String DLQ_BLOCKED_REASON = "dlq-blocked";

  private static final Duration FIRST_RETRY = Duration.ofSeconds(1);
  private static final Duration MAX_RETRY = Duration.ofSeconds(30);

  /** What every runtime of one configuration shares. */
  record Context(
      KafkafimProperties properties,
      ConsumerFactories consumerFactories,
      DlqProducers dlqProducers,
      ElasticsearchSinks sinks,
      Preflight.AuthCheck authCheck,
      SourceOptions sourceOptions,
      Duration sinkCloseTimeout,
      Clock clock) {}

  private final String name;
  private final Pipeline config;
  private final Context context;
  private final Consumer<PipelineRuntime> onChange;
  private final Set<String> clusters;

  private final Object lock = new Object();
  private PipelineState state = PipelineState.STOPPED; // guarded by lock
  private @Nullable String failure; // guarded by lock
  private final Set<String> pauseReasons = new TreeSet<>(); // guarded by lock
  private @Nullable Thread starter; // guarded by lock
  private int stops; // stops in progress, guarded by lock
  private @Nullable CountDownLatch stopSignal; // guarded by lock
  private @Nullable KafkaPipelineSource source; // guarded by lock
  private Map<RouteTarget.StreamKey, BulkSink<RecordProcessor.DocumentRef>> sinks =
      Map.of(); // guarded by lock

  PipelineRuntime(String name, Context context, Consumer<PipelineRuntime> onChange) {
    this.name = name;
    this.context = context;
    this.config = context.properties().pipelines().get(name);
    if (config == null) {
      throw new IllegalArgumentException("Unknown pipeline '" + name + "'");
    }
    this.onChange = onChange;
    Set<String> routed = new LinkedHashSet<>();
    for (Pipeline.Route route : config.routes()) {
      routed.add(route.cluster());
    }
    this.clusters = Set.copyOf(routed);
  }

  String name() {
    return name;
  }

  /** The Elasticsearch clusters this pipeline's routes write to. */
  Set<String> clusters() {
    return clusters;
  }

  /** The current state; {@link PipelineState#PAUSED} while started with a pause reason. */
  PipelineState state() {
    synchronized (lock) {
      return effectiveState();
    }
  }

  /** Why the last start failed, or {@code null}. */
  @Nullable String failure() {
    synchronized (lock) {
      return failure;
    }
  }

  /** The supervisor's active pause reasons. */
  Set<String> pauseReasons() {
    synchronized (lock) {
      return Set.copyOf(pauseReasons);
    }
  }

  /** The running source, for tests and telemetry. */
  @Nullable KafkaPipelineSource source() {
    synchronized (lock) {
      return source;
    }
  }

  /** Starts the pipeline in the background; no-op while starting or started. */
  void start() {
    synchronized (lock) {
      if (stops > 0) {
        throw new IllegalStateException("Pipeline '" + name + "' is stopping; start it afterwards");
      }
      if (starter != null || state.isStarted()) {
        return;
      }
      failure = null;
      CountDownLatch signal = new CountDownLatch(1);
      stopSignal = signal;
      setState(PipelineState.STARTING);
      starter = Thread.ofVirtual().name("kafkafim-start-" + name).start(() -> startLoop(signal));
    }
  }

  private void startLoop(CountDownLatch signal) {
    Duration delay = FIRST_RETRY;
    try {
      while (signal.getCount() > 0) {
        Attempt attempt = attemptStart(signal);
        if (attempt != Attempt.RETRY) {
          return;
        }
        if (signal.await(delay.toMillis(), TimeUnit.MILLISECONDS)) {
          return;
        }
        delay = delay.multipliedBy(2).compareTo(MAX_RETRY) > 0 ? MAX_RETRY : delay.multipliedBy(2);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (RuntimeException e) {
      LOG.error("Pipeline '{}': start failed unexpectedly", name, e);
      synchronized (lock) {
        failure = e.toString();
        setState(PipelineState.FAILED_PREFLIGHT);
      }
    } finally {
      synchronized (lock) {
        starter = null;
      }
    }
  }

  private enum Attempt {
    STARTED,
    FAILED,
    RETRY,
    CANCELLED
  }

  private Attempt attemptStart(CountDownLatch signal) {
    CompiledPipeline compiled;
    DlqWriter writer;
    try {
      compiled = PipelineMapping.compile(name, context.properties());
      writer = context.dlqProducers().writer(name);
    } catch (IllegalArgumentException | KafkaException e) {
      return failed(List.of("configuration cannot run: " + e.getMessage()));
    }
    Preflight.Result result =
        Preflight.run(compiled, () -> context.dlqProducers().checkTopic(name), context.authCheck());
    if (!result.configuration().isEmpty()) {
      return failed(result.configuration());
    }
    if (!result.transientProblems().isEmpty()) {
      LOG.warn(
          "Pipeline '{}': preflight could not complete, retrying: {}",
          name,
          String.join("; ", result.transientProblems()));
      return Attempt.RETRY;
    }
    KafkaPipelineSource created;
    synchronized (lock) {
      if (signal.getCount() == 0) {
        return Attempt.CANCELLED;
      }
      Map<RouteTarget.StreamKey, BulkSink<RecordProcessor.DocumentRef>> opened =
          new LinkedHashMap<>();
      RecordProcessor[] processor = new RecordProcessor[1];
      try {
        for (Map.Entry<RouteTarget.StreamKey, SinkSettings> stream :
            compiled.streams().entrySet()) {
          RouteTarget.StreamKey key = stream.getKey();
          opened.put(
              key,
              context
                  .sinks()
                  .open(
                      new SinkKey(name, key.cluster(), key.identity()),
                      stream.getValue(),
                      (action, outcome) -> processor[0].onOutcome(action, outcome)));
        }
        Map<RouteTarget.StreamKey, RecordProcessor.ActionSink> actionSinks = new LinkedHashMap<>();
        opened.forEach((key, sink) -> actionSinks.put(key, adapt(sink)));
        processor[0] =
            new RecordProcessor(compiled, actionSinks::get, writer::send, context.clock());
        created =
            new KafkaPipelineSource(
                name,
                config.source(),
                context
                    .consumerFactories()
                    .get(config.source().cluster(), config.source().identity()),
                new Handler(processor[0], actionSinks.values()),
                context.sourceOptions());
      } catch (IllegalArgumentException | KafkaException e) {
        closeSinks(opened, Duration.ZERO);
        return failed(List.of("configuration cannot run: " + e.getMessage()));
      }
      sinks = opened;
      source = created;
      resetReasonsForStart();
      pauseReasons.forEach(created::pause);
    }
    try {
      created.start();
    } catch (MissingTopicsException e) {
      discard(created);
      return failed(
          List.of(
              "source topics "
                  + e.topics()
                  + " do not exist or are not visible to identity '"
                  + config.source().identity()
                  + "' on Kafka cluster '"
                  + config.source().cluster()
                  + "'"));
    } catch (AuthenticationException | AuthorizationException e) {
      discard(created);
      return failed(
          List.of(
              "identity '"
                  + config.source().identity()
                  + "' on Kafka cluster '"
                  + config.source().cluster()
                  + "' rejected reading "
                  + config.source().topics()
                  + ": "
                  + e.getMessage()));
    } catch (KafkaException e) {
      discard(created);
      LOG.warn("Pipeline '{}': source could not start, retrying: {}", name, e.toString());
      return Attempt.RETRY;
    } catch (RuntimeException e) {
      discard(created);
      LOG.error("Pipeline '{}': source failed to start", name, e);
      return failed(List.of("source failed to start: " + e));
    }
    synchronized (lock) {
      if (signal.getCount() == 0) {
        // stop() raced with the start; it waits for this thread and then stops the source.
        return Attempt.CANCELLED;
      }
      setState(PipelineState.RUNNING);
    }
    LOG.info(
        "Pipeline '{}': running ({} -> {} bulk stream(s), DLQ '{}')",
        name,
        config.source().topics(),
        compiled.streams().size(),
        config.dlq().topic());
    return Attempt.STARTED;
  }

  /**
   * Prepares the pause reasons for a new source, under the lock. Breaker and auth reasons are
   * dropped: they belong to the previous run's sinks, whose senders were the only ones probing the
   * breaker or retrying the identity, so keeping them would pause the new source with nothing left
   * to clear them. If the cluster is still down or the identity still rejected, the new sinks'
   * first requests report it again (the breaker moves on to half-open and open, the sink to auth
   * failure), and the in-flight byte budget bounds what is fetched meanwhile. {@code dlq-blocked}
   * stays: the pipeline's DLQ writer outlives the source, keeps retrying its failed sends, and its
   * {@code onUnblocked} clears the reason. No foreign lock is taken here (the DLQ writer calls its
   * listener under its own lock, which leads to this runtime's lock).
   */
  void resetReasonsForStart() {
    synchronized (lock) {
      pauseReasons.removeIf(reason ->
          reason.startsWith(AUTH_REASON_PREFIX) || reason.startsWith(BREAKER_REASON_PREFIX));
      report();
    }
  }

  private Attempt failed(List<String> problems) {
    String message = String.join("; ", problems);
    LOG.error("Pipeline '{}': failed preflight, staying stopped: {}", name, message);
    synchronized (lock) {
      failure = message;
      setState(PipelineState.FAILED_PREFLIGHT);
    }
    return Attempt.FAILED;
  }

  /** Forgets a source that did not start, and closes the sinks opened for it. */
  private void discard(KafkaPipelineSource created) {
    Map<RouteTarget.StreamKey, BulkSink<RecordProcessor.DocumentRef>> toClose;
    synchronized (lock) {
      if (source == created) {
        source = null;
      }
      toClose = sinks;
      sinks = Map.of();
    }
    closeSinks(toClose, Duration.ZERO);
  }

  /**
   * Stops the pipeline gracefully and waits for it: cancels a pending start, drains and stops the
   * source, then closes the sinks. Idempotent.
   */
  void stop() {
    Thread pending;
    synchronized (lock) {
      stops++;
      if (stopSignal != null) {
        stopSignal.countDown();
      }
      pending = starter;
    }
    KafkaPipelineSource running = null;
    Map<RouteTarget.StreamKey, BulkSink<RecordProcessor.DocumentRef>> toClose = Map.of();
    try {
      if (pending != null) {
        try {
          pending.join();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      synchronized (lock) {
        running = source;
        toClose = sinks;
      }
      if (running != null) {
        running.stop();
      }
      closeSinks(toClose, context.sinkCloseTimeout());
    } finally {
      synchronized (lock) {
        // Only what this stop stopped is forgotten; no start can run while a stop is in progress.
        if (source == running) {
          source = null;
        }
        if (sinks == toClose) {
          sinks = Map.of();
        }
        pauseReasons.removeIf(reason -> reason.startsWith(AUTH_REASON_PREFIX));
        if (state != PipelineState.FAILED_PREFLIGHT) {
          setState(PipelineState.STOPPED);
        }
        stops--;
      }
    }
  }

  private void closeSinks(
      Map<RouteTarget.StreamKey, BulkSink<RecordProcessor.DocumentRef>> toClose, Duration timeout) {
    if (toClose.isEmpty()) {
      return;
    }
    List<Thread> closers = new ArrayList<>(toClose.size());
    for (BulkSink<RecordProcessor.DocumentRef> sink : toClose.values()) {
      closers.add(Thread.ofVirtual().start(() -> sink.close(timeout)));
    }
    for (Thread closer : closers) {
      try {
        closer.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** Adds a supervisor pause reason; the source stops fetching while any reason is active. */
  void pause(String reason) {
    synchronized (lock) {
      if (pauseReasons.add(reason)) {
        LOG.warn("Pipeline '{}': pausing ({})", name, reason);
        if (source != null) {
          source.pause(reason);
        }
        report();
      }
    }
  }

  /** Clears a supervisor pause reason. */
  void resume(String reason) {
    synchronized (lock) {
      if (pauseReasons.remove(reason)) {
        LOG.info("Pipeline '{}': {} cleared", name, reason);
        if (source != null) {
          source.resume(reason);
        }
        report();
      }
    }
  }

  /**
   * Re-reads the auth state of one bulk stream after a sink reported a change, and pauses or
   * resumes accordingly. Events are hints: one from a sink of an earlier run, or a failure event
   * overtaken by a recovery on another sender thread, cannot leave a stale pause, because only the
   * current sink's {@link BulkSink#authFailing()} decides.
   */
  void authChanged(String cluster, @Nullable String identity) {
    synchronized (lock) {
      BulkSink<RecordProcessor.DocumentRef> sink =
          sinks.get(new RouteTarget.StreamKey(cluster, identity));
      String reason = authReason(cluster, identity);
      if (sink != null && sink.authFailing()) {
        pause(reason);
      } else {
        resume(reason);
      }
    }
  }

  static String authReason(String cluster, @Nullable String identity) {
    return AUTH_REASON_PREFIX + cluster + "/" + (identity == null ? "<none>" : identity);
  }

  /** Whether a breaker state stops sends to its cluster. */
  static boolean isOpen(CircuitBreaker.State state) {
    return state == CircuitBreaker.State.OPEN || state == CircuitBreaker.State.FORCED_OPEN;
  }

  private void setState(PipelineState next) {
    state = next;
    report();
  }

  /** Reports a possible change of state or pause reasons; called with the lock held. */
  private void report() {
    onChange.accept(this);
  }

  private PipelineState effectiveState() {
    if (state == PipelineState.RUNNING && !pauseReasons.isEmpty()) {
      return PipelineState.PAUSED;
    }
    return state;
  }

  private static RecordProcessor.ActionSink adapt(BulkSink<RecordProcessor.DocumentRef> sink) {
    return new RecordProcessor.ActionSink() {
      @Override
      public void submit(
          io.github.matabar.kafkafim.sink.es.BulkAction<RecordProcessor.DocumentRef> a) {
        sink.submit(a);
      }

      @Override
      public void flush() {
        sink.flush();
      }
    };
  }

  /** The source's handler: the record processor, plus a flush when partitions go away. */
  private static final class Handler implements SourceRecordHandler {

    private final RecordProcessor processor;
    private final Collection<RecordProcessor.ActionSink> sinks;

    Handler(RecordProcessor processor, Collection<RecordProcessor.ActionSink> sinks) {
      this.processor = processor;
      this.sinks = List.copyOf(sinks);
    }

    @Override
    public void onBatch(List<SourceRecord> records) {
      for (SourceRecord record : records) {
        processor.process(InboundRecord.of(record));
      }
    }

    /**
     * Seals the open batches so actions of the revoked partitions are sent now rather than at
     * {@code max-age}; nothing waits for them. What completes before the source commits is
     * committed; later outcomes are ignored and the next owner re-reads those records (ADR-0025).
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      sinks.forEach(RecordProcessor.ActionSink::flush);
    }
  }
}
