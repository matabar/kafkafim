package io.github.matabar.kafkafim.dlq;

import io.github.matabar.kafkafim.config.KafkaCluster;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * The DLQ producers of one bound configuration: one shared, thread-safe Kafka producer per {@code
 * (source cluster, DLQ identity)} (DESIGN §5.2.1), and one {@link DlqWriter} per pipeline on top of
 * it. Producers are created on a pipeline's first {@link #writer} call. A reload builds a new
 * instance from the new configuration and closes the old one once its pipelines have stopped; a
 * pipeline that leaves an instance others still use has its writer there {@linkplain #evict
 * evicted}.
 *
 * <p><b>Lifecycle.</b> As a Spring bean this is a {@link SmartLifecycle} in phase {@link #PHASE},
 * below the Kafka sources ({@code KafkaPipelineSource.PHASE}, {@code MAX_VALUE - 100}) and above
 * the web server ({@code MAX_VALUE - 1024}). Spring stops higher phases first, so the DLQ producers
 * outlive the source drain, during which completing in-flight records may still dead-letter
 * documents, and every component in a phase between the two. {@link #close()} rejects new sends,
 * waits up to {@link DlqOptions#closeTimeout()} for in-flight sends to be acknowledged, then
 * completes every send still unacknowledged (including those waiting to retry) with {@link
 * DlqClosedException}.
 */
public final class DlqProducers implements SmartLifecycle, AutoCloseable {

  /** Lifecycle phase: stops after the Kafka sources, before the web server (DESIGN §7). */
  public static final int PHASE = Integer.MAX_VALUE - 1000;

  private static final Logger log = LoggerFactory.getLogger(DlqProducers.class);

  private final KafkafimProperties properties;
  private final DlqListener listener;
  private final DlqOptions options;
  private final Function<Map<String, Object>, DlqClient> clients;
  private final Map<Key, SharedProducer> producers = new ConcurrentHashMap<>();
  private final Map<String, DlqWriter> writers = new ConcurrentHashMap<>();
  private final Set<DlqWriter.Send> pending = ConcurrentHashMap.newKeySet();
  private final ExecutorService executor =
      Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("kafkafim-dlq-", 0).factory());
  private final ScheduledExecutorService scheduler;
  private final Object closeLock = new Object();
  private volatile boolean closed;
  private volatile boolean running;

  /**
   * Producers over a bound configuration, with the default timings.
   *
   * @param properties the validated configuration
   * @param listener metrics and supervisor hook
   */
  public DlqProducers(KafkafimProperties properties, DlqListener listener) {
    this(properties, listener, DlqOptions.DEFAULTS);
  }

  /**
   * Producers over a bound configuration.
   *
   * @param properties the validated configuration
   * @param listener metrics and supervisor hook
   * @param options timings
   */
  public DlqProducers(KafkafimProperties properties, DlqListener listener, DlqOptions options) {
    this(properties, listener, options, KafkaDlqClient::new);
  }

  DlqProducers(
      KafkafimProperties properties,
      DlqListener listener,
      DlqOptions options,
      Function<Map<String, Object>, DlqClient> clients) {
    this.properties = properties;
    this.listener = listener;
    this.options = options;
    this.clients = clients;
    ScheduledThreadPoolExecutor timer =
        new ScheduledThreadPoolExecutor(
            1, Thread.ofPlatform().name("kafkafim-dlq-retry").daemon().factory());
    timer.setRemoveOnCancelPolicy(true);
    this.scheduler = timer;
  }

  /**
   * The writer of a pipeline, creating the shared producer of its {@code (source cluster, DLQ
   * identity)} on first use.
   *
   * @param pipelineName a {@code kafkafim.pipelines} entry
   * @return the pipeline's writer, the same instance on every call
   * @throws IllegalArgumentException if the pipeline, its cluster or its DLQ identity is unknown,
   *     or the cluster's passthrough sets a key the DLQ producer owns
   * @throws IllegalStateException if closed
   * @throws org.apache.kafka.common.KafkaException if kafka-clients rejects the settings
   */
  public DlqWriter writer(String pipelineName) {
    ensureOpen();
    return writers.computeIfAbsent(pipelineName, this::createWriter);
  }

  /**
   * Describes a pipeline's DLQ topic with its DLQ identity, for the supervisor's preflight
   * (ADR-0021: the topic must exist; kafkafim never creates it). Blocks up to {@link
   * DlqOptions#topicCheckTimeout()}.
   *
   * @param pipelineName a {@code kafkafim.pipelines} entry
   * @return what the broker said
   * @throws IllegalArgumentException as {@link #writer}
   * @throws IllegalStateException if closed
   */
  public TopicStatus checkTopic(String pipelineName) {
    DlqWriter writer = writer(pipelineName);
    return producerFor(pipeline(pipelineName)).check(writer.topic(), options.topicCheckTimeout());
  }

  /**
   * Drops a pipeline's writer, for a pipeline that moved to other DLQ producers or was removed
   * (ADR-0035). Its unacknowledged sends, including those waiting to retry, complete with {@link
   * DlqClosedException} (their source records are re-delivered to the pipeline's next run), later
   * sends through it fail the same way, and it reports nothing more to the listener. The shared
   * producer stays until {@link #close()}. A later {@link #writer} call creates a new writer. No-op
   * if the pipeline has no writer.
   *
   * @param pipelineName a {@code kafkafim.pipelines} entry
   */
  public void evict(String pipelineName) {
    DlqWriter writer = writers.remove(pipelineName);
    if (writer == null) {
      return;
    }
    writer.evict();
    List<DlqWriter.Send> unacknowledged = new ArrayList<>();
    for (DlqWriter.Send send : pending) {
      if (send.writer() == writer) {
        unacknowledged.add(send);
      }
    }
    log.info(
        "Dropped the DLQ writer of pipeline '{}' to topic '{}'; {} unacknowledged DLQ record(s)"
            + " will be re-delivered to its next run",
        pipelineName,
        writer.topic(),
        unacknowledged.size());
    unacknowledged.forEach(DlqWriter.Send::abandon);
  }

  /** Number of distinct producers created so far. */
  int size() {
    return producers.size();
  }

  private DlqWriter createWriter(String pipelineName) {
    Pipeline pipeline = pipeline(pipelineName);
    String topic = pipeline.dlq().topic();
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("Pipeline '" + pipelineName + "' has no DLQ topic");
    }
    return new DlqWriter(pipelineName, topic, producerFor(pipeline), this);
  }

  private Pipeline pipeline(String pipelineName) {
    Pipeline pipeline = properties.pipelines().get(pipelineName);
    if (pipeline == null) {
      throw new IllegalArgumentException("Unknown pipeline '" + pipelineName + "'");
    }
    return pipeline;
  }

  private SharedProducer producerFor(Pipeline pipeline) {
    Key key = new Key(pipeline.source().cluster(), pipeline.dlq().identity());
    boolean[] created = {false};
    SharedProducer producer =
        producers.computeIfAbsent(
            key,
            k -> {
              created[0] = true;
              return createProducer(k);
            });
    if (closed && created[0]) {
      // close() may have iterated before this producer was added. Only a producer created here is
      // closed: an existing one is being flushed by close() itself.
      producer.client().close(Duration.ZERO);
      throw new IllegalStateException("DLQ producers are closed");
    }
    return producer;
  }

  private SharedProducer createProducer(Key key) {
    KafkaCluster cluster = properties.kafka().clusters().get(key.cluster());
    if (cluster == null) {
      throw new IllegalArgumentException("Unknown Kafka cluster '" + key.cluster() + "'");
    }
    KafkaCluster.KafkaIdentity identity = null;
    if (key.identity() != null) {
      identity = cluster.identities().get(key.identity());
      if (identity == null) {
        throw new IllegalArgumentException(
            "Unknown identity '" + key.identity() + "' on Kafka cluster '" + key.cluster() + "'");
      }
    }
    Map<String, Object> settings =
        ProducerSettings.forIdentity(key.cluster(), cluster, key.identity(), identity);
    return new SharedProducer(key.cluster(), key.identity(), clients.apply(settings));
  }

  // --- used by DlqWriter -------------------------------------------------------------------

  DlqOptions options() {
    return options;
  }

  boolean isClosed() {
    return closed;
  }

  /** Tracks a send until it is acknowledged or abandoned; {@code false} once closed. */
  boolean register(DlqWriter.Send send) {
    synchronized (closeLock) {
      if (closed) {
        return false;
      }
      pending.add(send);
      return true;
    }
  }

  void unregister(DlqWriter.Send send) {
    pending.remove(send);
  }

  /** Runs {@code task} on a DLQ thread, or {@code onRejected} inline once the threads are gone. */
  void dispatch(Runnable task, Runnable onRejected) {
    try {
      executor.execute(task);
    } catch (RejectedExecutionException e) {
      onRejected.run();
    }
  }

  /** Retries a send after {@code delay}; abandons it once closed. */
  void schedule(DlqWriter.Send send, Duration delay) {
    try {
      scheduler.schedule(
          () -> dispatch(send::attempt, send::abandon), delay.toMillis(), TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      send.abandon();
    }
  }

  /** Calls the listener, logging and swallowing what it throws. */
  void notify(Consumer<DlqListener> call) {
    try {
      call.accept(listener);
    } catch (RuntimeException e) {
      log.warn("DLQ listener failed", e);
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("DLQ producers are closed");
    }
  }

  // --- lifecycle ---------------------------------------------------------------------------

  @Override
  public void start() {
    running = true;
  }

  @Override
  public void stop() {
    close();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return PHASE;
  }

  /**
   * Rejects new sends, flushes in-flight ones for up to {@link DlqOptions#closeTimeout()} and
   * closes the producers; every send still unacknowledged then completes with {@link
   * DlqClosedException}. Idempotent.
   */
  @Override
  public void close() {
    synchronized (closeLock) {
      if (closed) {
        return;
      }
      closed = true;
    }
    running = false;
    scheduler.shutdownNow();
    long deadline = System.nanoTime() + options.closeTimeout().toNanos();
    for (SharedProducer producer : producers.values()) {
      try {
        producer.client().close(remaining(deadline));
      } catch (RuntimeException e) {
        log.warn("Closing the DLQ producer ({}) failed", producer.describeIdentity(), e);
      }
    }
    List<DlqWriter.Send> unacknowledged = new ArrayList<>(pending);
    if (!unacknowledged.isEmpty()) {
      log.warn(
          "{} DLQ record(s) not acknowledged at close; their source records will be re-delivered",
          unacknowledged.size());
    }
    unacknowledged.forEach(DlqWriter.Send::abandon);
    executor.shutdown();
    try {
      if (!executor.awaitTermination(remaining(deadline).toMillis() + 1, TimeUnit.MILLISECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private static Duration remaining(long deadlineNanos) {
    return Duration.ofNanos(Math.max(0, deadlineNanos - System.nanoTime()));
  }

  private record Key(String cluster, @Nullable String identity) {}
}
