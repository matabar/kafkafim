package io.github.matabar.kafkafim.source.kafka;

import io.github.matabar.kafkafim.config.Pipeline;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.event.ConcurrentContainerStoppedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener;
import org.springframework.kafka.listener.BatchInterceptor;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * The Kafka source of one pipeline (DESIGN §4.2, §4.3; ADR-0004, ADR-0025): a programmatically
 * created {@link ConcurrentMessageListenerContainer} with a batch listener, the cooperative-sticky
 * assignor, concurrency capped at the partition count, kafkafim's own per-partition {@link
 * OffsetTracker} and a pause/resume byte budget.
 *
 * <p>Commits happen only on consumer threads: every {@code commit-interval} before a poll (polls
 * continue while paused, so completions during a pause are committed too), synchronously when
 * partitions are revoked, and when the source stops. The container's own acknowledgement machinery
 * is not used ({@code AckMode.MANUAL}, never acknowledged).
 *
 * <p>Stopping is graceful and bounded (DESIGN §7, SIGTERM): fetching pauses, in-flight records get
 * {@link SourceOptions#drainTimeout()} to complete, then the container stops, which revokes every
 * partition (committing what completed) and leaves the group. The stop returns once every consumer
 * thread has finished, or after {@link SourceOptions#shutdownTimeout()}, also when the container
 * had already stopped itself. The source runs in lifecycle phase {@link #PHASE}.
 *
 * <p>A failure the source cannot recover from (the handler threw, or a consumer stopped other than
 * by {@link #stop()}) is reported to {@link SourceRecordHandler#onSourceFailed}; stopping the
 * source is then up to its owner.
 */
public final class KafkaPipelineSource implements SmartLifecycle {

  /** Lifecycle phase: stops before the web server (DESIGN §7). */
  public static final int PHASE = Integer.MAX_VALUE - 100;

  /** Pause reason while in-flight bytes are at or above {@code max-in-flight-bytes}. */
  public static final String BUDGET_REASON = "in-flight-bytes";

  /** Pause reason after the handler threw. */
  public static final String HANDLER_FAILURE_REASON = "handler-failure";

  /** Pause reason while the source drains on stop. */
  public static final String STOPPING_REASON = "stopping";

  private static final Set<String> RESERVED_REASONS =
      Set.of(BUDGET_REASON, HANDLER_FAILURE_REASON, STOPPING_REASON);

  private static final Logger LOG = LoggerFactory.getLogger(KafkaPipelineSource.class);

  private final String pipeline;
  private final Pipeline.Source source;
  private final ConsumerFactory<byte[], byte[]> consumerFactory;
  private final SourceRecordHandler handler;
  private final SourceOptions options;
  private final String clientId;
  private final Duration pollTimeout;
  private final PauseReasons pauseReasons;
  private final ByteBudget budget;
  private final OffsetTracker tracker;
  private final Map<Consumer<?, ?>, Long> lastCommitNanos = new ConcurrentHashMap<>();
  private final Object lifecycle = new Object();
  private volatile @Nullable ConcurrentMessageListenerContainer<byte[], byte[]> container;
  // Counted down when every consumer thread of the current container has finished.
  private volatile CountDownLatch consumersStopped = new CountDownLatch(0);
  private volatile int concurrency;

  /**
   * Creates a stopped source.
   *
   * @param pipeline the pipeline name (client ids are {@code kafkafim-<pipeline>-<n>})
   * @param source the pipeline's resolved source block (group id and identity already defaulted)
   * @param consumerFactory the factory for the source's {@code (cluster, identity)}, from {@link
   *     ConsumerFactories}
   * @param handler receives the records
   * @param options runtime knobs
   */
  public KafkaPipelineSource(
      String pipeline,
      Pipeline.Source source,
      ConsumerFactory<byte[], byte[]> consumerFactory,
      SourceRecordHandler handler,
      SourceOptions options) {
    this.pipeline = pipeline;
    this.source = source;
    this.consumerFactory = consumerFactory;
    this.handler = handler;
    this.options = options;
    this.clientId = "kafkafim-" + pipeline;
    this.pollTimeout =
        source.commitInterval().compareTo(options.maxPollTimeout()) < 0
            ? source.commitInterval()
            : options.maxPollTimeout();
    this.pauseReasons = new PauseReasons(new ContainerTarget());
    long max = source.maxInFlightBytes().toBytes();
    this.budget =
        new ByteBudget(
            max,
            (long) (max * options.resumeFraction()),
            exhausted -> {
              if (exhausted) {
                pauseReasons.add(BUDGET_REASON);
              } else {
                pauseReasons.remove(BUDGET_REASON);
              }
            });
    this.tracker = new OffsetTracker(budget);
  }

  /**
   * The container concurrency for a desired value and a partition count: never above the partition
   * count (DESIGN §4.1), never below one.
   *
   * @param desired consumers wanted
   * @param partitions partitions of all the pipeline's topics together
   * @return the concurrency to use
   */
  static int effectiveConcurrency(int desired, int partitions) {
    return Math.max(1, Math.min(desired, partitions));
  }

  /**
   * Reads topic metadata, sizes and starts the container.
   *
   * <p>The container creates its consumers one after another on the calling thread. If one cannot
   * be created, those already started are stopped (committing what completed and leaving the group)
   * before the failure is rethrown, so a source that failed to start does not keep consuming.
   *
   * @throws MissingTopicsException if a source topic does not exist or is not visible
   * @throws KafkaException if a consumer cannot be created
   */
  @Override
  public void start() {
    synchronized (lifecycle) {
      if (isRunning()) {
        return;
      }
      int partitions = partitionCount();
      concurrency = effectiveConcurrency(options.desiredConcurrency(), partitions);
      if (options.desiredConcurrency() > partitions) {
        LOG.warn(
            "Pipeline '{}': desired concurrency {} capped at the {} partitions of {}",
            pipeline,
            options.desiredConcurrency(),
            partitions,
            source.topics());
      }
      // State left by a container that stopped on its own (no revoke callback ran) is stale.
      tracker.revokeAll();
      lastCommitNanos.clear();
      CountDownLatch stopped = new CountDownLatch(1);
      ConcurrentMessageListenerContainer<byte[], byte[]> created = createContainer(stopped);
      consumersStopped = stopped;
      pauseReasons.remove(HANDLER_FAILURE_REASON);
      container = created;
      pauseReasons.apply();
      boolean started = false;
      try {
        created.start();
        started = true;
      } finally {
        if (!started) {
          abandonStart(created);
        }
      }
      LOG.info(
          "Pipeline '{}': consuming {} as group '{}' with concurrency {}",
          pipeline,
          source.topics(),
          source.groupId(),
          concurrency);
    }
  }

  /**
   * Pauses fetching, waits up to the drain timeout for in-flight records, then stops the container
   * (revoking, committing and leaving the group) and waits for its consumer threads, up to the
   * shutdown timeout. Records still incomplete are re-delivered to the next owner.
   */
  @Override
  public void stop() {
    synchronized (lifecycle) {
      ConcurrentMessageListenerContainer<byte[], byte[]> running = container;
      if (running == null) {
        return;
      }
      pauseReasons.add(STOPPING_REASON);
      try {
        if (!tracker.awaitIdle(options.drainTimeout())) {
          LOG.warn(
              "Pipeline '{}': {} records still in flight after {}; they will be re-delivered",
              pipeline,
              tracker.pendingRecords(),
              options.drainTimeout());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      try {
        stopContainer(running);
      } finally {
        pauseReasons.remove(STOPPING_REASON);
      }
      LOG.info("Pipeline '{}': stopped", pipeline);
    }
  }

  /**
   * Stops asynchronously, so that the lifecycle processor drains every pipeline of the phase in
   * parallel within one {@code timeout-per-shutdown-phase} instead of one after another.
   *
   * @param callback run once this source has stopped
   */
  @Override
  public void stop(Runnable callback) {
    Thread.ofVirtual()
        .name(clientId + "-stop")
        .start(() -> {
          try {
            stop();
          } catch (RuntimeException e) {
            LOG.error("Pipeline '{}': stop failed", pipeline, e);
          } finally {
            callback.run();
          }
        });
  }

  /**
   * Stops the container and waits for its consumer threads, up to the shutdown timeout; the source
   * is stopped afterwards, also if the container's stop throws. Under the lifecycle lock.
   */
  private void stopContainer(ConcurrentMessageListenerContainer<byte[], byte[]> running) {
    long deadline = System.nanoTime() + options.shutdownTimeout().toNanos();
    try {
      running.stop();
      awaitConsumers(deadline);
    } finally {
      tracker.revokeAll();
      lastCommitNanos.clear();
      container = null;
    }
  }

  /**
   * Stops a container whose start threw. The parent container counts as running from before its
   * first consumer, and the consumers created before the failing one start polling once the start
   * has returned: without this they would stay in the group with no owner, and the source would
   * report itself running. A failure of the stop is logged, so the start's own failure is the one
   * that propagates.
   */
  private void abandonStart(ConcurrentMessageListenerContainer<byte[], byte[]> created) {
    if (neverRan(created)) {
      // It threw before any consumer existed: nothing to stop, and nothing would report stopped.
      container = null;
      return;
    }
    LOG.warn("Pipeline '{}': the container failed to start; stopping its consumers", pipeline);
    try {
      stopContainer(created);
    } catch (RuntimeException e) {
      LOG.warn("Pipeline '{}': stopping the half-started container failed", pipeline, e);
    }
  }

  /**
   * Whether a container whose start threw never had a consumer. "Not running" alone does not say
   * so: a first consumer that hits an {@link Error} or a fatal error as soon as the failed start
   * returns stops the container itself, and its thread may still be wrapping up. The container
   * keeps its consumers listed until the last of them has reported stopped, so one that is not
   * running and lists none has nothing left to wait for.
   *
   * @param created a container whose start threw
   * @return {@code true} if there is neither a consumer to stop nor one to wait for
   */
  static boolean neverRan(ConcurrentMessageListenerContainer<?, ?> created) {
    return !created.isRunning() && created.getContainers().isEmpty();
  }

  /**
   * Waits for the consumer threads. {@code stop()} of a container that stopped itself (a consumer
   * hit an {@link Error} or a fatal error, ADR-0036) returns at once, while its other consumers may
   * still be revoking (flushing the sinks, committing): the sinks must not close under them.
   *
   * <p>The signal is the parent's {@link ConcurrentContainerStoppedEvent}. It can come early if a
   * consumer stops itself within microseconds of the start, before a sibling has registered with
   * the parent; it never comes if a consumer's wrap-up throws before it reports, and the stop then
   * waits the full shutdown timeout. Both are rare, and the stop stays bounded.
   */
  private void awaitConsumers(long deadline) {
    try {
      if (!consumersStopped.await(
          Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
        LOG.warn(
            "Pipeline '{}': Kafka consumer threads did not report stopped within {}",
            pipeline,
            options.shutdownTimeout());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * {@code false}: the supervisor starts each source explicitly, so a {@link
   * MissingTopicsException} fails one pipeline instead of the application context. Stopping on
   * shutdown still goes through the lifecycle phase.
   */
  @Override
  public boolean isAutoStartup() {
    return false;
  }

  @Override
  public boolean isRunning() {
    ConcurrentMessageListenerContainer<byte[], byte[]> running = container;
    return running != null && running.isRunning();
  }

  @Override
  public int getPhase() {
    return PHASE;
  }

  /**
   * Pauses fetching on all of the pipeline's partitions for a reason (e.g. a breaker or an auth
   * failure). Fetching resumes once every reason is cleared. Commits continue while paused.
   *
   * @param reason a reason other than the reserved {@link #BUDGET_REASON}, {@link
   *     #HANDLER_FAILURE_REASON} and {@link #STOPPING_REASON}
   */
  public void pause(String reason) {
    pauseReasons.add(checkReason(reason));
  }

  /**
   * Clears a pause reason set with {@link #pause(String)}.
   *
   * @param reason the reason
   */
  public void resume(String reason) {
    pauseReasons.remove(checkReason(reason));
  }

  /** The active pause reasons, including the reserved ones. */
  public Set<String> pauseReasons() {
    return pauseReasons.active();
  }

  /** The pipeline name. */
  public String pipeline() {
    return pipeline;
  }

  /** The container concurrency chosen at the last start, or 0 before the first start. */
  public int concurrency() {
    return concurrency;
  }

  /** Partitions currently assigned to this source's consumers. */
  public Collection<TopicPartition> assignedPartitions() {
    ConcurrentMessageListenerContainer<byte[], byte[]> running = container;
    if (running == null) {
      return List.of();
    }
    Collection<TopicPartition> assigned = running.getAssignedPartitions();
    return assigned == null ? List.of() : List.copyOf(assigned);
  }

  /** Bytes delivered to the handler and not yet completed or abandoned. */
  public long inFlightBytes() {
    return budget.outstanding();
  }

  /** Records delivered to the handler and not yet completed or abandoned. */
  public long inFlightRecords() {
    return tracker.pendingRecords();
  }

  /**
   * Whether every consumer of the running container holds at least one broker connection (kafka
   * clients' {@code connection-count}), for readiness and {@code kafkafim_kafka_connected}. {@code
   * false} while stopped or before the consumers exist.
   */
  public boolean kafkaConnected() {
    ConcurrentMessageListenerContainer<byte[], byte[]> running = container;
    if (running == null || !running.isRunning()) {
      return false;
    }
    Map<String, Map<MetricName, ? extends Metric>> metrics = running.metrics();
    if (metrics.isEmpty()) {
      return false;
    }
    for (Map<MetricName, ? extends Metric> client : metrics.values()) {
      if (!hasConnection(client)) {
        return false;
      }
    }
    return true;
  }

  private static boolean hasConnection(Map<MetricName, ? extends Metric> client) {
    for (Map.Entry<MetricName, ? extends Metric> metric : client.entrySet()) {
      MetricName name = metric.getKey();
      if ("connection-count".equals(name.name()) && "consumer-metrics".equals(name.group())) {
        return metric.getValue().metricValue() instanceof Number count && count.doubleValue() > 0;
      }
    }
    return false;
  }

  /** Whether the container's consumers are actually paused (not just requested). */
  public boolean isConsumerPaused() {
    ConcurrentMessageListenerContainer<byte[], byte[]> running = container;
    return running != null && running.isContainerPaused();
  }

  private static String checkReason(String reason) {
    if (reason == null || reason.isBlank() || RESERVED_REASONS.contains(reason)) {
      throw new IllegalArgumentException("Not a usable pause reason: " + reason);
    }
    return reason;
  }

  private int partitionCount() {
    try (Consumer<byte[], byte[]> metadata =
        consumerFactory.createConsumer(
            null, clientId + "-metadata", null, ConsumerSettings.forPipeline(source))) {
      int total = 0;
      List<String> missing = new ArrayList<>();
      for (String topic : source.topics()) {
        List<PartitionInfo> infos = metadata.partitionsFor(topic, options.metadataTimeout());
        if (infos == null || infos.isEmpty()) {
          missing.add(topic);
        } else {
          total += infos.size();
        }
      }
      if (!missing.isEmpty()) {
        throw new MissingTopicsException(pipeline, missing);
      }
      return total;
    }
  }

  private ConcurrentMessageListenerContainer<byte[], byte[]> createContainer(
      CountDownLatch stopped) {
    ContainerProperties properties =
        new ContainerProperties(source.topics().toArray(String[]::new));
    properties.setGroupId(source.groupId());
    properties.setClientId(clientId);
    properties.setAckMode(ContainerProperties.AckMode.MANUAL);
    properties.setKafkaConsumerProperties(ConsumerSettings.forPipeline(source));
    properties.setConsumerRebalanceListener(new Rebalance());
    properties.setMessageListener(new Listener());
    properties.setPollTimeout(pollTimeout.toMillis());
    properties.setShutdownTimeout(options.shutdownTimeout().toMillis());
    properties.setAuthExceptionRetryInterval(options.authRetryInterval());
    ConcurrentMessageListenerContainer<byte[], byte[]> created =
        new ConcurrentMessageListenerContainer<>(consumerFactory, properties);
    created.setConcurrency(concurrency);
    created.setBatchInterceptor(new CommitBeforePoll());
    // The listener never throws to the container; if something still does, stop rather than skip.
    created.setCommonErrorHandler(new CommonContainerStoppingErrorHandler());
    // To hear of consumers that stop on their own; the children inherit the publisher. With a
    // publisher set, a stopping child also notifies its parent (childStopped); harmless here, since
    // every start builds a new container and restartAfterAuthExceptions stays off. Once the parent
    // is stopped and its last child has finished, the parent publishes
    // ConcurrentContainerStoppedEvent: every consumer thread is done (stop() waits for it).
    created.setApplicationEventPublisher(event -> {
      if (event instanceof ConcurrentContainerStoppedEvent) {
        stopped.countDown();
      } else {
        onContainerEvent(event);
      }
    });
    created.setBeanName(clientId);
    return created;
  }

  /**
   * A consumer that stops for any reason but a normal stop (an error the container does not retry,
   * or an {@link Error} from the handler) leaves its partitions unconsumed: report it.
   */
  private void onContainerEvent(Object event) {
    if (event instanceof ConsumerStoppedEvent stopped
        && stopped.getReason() != ConsumerStoppedEvent.Reason.NORMAL) {
      reportFailure("Kafka consumer stopped (" + stopped.getReason() + ")");
    }
  }

  private void reportFailure(String cause) {
    try {
      handler.onSourceFailed(cause);
    } catch (RuntimeException e) {
      LOG.warn("Pipeline '{}': failure handler failed", pipeline, e);
    }
  }

  /** Commits what advanced, if {@code commit-interval} elapsed for this consumer. */
  private void commitIfDue(Consumer<?, ?> consumer) {
    long now = System.nanoTime();
    Long last = lastCommitNanos.get(consumer);
    if (last != null && now - last < source.commitInterval().toNanos()) {
      return;
    }
    lastCommitNanos.put(consumer, now);
    Map<TopicPartition, OffsetAndMetadata> offsets =
        tracker.committable(consumer, consumer.assignment());
    if (offsets.isEmpty()) {
      return;
    }
    consumer.commitAsync(
        offsets,
        (committed, exception) -> {
          if (exception == null) {
            tracker.committed(consumer, committed);
          } else {
            LOG.warn(
                "Pipeline '{}': offset commit failed, retrying next interval: {}",
                pipeline,
                exception.toString());
          }
        });
  }

  /** Synchronously commits what completed for partitions about to be revoked. */
  private void commitNow(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
    Map<TopicPartition, OffsetAndMetadata> offsets = tracker.committable(consumer, partitions);
    if (offsets.isEmpty()) {
      return;
    }
    try {
      consumer.commitSync(offsets, options.commitTimeout());
      tracker.committed(consumer, offsets);
    } catch (KafkaException e) {
      LOG.warn(
          "Pipeline '{}': commit on revoke failed; the next owner re-reads from the last commit:"
              + " {}",
          pipeline,
          e.toString());
    }
  }

  private final class Listener implements BatchConsumerAwareMessageListener<byte[], byte[]> {

    @Override
    public void onMessage(
        List<ConsumerRecord<byte[], byte[]>> data, @Nullable Consumer<?, ?> consumer) {
      Objects.requireNonNull(consumer, "consumer-aware listener without a consumer");
      long maxRecordBytes = source.maxRecordBytes().toBytes();
      List<SourceRecord> batch = new ArrayList<>(data.size());
      for (ConsumerRecord<byte[], byte[]> record : data) {
        batch.add(tracker.track(consumer, record, maxRecordBytes));
      }
      try {
        handler.onBatch(Collections.unmodifiableList(batch));
      } catch (RuntimeException e) {
        LOG.error(
            "Pipeline '{}': record handler failed; pausing until the pipeline is restarted",
            pipeline,
            e);
        pauseReasons.add(HANDLER_FAILURE_REASON);
        reportFailure("record handler failed: " + e);
      }
    }
  }

  private final class Rebalance implements ConsumerAwareRebalanceListener {

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> assigned) {
      tracker.assign(consumer, assigned);
    }

    @Override
    public void onPartitionsRevokedBeforeCommit(
        Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
      List<TopicPartition> tracked = tracker.tracked(consumer, partitions);
      if (tracked.isEmpty()) {
        return;
      }
      try {
        handler.onPartitionsRevoked(tracked);
      } catch (RuntimeException e) {
        LOG.error("Pipeline '{}': revoke handler failed", pipeline, e);
      }
      commitNow(consumer, tracked);
      int abandoned = tracker.revoke(consumer, tracked);
      LOG.info(
          "Pipeline '{}': revoked {}, {} incomplete records left for the next owner",
          pipeline,
          tracked,
          abandoned);
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
      List<TopicPartition> tracked = tracker.tracked(consumer, partitions);
      if (tracked.isEmpty()) {
        return;
      }
      try {
        handler.onPartitionsLost(tracked);
      } catch (RuntimeException e) {
        LOG.error("Pipeline '{}': lost-partitions handler failed", pipeline, e);
      }
      int abandoned = tracker.revoke(consumer, tracked);
      LOG.warn(
          "Pipeline '{}': lost {}, {} incomplete records left for the next owner",
          pipeline,
          tracked,
          abandoned);
    }
  }

  /** Runs before every poll on each consumer thread, including while paused or idle. */
  private final class CommitBeforePoll implements BatchInterceptor<byte[], byte[]> {

    @Override
    public ConsumerRecords<byte[], byte[]> intercept(
        ConsumerRecords<byte[], byte[]> records, Consumer<byte[], byte[]> consumer) {
      return records;
    }

    @Override
    public void setupThreadState(Consumer<?, ?> consumer) {
      commitIfDue(consumer);
    }
  }

  private final class ContainerTarget implements PauseReasons.Target {

    @Override
    public void pause() {
      ConcurrentMessageListenerContainer<byte[], byte[]> running = container;
      if (running != null) {
        running.pause();
      }
    }

    @Override
    public void resume() {
      ConcurrentMessageListenerContainer<byte[], byte[]> running = container;
      if (running != null) {
        running.resume();
      }
    }
  }
}
