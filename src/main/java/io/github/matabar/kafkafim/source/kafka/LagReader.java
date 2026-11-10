package io.github.matabar.kafkafim.source.kafka;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the consumer lag of a set of partitions with one extra consumer that never joins the group
 * (DESIGN §10). The consumer is created on first use and closed by {@link #close()}, which the
 * source calls when it stops; a later read creates a new one.
 *
 * <p>A stop must not wait for a lag read: {@link #close()} wakes the consumer up, so a request in
 * progress gives up at once. The consumer is published for that wake-up before the read checks
 * whether the source still runs, so a stop that begins while the consumer is being created either
 * sees it and wakes it, or the read sees the stop and returns without a broker request.
 */
final class LagReader {

  private static final Logger LOG = LoggerFactory.getLogger(LagReader.class);

  private final String pipeline;
  private final Supplier<Consumer<byte[], byte[]>> factory;
  private final BooleanSupplier running;
  private final Object lock = new Object();
  private @Nullable Consumer<byte[], byte[]> consumer; // guarded by lock
  private volatile @Nullable Consumer<byte[], byte[]> toWake;

  /**
   * Creates a reader without a consumer.
   *
   * @param pipeline the pipeline name, for logs
   * @param factory creates the lag consumer
   * @param running whether the source runs; must turn {@code false} (a volatile write) before the
   *     source calls {@link #close()} on stop
   */
  LagReader(String pipeline, Supplier<Consumer<byte[], byte[]>> factory, BooleanSupplier running) {
    this.pipeline = pipeline;
    this.factory = factory;
    this.running = running;
  }

  /**
   * Lag per partition: log end offset minus the committed offset. Partitions without a committed
   * offset are left out. One thread at a time gets the consumer; others wait for it.
   *
   * @param partitions the partitions to read, not empty
   * @param timeout bound on each of the two broker requests
   * @return lag per partition; empty when the source does not run (or stops during the read)
   * @throws KafkaException if the broker cannot answer in time, or a stop woke the read up
   */
  Map<TopicPartition, Long> read(Set<TopicPartition> partitions, Duration timeout) {
    synchronized (lock) {
      if (!running.getAsBoolean()) {
        return Map.of();
      }
      Consumer<byte[], byte[]> current = consumer;
      if (current == null) {
        current = factory.get();
        consumer = current;
        toWake = current;
        // A stop that began while the consumer was created did not see it to wake it up.
        if (!running.getAsBoolean()) {
          return Map.of();
        }
      }
      Map<TopicPartition, OffsetAndMetadata> committed = current.committed(partitions, timeout);
      Map<TopicPartition, Long> ends = current.endOffsets(partitions, timeout);
      Map<TopicPartition, Long> lag = new LinkedHashMap<>();
      for (Map.Entry<TopicPartition, Long> end : ends.entrySet()) {
        OffsetAndMetadata offset = committed.get(end.getKey());
        if (offset != null && end.getValue() != null) {
          lag.put(end.getKey(), Math.max(0, end.getValue() - offset.offset()));
        }
      }
      return lag;
    }
  }

  /** Wakes up a read in progress, then closes the consumer once the read gave up. */
  void close() {
    Consumer<byte[], byte[]> waking = toWake;
    if (waking != null) {
      waking.wakeup(); // a read in progress gives up instead of delaying the stop
    }
    synchronized (lock) {
      if (consumer != null) {
        try {
          consumer.close(Duration.ZERO);
        } catch (KafkaException e) {
          LOG.debug("Pipeline '{}': closing the lag consumer failed: {}", pipeline, e.toString());
        }
        consumer = null;
        toWake = null;
      }
    }
  }
}
