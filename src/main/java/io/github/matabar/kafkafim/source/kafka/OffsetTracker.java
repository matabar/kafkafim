package io.github.matabar.kafkafim.source.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-partition completion tracking for one pipeline (DESIGN §4.3, ADR-0008, ADR-0025).
 *
 * <p>Every delivered record is registered with its partition's {@link PartitionOffsets} and charged
 * to the pipeline's {@link ByteBudget}. A record leaves the tracker exactly once: completed (its
 * offset may become committable, its bytes are released) or abandoned on revoke (its bytes are
 * released, its offset is never committed here). The tracker computes commits; the consumer thread
 * performs them.
 *
 * <p>State is owned per assignment: each {@link PartitionOffsets} belongs to the consumer it was
 * assigned to, and only that consumer's callbacks can commit, revoke or lose it. A partition that
 * moves between consumers of the same source (for example after one consumer missed its session
 * timeout) gets fresh state on the new consumer; the old state is closed, and its incomplete
 * records are abandoned, so a late {@code onPartitionsLost} on the old consumer cannot touch the
 * new owner's records.
 */
final class OffsetTracker {

  private static final Logger LOG = LoggerFactory.getLogger(OffsetTracker.class);
  private static final long UNOWNED_WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

  private final ByteBudget budget;
  private final AtomicLong lastUnownedWarn =
      new AtomicLong(System.nanoTime() - UNOWNED_WARN_INTERVAL_NANOS);
  private final Map<TopicPartition, PartitionOffsets> partitions = new ConcurrentHashMap<>();
  private final Object idle = new Object();
  private long pending;

  OffsetTracker(ByteBudget budget) {
    this.budget = budget;
  }

  /**
   * Installs fresh state for partitions newly assigned to a consumer; the only way state is
   * created. State left behind by a previous assignment (to any consumer) is closed and its
   * incomplete records abandoned.
   *
   * @param consumer the consumer the partitions were assigned to
   * @param assigned the newly assigned partitions
   */
  void assign(Object consumer, Collection<TopicPartition> assigned) {
    for (TopicPartition tp : assigned) {
      install(tp, consumer);
    }
  }

  private PartitionOffsets install(TopicPartition tp, Object consumer) {
    PartitionOffsets fresh = new PartitionOffsets(tp, consumer);
    PartitionOffsets previous = partitions.put(tp, fresh);
    if (previous != null) {
      abandon(previous);
    }
    return fresh;
  }

  private int abandon(PartitionOffsets state) {
    int abandoned = 0;
    for (SourceRecord record : state.close()) {
      if (record.abandon()) {
        finished(record, false);
        abandoned++;
      }
    }
    return abandoned;
  }

  /**
   * Registers a delivered record.
   *
   * @param consumer the consumer that polled it
   * @param record the record as polled
   * @param maxRecordBytes values longer than this are flagged {@linkplain SourceRecord#oversize()
   *     oversize}
   * @return the record's completion handle
   */
  SourceRecord track(Object consumer, ConsumerRecord<byte[], byte[]> record, long maxRecordBytes) {
    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
    int valueBytes = record.value() == null ? 0 : record.value().length;
    int keyBytes = record.key() == null ? 0 : record.key().length;
    PartitionOffsets owner = partitions.get(tp);
    if (owner == null || owner.consumer() != consumer) {
      // Only assign() installs state, from the rebalance callback where the group actually gave
      // the partition to this consumer. A record without owned state comes from a consumer that
      // lost the partition but does not know yet (a zombie, or a thread of a stopped container):
      // it must never take over the real owner's state. It is abandoned at once (bytes released,
      // completion ignored) and the partition's owner re-reads it.
      owner = new PartitionOffsets(tp, consumer);
      owner.close();
      warnUnowned(tp);
    }
    SourceRecord handle =
        new SourceRecord(
            record, valueBytes > maxRecordBytes, (long) valueBytes + keyBytes, owner, this);
    synchronized (idle) {
      pending++;
    }
    budget.acquire(handle.sizeBytes());
    if (!owner.add(handle) && handle.abandon()) {
      finished(handle, false);
    }
    return handle;
  }

  /** At most one warning per interval; records keep being abandoned in between. */
  private void warnUnowned(TopicPartition tp) {
    long now = System.nanoTime();
    long last = lastUnownedWarn.get();
    if (now - last >= UNOWNED_WARN_INTERVAL_NANOS && lastUnownedWarn.compareAndSet(last, now)) {
      LOG.warn(
          "Records for {} arrived on a consumer that does not own it; abandoning them for the"
              + " partition's owner to re-read (a stale consumer, or an assignment callback that"
              + " failed)",
          tp);
    }
  }

  /**
   * Called exactly once per record, by whoever completed or abandoned it.
   *
   * @param record the record
   * @param completed {@code true} when completed, {@code false} when abandoned
   */
  void finished(SourceRecord record, boolean completed) {
    if (completed) {
      record.owner().completed(record);
    }
    budget.release(record.sizeBytes());
    synchronized (idle) {
      pending--;
      if (pending == 0) {
        idle.notifyAll();
      }
    }
  }

  /** The state of {@code tp} if it belongs to {@code consumer}. */
  private PartitionOffsets ownedBy(Object consumer, TopicPartition tp) {
    PartitionOffsets state = partitions.get(tp);
    return state != null && state.consumer() == consumer ? state : null;
  }

  /**
   * Offsets that advanced since their last commit, for partitions the consumer owns.
   *
   * @param consumer the committing consumer
   * @param among partitions to consider (typically that consumer's assignment)
   * @return offsets to commit, possibly empty
   */
  Map<TopicPartition, OffsetAndMetadata> committable(
      Object consumer, Collection<TopicPartition> among) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
    for (TopicPartition tp : among) {
      PartitionOffsets state = ownedBy(consumer, tp);
      if (state != null) {
        long offset = state.committable();
        if (offset >= 0) {
          offsets.put(tp, new OffsetAndMetadata(offset));
        }
      }
    }
    return offsets;
  }

  /** Records successful commits by a consumer, so they are not repeated. */
  void committed(Object consumer, Map<TopicPartition, OffsetAndMetadata> offsets) {
    offsets.forEach((tp, offset) -> {
      PartitionOffsets state = ownedBy(consumer, tp);
      if (state != null) {
        state.committed(offset.offset());
      }
    });
  }

  /** The partitions among {@code candidates} whose current state belongs to the consumer. */
  List<TopicPartition> tracked(Object consumer, Collection<TopicPartition> candidates) {
    List<TopicPartition> tracked = new ArrayList<>();
    for (TopicPartition tp : candidates) {
      if (ownedBy(consumer, tp) != null) {
        tracked.add(tp);
      }
    }
    return tracked;
  }

  /**
   * Stops tracking partitions the consumer owns: their pending records are abandoned (bytes
   * released, offsets never committed here) and later completions of those records are ignored.
   * State of these partitions that now belongs to another consumer is left alone.
   *
   * @param consumer the consumer whose partitions were revoked or lost
   * @param revoked the partitions
   * @return number of records abandoned
   */
  int revoke(Object consumer, Collection<TopicPartition> revoked) {
    int abandoned = 0;
    for (TopicPartition tp : revoked) {
      PartitionOffsets state = ownedBy(consumer, tp);
      if (state != null && partitions.remove(tp, state)) {
        abandoned += abandon(state);
      }
    }
    return abandoned;
  }

  /** Drops all state, whoever owns it; for a source that stopped or is (re)starting. */
  int revokeAll() {
    int abandoned = 0;
    for (TopicPartition tp : List.copyOf(partitions.keySet())) {
      PartitionOffsets state = partitions.remove(tp);
      if (state != null) {
        abandoned += abandon(state);
      }
    }
    return abandoned;
  }

  /** Records delivered and neither completed nor abandoned. */
  long pendingRecords() {
    synchronized (idle) {
      return pending;
    }
  }

  /**
   * Waits until no record is pending.
   *
   * @return whether the tracker became idle within the timeout
   */
  boolean awaitIdle(Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    synchronized (idle) {
      while (pending > 0) {
        long left = deadline - System.nanoTime();
        if (left <= 0) {
          return false;
        }
        idle.wait(Math.max(1, left / 1_000_000));
      }
      return true;
    }
  }
}
