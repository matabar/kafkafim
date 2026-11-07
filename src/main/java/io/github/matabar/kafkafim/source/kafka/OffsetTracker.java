package io.github.matabar.kafkafim.source.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Per-partition completion tracking for one pipeline (DESIGN §4.3, ADR-0008, ADR-0025).
 *
 * <p>Every delivered record is registered with its partition's {@link PartitionOffsets} and charged
 * to the pipeline's {@link ByteBudget}. A record leaves the tracker exactly once: completed (its
 * offset may become committable, its bytes are released) or abandoned on revoke (its bytes are
 * released, its offset is never committed here). The tracker computes commits; the consumer thread
 * performs them.
 */
final class OffsetTracker {

  private final ByteBudget budget;
  private final Map<TopicPartition, PartitionOffsets> partitions = new ConcurrentHashMap<>();
  private final Object idle = new Object();
  private long pending;

  OffsetTracker(ByteBudget budget) {
    this.budget = budget;
  }

  /** Prepares tracking for newly assigned partitions. */
  void assign(Collection<TopicPartition> assigned) {
    assigned.forEach(tp -> partitions.computeIfAbsent(tp, PartitionOffsets::new));
  }

  /**
   * Registers a delivered record.
   *
   * @param record the record as polled
   * @param maxRecordBytes values longer than this are flagged {@linkplain SourceRecord#oversize()
   *     oversize}
   * @return the record's completion handle
   */
  SourceRecord track(ConsumerRecord<byte[], byte[]> record, long maxRecordBytes) {
    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
    int valueBytes = record.value() == null ? 0 : record.value().length;
    int keyBytes = record.key() == null ? 0 : record.key().length;
    PartitionOffsets owner = partitions.computeIfAbsent(tp, PartitionOffsets::new);
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

  /**
   * Offsets that advanced since their last commit, for the given partitions.
   *
   * @param among partitions to consider (typically one consumer's assignment)
   * @return offsets to commit, possibly empty
   */
  Map<TopicPartition, OffsetAndMetadata> committable(Collection<TopicPartition> among) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
    for (TopicPartition tp : among) {
      PartitionOffsets state = partitions.get(tp);
      if (state != null) {
        long offset = state.committable();
        if (offset >= 0) {
          offsets.put(tp, new OffsetAndMetadata(offset));
        }
      }
    }
    return offsets;
  }

  /** Records successful commits, so they are not repeated. */
  void committed(Map<TopicPartition, OffsetAndMetadata> offsets) {
    offsets.forEach((tp, offset) -> {
      PartitionOffsets state = partitions.get(tp);
      if (state != null) {
        state.committed(offset.offset());
      }
    });
  }

  /** The partitions among {@code candidates} that are currently tracked. */
  List<TopicPartition> tracked(Collection<TopicPartition> candidates) {
    List<TopicPartition> tracked = new ArrayList<>();
    for (TopicPartition tp : candidates) {
      if (partitions.containsKey(tp)) {
        tracked.add(tp);
      }
    }
    return tracked;
  }

  /**
   * Stops tracking partitions: their pending records are abandoned (bytes released, offsets never
   * committed here) and later completions of those records are ignored.
   *
   * @return number of records abandoned
   */
  int revoke(Collection<TopicPartition> revoked) {
    int abandoned = 0;
    for (TopicPartition tp : revoked) {
      PartitionOffsets state = partitions.remove(tp);
      if (state == null) {
        continue;
      }
      for (SourceRecord record : state.close()) {
        if (record.abandon()) {
          finished(record, false);
          abandoned++;
        }
      }
    }
    return abandoned;
  }

  /** Revokes every tracked partition. */
  int revokeAll() {
    return revoke(List.copyOf(partitions.keySet()));
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
