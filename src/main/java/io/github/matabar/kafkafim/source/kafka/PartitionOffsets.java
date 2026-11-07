package io.github.matabar.kafkafim.source.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.kafka.common.TopicPartition;

/**
 * Offsets of one partition during one assignment: the records delivered and not yet complete.
 *
 * <p>The committable offset (Kafka's "next offset to read") is the lowest incomplete offset, or one
 * past the highest delivered offset when nothing is pending. Gaps in the offset sequence (compacted
 * topics, transaction markers) need no special handling: only delivered offsets are ever pending.
 *
 * <p>An instance belongs to one assignment of the partition to one consumer ({@link #consumer()}).
 * A revoke or loss {@linkplain #close() closes} it; a later assignment of the same partition, to
 * the same or another consumer of the source, gets a new one, so completions of records from the
 * old assignment never touch the new state.
 */
final class PartitionOffsets {

  private static final long NONE = -1;

  private final TopicPartition partition;
  private final Object consumer;
  private final NavigableMap<Long, SourceRecord> pending = new TreeMap<>();
  private long nextAfterDelivered = NONE;
  private long committed = NONE;
  private boolean closed;

  PartitionOffsets(TopicPartition partition, Object consumer) {
    this.partition = partition;
    this.consumer = consumer;
  }

  /** The consumer this assignment belongs to. */
  Object consumer() {
    return consumer;
  }

  TopicPartition partition() {
    return partition;
  }

  /**
   * Registers a delivered record.
   *
   * @return whether it was registered; {@code false} once closed
   */
  synchronized boolean add(SourceRecord record) {
    if (closed) {
      return false;
    }
    pending.put(record.offset(), record);
    nextAfterDelivered = Math.max(nextAfterDelivered, record.offset() + 1);
    return true;
  }

  /** Removes a completed record; ignored once closed. */
  synchronized void completed(SourceRecord record) {
    if (!closed) {
      pending.remove(record.offset(), record);
    }
  }

  /**
   * The offset to commit, if it advanced past the last commit.
   *
   * @return the offset, or {@code -1} when there is nothing new to commit
   */
  synchronized long committable() {
    if (closed || nextAfterDelivered == NONE) {
      return NONE;
    }
    long offset = pending.isEmpty() ? nextAfterDelivered : pending.firstKey();
    return offset > committed ? offset : NONE;
  }

  /** Records a successful commit. */
  synchronized void committed(long offset) {
    committed = Math.max(committed, offset);
  }

  synchronized int pendingCount() {
    return pending.size();
  }

  /**
   * Closes the instance and hands back what was still pending.
   *
   * @return the pending records, lowest offset first
   */
  synchronized List<SourceRecord> close() {
    closed = true;
    List<SourceRecord> dropped = new ArrayList<>(pending.values());
    pending.clear();
    return dropped;
  }
}
