package io.github.matabar.kafkafim.source.kafka;

import java.util.Collection;
import java.util.List;
import org.apache.kafka.common.TopicPartition;

/**
 * Receives one pipeline's records from its {@link KafkaPipelineSource} and reports completion
 * through {@link SourceRecord#complete()}. Implemented by the supervisor, which wires decoding,
 * routing, the ES sink and the DLQ (ADR-0003 keeps {@code source.kafka} free of those packages).
 *
 * <p>All methods run on a consumer thread: return quickly (hand bulk work to other threads) and do
 * not throw. If {@link #onBatch} throws, the source pauses the pipeline with reason {@value
 * KafkaPipelineSource#HANDLER_FAILURE_REASON} and the batch's incomplete records stay uncommitted
 * until the pipeline is restarted.
 */
public interface SourceRecordHandler {

  /**
   * One poll's records, in offset order per partition. Every record must eventually be completed,
   * or its partition revoked; oversize records ({@link SourceRecord#oversize()}) must not be
   * decoded. A record may already be {@linkplain SourceRecord#isAbandoned() abandoned} on arrival
   * (it came from a consumer that no longer owns its partition); completing it is a no-op, and it
   * may be skipped.
   *
   * @param records the records, unmodifiable
   */
  void onBatch(List<SourceRecord> records);

  /**
   * Partitions are about to be revoked (rebalance, or the source stopping). Called before the
   * source commits what completed for them; the handler may flush or abandon in-flight work for
   * them. Records of these partitions that are still incomplete afterwards are abandoned: their
   * later completions are ignored and the next owner re-reads them.
   *
   * @param partitions the revoked partitions that delivered records to this source
   */
  default void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

  /**
   * Partitions were lost (session timeout, fencing): nothing can be committed for them, and their
   * incomplete records are abandoned.
   *
   * @param partitions the lost partitions that delivered records to this source
   */
  default void onPartitionsLost(Collection<TopicPartition> partitions) {}
}
