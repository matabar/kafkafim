package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.source.kafka.SourceRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The part of {@link SourceRecord} the {@link RecordProcessor} uses: the raw record and its
 * completion handle. A seam so the completion accounting is unit-testable without a consumer.
 */
interface InboundRecord {

  /** The raw record. */
  ConsumerRecord<byte[], byte[]> record();

  /** Whether the value exceeds {@code max-record-bytes}; such a record is never decoded. */
  boolean oversize();

  /** Declares how many {@link #complete()} calls complete the record; zero completes it now. */
  void expectParts(int parts);

  /** Completes one part. */
  void complete();

  /** Whether the record's partition was revoked or lost; completing it is then a no-op. */
  boolean isAbandoned();

  /** Adapts a source record. */
  static InboundRecord of(SourceRecord source) {
    return new InboundRecord() {
      @Override
      public ConsumerRecord<byte[], byte[]> record() {
        return source.record();
      }

      @Override
      public boolean oversize() {
        return source.oversize();
      }

      @Override
      public void expectParts(int parts) {
        source.expectParts(parts);
      }

      @Override
      public void complete() {
        source.complete();
      }

      @Override
      public boolean isAbandoned() {
        return source.isAbandoned();
      }

      @Override
      public String toString() {
        return source.toString();
      }
    };
  }
}
