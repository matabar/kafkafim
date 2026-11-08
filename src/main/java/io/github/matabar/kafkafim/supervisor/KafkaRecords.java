package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.dlq.DlqRecordFormat;
import io.github.matabar.kafkafim.route.HeaderLookup;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

/** Adapters from kafka-clients types to what {@code route} and {@code decode} take. */
final class KafkaRecords {

  private KafkaRecords() {}

  /** The {@link HeaderLookup} over a record's headers: the last header of a name wins. */
  static HeaderLookup headers(Headers headers) {
    return name -> {
      Header header = headers.lastHeader(name);
      return header == null ? null : header.value();
    };
  }

  /**
   * The ingest time of a record (DESIGN §4.5): its Kafka timestamp, or the clock when it has none.
   * When {@code document.timestamp-field} is unset and the record carries {@code
   * kafkafim.source.timestamp} (it was read from a DLQ topic, ADR-0027), that original timestamp is
   * the ingest time instead, so a replay pipeline resolves {@code %{+...}} patterns to the original
   * date. A header that does not parse as an ISO-8601 instant is ignored.
   *
   * @param record the Kafka record
   * @param timestampFieldSet whether the pipeline reads event time from a field
   * @param clock fallback for records without a timestamp
   * @return the ingest time
   */
  static Instant ingestTime(
      ConsumerRecord<byte[], byte[]> record, boolean timestampFieldSet, Clock clock) {
    if (!timestampFieldSet) {
      Instant original = sourceTimestamp(record.headers());
      if (original != null) {
        return original;
      }
    }
    return record.timestamp() >= 0 ? Instant.ofEpochMilli(record.timestamp()) : clock.instant();
  }

  private static @Nullable Instant sourceTimestamp(Headers headers) {
    Header header = headers.lastHeader(DlqRecordFormat.SOURCE_TIMESTAMP);
    if (header == null || header.value() == null) {
      return null;
    }
    try {
      return Instant.parse(new String(header.value(), StandardCharsets.UTF_8));
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  /** The record's Kafka timestamp, or {@code null} when it has none. */
  static @Nullable Instant timestamp(ConsumerRecord<byte[], byte[]> record) {
    return record.timestamp() >= 0 ? Instant.ofEpochMilli(record.timestamp()) : null;
  }
}
