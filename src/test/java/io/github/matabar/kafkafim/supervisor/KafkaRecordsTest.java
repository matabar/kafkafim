package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.dlq.DlqRecordFormat;
import io.github.matabar.kafkafim.route.HeaderLookup;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class KafkaRecordsTest {

  private static final Instant NOW = Instant.parse("2026-11-08T12:00:00Z");
  private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
  private static final Instant RECORD_TIME = Instant.parse("2026-03-04T05:06:07Z");
  private static final Instant ORIGINAL = Instant.parse("2025-12-31T23:59:59.123Z");

  private static ConsumerRecord<byte[], byte[]> record(long timestamp, RecordHeaders headers) {
    return new ConsumerRecord<>(
        "t",
        0,
        0,
        timestamp,
        timestamp < 0 ? TimestampType.NO_TIMESTAMP_TYPE : TimestampType.CREATE_TIME,
        0,
        0,
        null,
        new byte[] {'{', '}'},
        headers,
        Optional.empty());
  }

  private static RecordHeaders sourceTimestamp(String value) {
    RecordHeaders headers = new RecordHeaders();
    headers.add(
        new RecordHeader(DlqRecordFormat.SOURCE_TIMESTAMP, value.getBytes(StandardCharsets.UTF_8)));
    return headers;
  }

  @Test
  void headerLookupReturnsTheLastHeaderOfName() {
    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader("h", new byte[] {1}));
    headers.add(new RecordHeader("h", new byte[] {2}));
    headers.add(new RecordHeader("n", null));
    HeaderLookup lookup = KafkaRecords.headers(headers);
    assertThat(lookup.lastValue("h")).containsExactly(2);
    assertThat(lookup.lastValue("n")).isNull();
    assertThat(lookup.lastValue("missing")).isNull();
  }

  @Test
  void ingestTimeIsTheRecordTimestamp() {
    assertThat(
            KafkaRecords.ingestTime(
                record(RECORD_TIME.toEpochMilli(), new RecordHeaders()), false, CLOCK))
        .isEqualTo(RECORD_TIME);
  }

  @Test
  void recordWithoutTimestampUsesTheClock() {
    assertThat(KafkaRecords.ingestTime(record(-1, new RecordHeaders()), false, CLOCK))
        .isEqualTo(NOW);
    assertThat(KafkaRecords.timestamp(record(-1, new RecordHeaders()))).isNull();
  }

  @Test
  void replayedDlqRecordUsesItsOriginalTimestampWhenNoTimestampFieldIsSet() {
    assertThat(
            KafkaRecords.ingestTime(
                record(RECORD_TIME.toEpochMilli(), sourceTimestamp(ORIGINAL.toString())),
                false,
                CLOCK))
        .isEqualTo(ORIGINAL);
  }

  @Test
  void withTimestampFieldTheDlqHeaderIsNotUsed() {
    assertThat(
            KafkaRecords.ingestTime(
                record(RECORD_TIME.toEpochMilli(), sourceTimestamp(ORIGINAL.toString())),
                true,
                CLOCK))
        .isEqualTo(RECORD_TIME);
  }

  @Test
  void unparsableDlqHeaderIsIgnored() {
    assertThat(
            KafkaRecords.ingestTime(
                record(RECORD_TIME.toEpochMilli(), sourceTimestamp("yesterday")), false, CLOCK))
        .isEqualTo(RECORD_TIME);
  }
}
