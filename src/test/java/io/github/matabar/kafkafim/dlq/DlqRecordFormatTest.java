package io.github.matabar.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DlqRecordFormatTest {

  private static final Instant SOURCE_TS = Instant.parse("2026-11-01T08:00:00.250Z");
  private static final Instant FIRST_FAILURE = Instant.parse("2026-11-08T10:15:30.123Z");

  static DlqRequest request(List<Header> headers, byte[] key, byte[] value) {
    return new DlqRequest(
        "orders.v2",
        3,
        12345678901L,
        SOURCE_TS,
        key,
        value,
        headers,
        "es_error",
        "{\"type\":\"mapper_parsing_exception\"}",
        "search-eu",
        "orders-eu-2026.11",
        4,
        FIRST_FAILURE,
        7);
  }

  private static Header header(String key, String value) {
    return new RecordHeader(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
  }

  private static List<Map.Entry<String, String>> entries(Iterable<Header> headers) {
    List<Map.Entry<String, String>> out = new ArrayList<>();
    for (Header header : headers) {
      out.add(
          Map.entry(
              header.key(),
              header.value() == null
                  ? "<null>"
                  : new String(header.value(), StandardCharsets.UTF_8)));
    }
    return out;
  }

  @Test
  void writesEveryHeaderAfterTheOriginalOnes() {
    byte[] key = {1, 2, 3};
    byte[] value = {(byte) 0xff, 0, '{'};
    DlqRequest request =
        request(List.of(header("traceparent", "00-abc-def-01"), header("x", "1")), key, value);

    ProducerRecord<byte[], byte[]> record =
        DlqRecordFormat.toProducerRecord("orders.kafkafim-dlq", "orders", request);

    assertThat(record.topic()).isEqualTo("orders.kafkafim-dlq");
    assertThat(record.key()).isSameAs(key);
    assertThat(record.value()).isSameAs(value);
    assertThat(record.partition()).isNull();
    assertThat(record.timestamp()).as("stamped by the producer at send time").isNull();
    assertThat(entries(record.headers()))
        .containsExactly(
            Map.entry("traceparent", "00-abc-def-01"),
            Map.entry("x", "1"),
            Map.entry("kafkafim.pipeline", "orders"),
            Map.entry("kafkafim.error.type", "es_error"),
            Map.entry("kafkafim.error.reason", "{\"type\":\"mapper_parsing_exception\"}"),
            Map.entry("kafkafim.source.topic", "orders.v2"),
            Map.entry("kafkafim.source.partition", "3"),
            Map.entry("kafkafim.source.offset", "12345678901"),
            Map.entry("kafkafim.source.timestamp", "2026-11-01T08:00:00.250Z"),
            Map.entry("kafkafim.target.cluster", "search-eu"),
            Map.entry("kafkafim.target.index", "orders-eu-2026.11"),
            Map.entry("kafkafim.attempts", "4"),
            Map.entry("kafkafim.first-failure-ts", "2026-11-08T10:15:30.123Z"),
            Map.entry("kafkafim.doc-index", "7"));
  }

  @Test
  void omitsUnknownValuesAndKeepsNullKeyAndValue() {
    DlqRequest request =
        new DlqRequest(
            "orders.v2",
            0,
            0,
            null,
            null,
            null,
            null,
            "decode",
            null,
            null,
            null,
            1,
            Instant.parse("2026-11-08T10:15:30Z"),
            null);

    ProducerRecord<byte[], byte[]> record =
        DlqRecordFormat.toProducerRecord("dlq", "orders", request);

    assertThat(record.key()).isNull();
    assertThat(record.value()).isNull();
    assertThat(entries(record.headers()))
        .containsExactly(
            Map.entry("kafkafim.pipeline", "orders"),
            Map.entry("kafkafim.error.type", "decode"),
            Map.entry("kafkafim.source.topic", "orders.v2"),
            Map.entry("kafkafim.source.partition", "0"),
            Map.entry("kafkafim.source.offset", "0"),
            Map.entry("kafkafim.attempts", "1"),
            Map.entry("kafkafim.first-failure-ts", "2026-11-08T10:15:30Z"));
  }

  @Test
  void replayedRecordKeepsItsOwnHeadersButOnlyTheCurrentKafkafimHeaders() {
    List<Header> replayed =
        List.of(
            header("traceparent", "00-abc-def-01"),
            header("kafkafim.pipeline", "orders"),
            header("kafkafim.error.type", "es_error"),
            header("x", "1"),
            header("kafkafim.doc-index", "3"),
            header("Kafkafim.Error.Reason", "spoofed"),
            header("x", "2"),
            header("empty", null),
            header("kafkafim.source.topic", "orders.v2"));
    DlqRequest request =
        new DlqRequest(
            "orders.kafkafim-dlq",
            1,
            42,
            null,
            new byte[] {1},
            new byte[] {2},
            replayed,
            "no_route",
            null,
            null,
            null,
            1,
            FIRST_FAILURE,
            null);

    List<Map.Entry<String, String>> headers =
        entries(DlqRecordFormat.headers("orders-replay", request));

    assertThat(headers)
        .containsExactly(
            Map.entry("traceparent", "00-abc-def-01"),
            Map.entry("x", "1"),
            Map.entry("x", "2"),
            Map.entry("empty", "<null>"),
            Map.entry("kafkafim.pipeline", "orders-replay"),
            Map.entry("kafkafim.error.type", "no_route"),
            Map.entry("kafkafim.source.topic", "orders.kafkafim-dlq"),
            Map.entry("kafkafim.source.partition", "1"),
            Map.entry("kafkafim.source.offset", "42"),
            Map.entry("kafkafim.attempts", "1"),
            Map.entry("kafkafim.first-failure-ts", "2026-11-08T10:15:30.123Z"));
  }

  @Test
  void reasonIsCutTo4KiB() {
    String ascii = "a".repeat(5000);
    DlqRequest base = request(List.of(), null, null);
    DlqRequest request =
        new DlqRequest(
            base.sourceTopic(),
            base.sourcePartition(),
            base.sourceOffset(),
            base.sourceTimestamp(),
            null,
            null,
            List.of(),
            base.errorType(),
            ascii,
            null,
            null,
            1,
            base.firstFailure(),
            null);

    Header reason = DlqRecordFormat.headers("p", request).lastHeader("kafkafim.error.reason");

    assertThat(reason.value()).hasSize(DlqRecordFormat.MAX_REASON_BYTES);
    assertThat(new String(reason.value(), StandardCharsets.UTF_8)).isEqualTo("a".repeat(4096));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "é", // 2 bytes
        "€", // 3 bytes
        "😀", // 4 bytes, a surrogate pair in Java
      })
  void truncationNeverSplitsCodePoints(String character) {
    for (String prefix : new String[] {"", "a", "ab", "abc"}) {
      String text = prefix + character.repeat(3000);

      byte[] cut = DlqRecordFormat.truncateUtf8(text, 4096);

      int width = character.getBytes(StandardCharsets.UTF_8).length;
      int expected = prefix.length() + (4096 - prefix.length()) / width * width;
      assertThat(cut).as("prefix %s", prefix.length()).hasSize(expected);
      String decoded = new String(cut, StandardCharsets.UTF_8);
      assertThat(decoded).doesNotContain("�");
      assertThat(text).startsWith(decoded);
    }
  }

  @Test
  void textThatFitsIsUnchanged() {
    String exact = "€".repeat(1365) + "a"; // 4096 bytes

    assertThat(DlqRecordFormat.truncateUtf8(exact, 4096))
        .isEqualTo(exact.getBytes(StandardCharsets.UTF_8));
    assertThat(DlqRecordFormat.truncateUtf8("", 4096)).isEmpty();
  }

  @Test
  void instantsAreIsoUtc() {
    DlqRequest request =
        new DlqRequest(
            "t",
            0,
            0,
            Instant.ofEpochMilli(0),
            null,
            null,
            List.of(),
            "decode",
            null,
            null,
            null,
            1,
            Instant.parse("2026-11-08T10:15:30.123456789Z"),
            null);

    var headers = DlqRecordFormat.headers("p", request);

    assertThat(new String(headers.lastHeader("kafkafim.source.timestamp").value()))
        .isEqualTo("1970-01-01T00:00:00Z");
    assertThat(new String(headers.lastHeader("kafkafim.first-failure-ts").value()))
        .isEqualTo("2026-11-08T10:15:30.123456789Z");
  }

  @Test
  void requestValidation() {
    Instant now = Instant.now();
    assertThatThrownBy(() ->
            new DlqRequest(
                "t", 0, 0, null, null, null, null, "decode", null, null, null, 0, now, null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() ->
            new DlqRequest(
                "t", -1, 0, null, null, null, null, "decode", null, null, null, 1, now, null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() ->
            new DlqRequest("t", 0, 0, null, null, null, null, " ", null, null, null, 1, now, -1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() ->
            new DlqRequest(
                "t", 0, 0, null, null, null, null, "decode", null, null, null, 1, now, -1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() ->
            new DlqRequest(
                "t", 0, 0, null, null, null, null, "decode", null, null, null, 1, null, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void toStringNeverShowsData() {
    DlqRequest request =
        request(
            List.of(header("secret-header", "s3cr3t")),
            "key-data".getBytes(StandardCharsets.UTF_8),
            "value-data".getBytes(StandardCharsets.UTF_8));

    assertThat(request.toString())
        .contains("orders.v2-3@12345678901#7", "es_error")
        .doesNotContain("s3cr3t", "key-data", "value-data", "mapper_parsing");
  }

  @Test
  void headersAreSnapshotted() {
    List<Header> headers = new ArrayList<>(List.of(header("a", "1")));
    DlqRequest request = request(headers, null, null);

    headers.add(header("b", "2"));

    assertThat(request.headers()).hasSize(1);
    assertThatThrownBy(() -> request.headers().add(header("c", "3")))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
