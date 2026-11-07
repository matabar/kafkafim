package io.github.matabar.kafkafim.sink.es;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.github.matabar.kafkafim.config.Pipeline.Op;
import java.util.List;
import org.junit.jupiter.api.Test;

class BulkBodyTest {

  static BulkAction<String> action(
      String index, String id, Op op, Long version, String pipeline, String source) {
    return new BulkAction<>(index, id, op, version, pipeline, source.getBytes(UTF_8), "h");
  }

  static String meta(BulkAction<?> action) {
    return new String(BulkBody.metadataLine(action), UTF_8);
  }

  @Test
  void indexWithId() {
    assertThat(meta(action("orders", "1", Op.INDEX, null, null, "{}")))
        .isEqualTo("{\"index\":{\"_index\":\"orders\",\"_id\":\"1\"}}");
  }

  @Test
  void createWithoutIdOmitsId() {
    assertThat(meta(action("logs-app", null, Op.CREATE, null, null, "{}")))
        .isEqualTo("{\"create\":{\"_index\":\"logs-app\"}}");
  }

  @Test
  void externalVersionAndIngestPipeline() {
    assertThat(meta(action("orders", "7", Op.INDEX, 42L, "orders-enrich", "{}")))
        .isEqualTo(
            "{\"index\":{\"_index\":\"orders\",\"_id\":\"7\",\"version\":42,"
                + "\"version_type\":\"external\",\"pipeline\":\"orders-enrich\"}}");
  }

  @Test
  void versionZeroAndMaxLong() {
    assertThat(meta(action("o", "1", Op.INDEX, 0L, null, "{}"))).contains("\"version\":0,");
    assertThat(meta(action("o", "1", Op.INDEX, Long.MAX_VALUE, null, "{}")))
        .contains("\"version\":9223372036854775807,");
  }

  @Test
  void idIsEscaped() {
    String id = "a\"b\\c" + (char) 1 + "é";
    String escapedControl = "\\" + "u0001";
    assertThat(meta(action("orders", id, Op.INDEX, null, null, "{}")))
        .isEqualTo(
            "{\"index\":{\"_index\":\"orders\",\"_id\":\"a\\\"b\\\\c" + escapedControl + "é\"}}");
  }

  @Test
  void sourceIsCopiedVerbatimAndSizesMatch() {
    // Number spelling, key order, whitespace and unicode survive: the bytes are not re-serialised.
    String source = "{\"b\":1.0E2, \"a\":[1 ,2],\"u\":\"\\u00e9\",\"x\":\"é\"}";
    BulkAction<String> first = action("orders", "1", Op.INDEX, null, null, source);
    BulkAction<String> second = action("orders", null, Op.INDEX, 3L, "p", "{}");
    List<TestEntry> entries = List.of(TestEntry.of(first), TestEntry.of(second));
    long size = entries.stream().mapToLong(TestEntry::size).sum();

    byte[] body = BulkBody.build(entries, size);

    assertThat(body).hasSize((int) size);
    assertThat(new String(body, UTF_8))
        .isEqualTo(meta(first) + "\n" + source + "\n" + meta(second) + "\n{}\n");
  }

  @Test
  void sizeCountsBothLineFeeds() {
    assertThat(BulkBody.size(new byte[10], new byte[5])).isEqualTo(17);
  }

  @Test
  void actionRejectsLineFeedInSource() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> action("orders", "1", Op.INDEX, null, null, "{\"a\":\n1}"))
        .withMessageContaining("line feed");
  }

  @Test
  void actionRejectsEmptySourceAndNegativeVersion() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> action("orders", "1", Op.INDEX, null, null, ""));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> action("orders", "1", Op.INDEX, -1L, null, "{}"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> action("", "1", Op.INDEX, null, null, "{}"));
  }

  @Test
  void actionRejectsWhatElasticsearchWouldRejectForTheWholeRequest() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> action("orders", "1", Op.CREATE, 5L, null, "{}"))
        .withMessageContaining("external versions");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> action("orders", "1", Op.INDEX, null, "", "{}"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> action("orders", "", Op.INDEX, null, null, "{}"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> action("orders", "é".repeat(257), Op.INDEX, null, null, "{}"));
    assertThat(action("orders", "x".repeat(512), Op.INDEX, null, null, "{}").id()).hasSize(512);
  }

  record TestEntry(byte[] metadataLine, byte[] source) implements BulkBody.Entry {
    static TestEntry of(BulkAction<?> action) {
      return new TestEntry(BulkBody.metadataLine(action), action.source());
    }

    long size() {
      return BulkBody.size(metadataLine, source);
    }
  }
}
