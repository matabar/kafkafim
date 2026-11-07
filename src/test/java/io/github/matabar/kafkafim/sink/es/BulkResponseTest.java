package io.github.matabar.kafkafim.sink.es;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BulkResponseTest {

  @Test
  void allOkStopsAtErrorsFalse() {
    BulkResponse response = BulkResponse.parse(Fixtures.load("bulk-all-ok.json"));
    assertThat(response.errors()).isFalse();
    assertThat(response.items()).isEmpty();
  }

  @Test
  void fastPathDoesNotReadTheItems() {
    // Whatever follows errors:false is never parsed, so a large success response costs nothing.
    byte[] body = "{\"errors\":false,\"items\":[garbage".getBytes(UTF_8);
    assertThat(BulkResponse.parse(body).errors()).isFalse();
  }

  @Test
  void mixedErrorsMapByPosition() {
    BulkResponse response = BulkResponse.parse(Fixtures.load("bulk-mixed-errors.json"));
    assertThat(response.errors()).isTrue();
    assertThat(response.items())
        .extracting(BulkResponse.Item::status, BulkResponse.Item::errorType)
        .containsExactly(
            tuple(201, null),
            tuple(400, "document_parsing_exception"),
            tuple(400, "strict_dynamic_mapping_exception"),
            tuple(409, "version_conflict_engine_exception"),
            tuple(409, "version_conflict_engine_exception"),
            tuple(200, null),
            tuple(404, "index_not_found_exception"),
            tuple(400, "illegal_argument_exception"),
            tuple(400, "document_parsing_exception"));
  }

  @Test
  void errorJsonIsTheWholeErrorObjectIncludingCause() {
    BulkResponse.Item item =
        BulkResponse.parse(Fixtures.load("bulk-mixed-errors.json")).items().get(1);
    assertThat(item.errorJson())
        .startsWith("{\"type\":\"document_parsing_exception\"")
        .contains("\"caused_by\":{\"type\":\"illegal_argument_exception\"")
        .endsWith("}");
  }

  @Test
  void unfilteredResponseWithTookAndMetadataParses() {
    BulkResponse response =
        BulkResponse.parse(Fixtures.load("bulk-create-conflict-unfiltered.json"));
    assertThat(response.items()).extracting(BulkResponse.Item::status).containsExactly(200, 409);
  }

  @Test
  void itemLevelRejectionsAndForbidden() {
    assertThat(BulkResponse.parse(Fixtures.load("bulk-item-rejected.json")).items())
        .extracting(BulkResponse.Item::status)
        .containsExactly(429, 429, 200, 429, 429, 429, 429, 429);
    assertThat(BulkResponse.parse(Fixtures.load("bulk-item-forbidden.json")).items())
        .extracting(BulkResponse.Item::status, BulkResponse.Item::errorType)
        .containsExactly(tuple(201, null), tuple(403, "security_exception"));
  }

  @Test
  void errorWithoutTypeIsUnknown() {
    byte[] body =
        "{\"errors\":true,\"items\":[{\"index\":{\"status\":500,\"error\":\"boom\"}}]}"
            .getBytes(UTF_8);
    BulkResponse.Item item = BulkResponse.parse(body).items().getFirst();
    assertThat(item.errorType()).isEqualTo("unknown");
    assertThat(item.errorJson()).isEqualTo("\"boom\"");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "[]",
        "{}",
        "{\"errors\":true}",
        "{\"errors\":true,\"items\":[{\"index\":{}}]}",
        "{\"errors\":true,\"items\":[1]}",
        "{\"errors\":true,\"items\":[{\"index\":{\"status\":200},\"create\":{}}]}",
        "{\"errors\":true,\"items\":[{\"index\":{\"status\":200}}",
        "<html>bad gateway</html>"
      })
  void malformedResponsesAreRejected(String body) {
    assertThatThrownBy(() -> BulkResponse.parse(body.getBytes(UTF_8)))
        .isInstanceOf(BulkResponse.MalformedResponseException.class);
  }
}
