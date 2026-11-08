package io.github.matabar.kafkafim.sink.es;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ErrorSummaryTest {

  /** A whole-request error whose reason quotes a field value, as parsing errors do. */
  static final String QUOTING_500 =
      """
      {"error":{"root_cause":[{"type":"document_parsing_exception",\
      "reason":"failed to parse field [card] of type [long]. Preview of field's value: \
      '4111-SECRET'"},{"type":"number_format_exception","reason":"For input string: \
      \\"4111-SECRET\\""}],"type":"document_parsing_exception","reason":"failed to parse field \
      [card] of type [long]. Preview of field's value: '4111-SECRET'"},"status":500}\
      """;

  static String ofResponse(String body, boolean withReason) {
    return ErrorSummary.ofResponse(body.getBytes(UTF_8), withReason);
  }

  @Test
  void keepsTypesAndDropsTheReason() {
    assertThat(ofResponse(QUOTING_500, false))
        .isEqualTo(
            "type=document_parsing_exception,"
                + " root_cause=[document_parsing_exception, number_format_exception]")
        .doesNotContain("SECRET");
  }

  @Test
  void capturedRequestErrorsAreSummarizedByType() {
    assertThat(ErrorSummary.ofResponse(Fixtures.load("request-429.json"), false))
        .isEqualTo(
            "type=es_rejected_execution_exception, root_cause=[es_rejected_execution_exception]");
    assertThat(ErrorSummary.ofResponse(Fixtures.load("request-400-malformed.json"), false))
        .isEqualTo("type=illegal_argument_exception, root_cause=[illegal_argument_exception]");
  }

  @Test
  void securityErrorsKeepTheirReasonButNotTheHeaders() {
    assertThat(ErrorSummary.ofResponse(Fixtures.load("request-403.json"), true))
        .startsWith(
            "type=security_exception, root_cause=[security_exception], reason=action"
                + " [indices:data/write/bulk] is unauthorized for user [kfm-reader]");
    assertThat(ErrorSummary.ofResponse(Fixtures.load("request-401.json"), true))
        .contains("reason=unable to authenticate user [elastic]")
        .doesNotContain("WWW-Authenticate");
  }

  @Test
  void itemErrorJsonIsSummarizedTheSameWay() {
    String error =
        BulkResponse.parse(Fixtures.load("bulk-item-forbidden.json")).items().get(1).errorJson();

    assertThat(ErrorSummary.ofError(error, true))
        .startsWith("type=security_exception, reason=action [indices:data/write/bulk[s]]")
        .contains("on indices [kfm-strict]");
    assertThat(ErrorSummary.ofError(error, false)).isEqualTo("type=security_exception");
    assertThat(ErrorSummary.ofError("\"a string\"", true)).isEqualTo("error is not an object");
  }

  @Test
  void bodiesWithoutAnErrorObjectAreSummarizedBySize() {
    assertThat(ofResponse("<html>proxy 4111-SECRET</html>", true)).isEqualTo("non-JSON, 30 bytes");
    assertThat(ofResponse("", false)).isEqualTo("empty body");
    assertThat(ofResponse("{\"error\":\"4111-SECRET\",\"status\":400}", false))
        .isEqualTo("JSON without an error object, 36 bytes");
    assertThat(ofResponse("[1,2]", false)).isEqualTo("non-JSON, 5 bytes");
    assertThat(ofResponse("{\"error\":{\"type\":\"x\",", false)).isEqualTo("non-JSON, 21 bytes");
  }

  @Test
  void missingTypeIsUnknownAndLongValuesAreCut() {
    assertThat(ofResponse("{\"error\":{\"reason\":\"4111-SECRET\"}}", false))
        .isEqualTo("type=unknown");
    String longType = "t".repeat(ErrorSummary.MAX_TYPE_CHARS + 10);
    assertThat(ofResponse("{\"error\":{\"type\":\"" + longType + "\"}}", false))
        .isEqualTo("type=" + "t".repeat(ErrorSummary.MAX_TYPE_CHARS) + "…");
    StringBuilder causes = new StringBuilder();
    for (int i = 0; i < ErrorSummary.MAX_ROOT_CAUSES + 2; i++) {
      causes.append(i == 0 ? "" : ",").append("{\"type\":\"c").append(i).append("\"}");
    }
    assertThat(ofResponse("{\"error\":{\"type\":\"x\",\"root_cause\":[" + causes + "]}}", false))
        .isEqualTo("type=x, root_cause=[c0, c1, c2, c3, c4]");
  }
}
