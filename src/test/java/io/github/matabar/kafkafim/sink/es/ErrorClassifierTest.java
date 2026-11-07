package io.github.matabar.kafkafim.sink.es;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.config.Pipeline.OnVersionConflict;
import io.github.matabar.kafkafim.config.Pipeline.Op;
import io.github.matabar.kafkafim.sink.es.ErrorClassifier.ItemVerdict;
import io.github.matabar.kafkafim.sink.es.ErrorClassifier.RequestVerdict;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The classification table against responses captured from Elasticsearch 8.19.0 (see {@code
 * src/test/resources/sink/es/README.md} for how each was produced).
 */
class ErrorClassifierTest {

  static BulkAction<String> action(Op op, Long version) {
    return new BulkAction<>("kfm-orders", "1", op, version, null, "{}".getBytes(UTF_8), "h");
  }

  static List<ItemVerdict> classify(
      String fixture, List<BulkAction<String>> actions, OnVersionConflict policy) {
    BulkResponse response = BulkResponse.parse(Fixtures.load(fixture));
    assertThat(response.items()).hasSameSizeAs(actions);
    return IntStream.range(0, actions.size())
        .mapToObj(i -> ErrorClassifier.item(response.items().get(i), actions.get(i), policy))
        .toList();
  }

  /** The actions of {@code bulk-mixed-errors.json}, in request order. */
  static final List<BulkAction<String>> MIXED =
      List.of(
          action(Op.INDEX, null), // 201
          action(Op.INDEX, null), // 400 document_parsing_exception (not a number)
          action(Op.INDEX, null), // 400 strict_dynamic_mapping_exception
          action(Op.CREATE, null), // 409 document already exists
          action(Op.INDEX, 3L), // 409 external version 3 < stored 5
          action(Op.INDEX, 7L), // 200 external version 7 > stored 5
          action(Op.INDEX, null), // 404 index_not_found_exception
          action(Op.INDEX, null), // 400 illegal_argument_exception (no such ingest pipeline)
          action(Op.INDEX, null)); // 400 document_parsing_exception (object for a long)

  @Test
  void mixedResponseUnderIgnore() {
    assertThat(classify("bulk-mixed-errors.json", MIXED, OnVersionConflict.IGNORE))
        .containsExactly(
            ItemVerdict.INDEXED,
            ItemVerdict.DLQ_ES_ERROR,
            ItemVerdict.DLQ_ES_ERROR,
            ItemVerdict.INDEXED,
            ItemVerdict.STALE,
            ItemVerdict.INDEXED,
            ItemVerdict.RETRY,
            ItemVerdict.DLQ_ES_ERROR,
            ItemVerdict.DLQ_ES_ERROR);
  }

  @Test
  void mixedResponseUnderDlqPolicyOnlyChangesTheVersionedConflict() {
    assertThat(classify("bulk-mixed-errors.json", MIXED, OnVersionConflict.DLQ).get(4))
        .isEqualTo(ItemVerdict.DLQ_VERSION_CONFLICT);
    assertThat(classify("bulk-mixed-errors.json", MIXED, OnVersionConflict.DLQ).get(3))
        .as("op: create without a version: an earlier attempt landed")
        .isEqualTo(ItemVerdict.INDEXED);
  }

  @Test
  void createConflictInUnfilteredResponse() {
    assertThat(
            classify(
                "bulk-create-conflict-unfiltered.json",
                List.of(action(Op.INDEX, null), action(Op.CREATE, null)),
                OnVersionConflict.IGNORE))
        .containsExactly(ItemVerdict.INDEXED, ItemVerdict.INDEXED);
  }

  @Test
  void itemRejectedExecutionIsRetried() {
    List<BulkAction<String>> actions =
        IntStream.range(0, 8).mapToObj(i -> action(Op.INDEX, null)).toList();
    assertThat(classify("bulk-item-rejected.json", actions, OnVersionConflict.IGNORE))
        .containsExactly(
            ItemVerdict.RETRY,
            ItemVerdict.RETRY,
            ItemVerdict.INDEXED,
            ItemVerdict.RETRY,
            ItemVerdict.RETRY,
            ItemVerdict.RETRY,
            ItemVerdict.RETRY,
            ItemVerdict.RETRY);
  }

  @Test
  void itemForbiddenIsAnAuthFailureNeverDlq() {
    assertThat(
            classify(
                "bulk-item-forbidden.json",
                List.of(action(Op.INDEX, null), action(Op.INDEX, null)),
                OnVersionConflict.IGNORE))
        .containsExactly(ItemVerdict.INDEXED, ItemVerdict.AUTH_FAILURE);
  }

  @ParameterizedTest
  @CsvSource({
    "201, , INDEX, false, INDEXED",
    "200, , CREATE, true, INDEXED",
    "400, mapper_parsing_exception, INDEX, false, DLQ_ES_ERROR",
    "409, version_conflict_engine_exception, INDEX, false, DLQ_ES_ERROR",
    "409, some_other_conflict, CREATE, false, DLQ_ES_ERROR",
    "401, security_exception, INDEX, false, AUTH_FAILURE",
    "404, index_closed_exception, INDEX, false, DLQ_ES_ERROR",
    "429, circuit_breaking_exception, INDEX, false, RETRY",
    "500, internal_error, INDEX, false, RETRY",
    "503, unavailable_shards_exception, INDEX, true, RETRY",
    "302, , INDEX, false, RETRY"
  })
  void syntheticItems(int status, String type, Op op, boolean versioned, ItemVerdict expected) {
    assertThat(
            ErrorClassifier.item(
                new ErrorClassifier.Item(status, type, op, versioned, OnVersionConflict.IGNORE)))
        .isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "request-401.json, 401, AUTH_FAILURE, security_exception",
    "request-403.json, 403, AUTH_FAILURE, security_exception",
    "request-429.json, 429, RETRY_CLUSTER, es_rejected_execution_exception",
    "request-400-malformed.json, 400, RETRY_UNEXPECTED, illegal_argument_exception",
    "request-413.json, 413, SPLIT, ''"
  })
  void capturedRequestLevelResponses(
      String fixture, int status, RequestVerdict expected, String bodyMentions) {
    assertThat(new String(Fixtures.load(fixture), UTF_8)).contains(bodyMentions);
    assertThat(ErrorClassifier.request(status)).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "200, PARSE_ITEMS",
    "201, PARSE_ITEMS",
    "500, RETRY_CLUSTER",
    "502, RETRY_CLUSTER",
    "503, RETRY_CLUSTER",
    "504, RETRY_CLUSTER",
    "404, RETRY_UNEXPECTED",
    "301, RETRY_UNEXPECTED"
  })
  void requestStatuses(int status, RequestVerdict expected) {
    assertThat(ErrorClassifier.request(status)).isEqualTo(expected);
  }

  @Test
  void itemTableEndsWithCatchAll() {
    assertThat(ErrorClassifier.ITEM_RULES.getLast().verdict()).isEqualTo(ItemVerdict.RETRY);
    assertThat(
            ErrorClassifier.ITEM_RULES
                .getLast()
                .condition()
                .test(new ErrorClassifier.Item(0, null, Op.INDEX, false, OnVersionConflict.DLQ)))
        .isTrue();
  }
}
