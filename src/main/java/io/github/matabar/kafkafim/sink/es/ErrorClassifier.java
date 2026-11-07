package io.github.matabar.kafkafim.sink.es;

import io.github.matabar.kafkafim.config.Pipeline.OnVersionConflict;
import io.github.matabar.kafkafim.config.Pipeline.Op;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * The one table that decides data loss versus stall for every Elasticsearch answer (DESIGN §7).
 * Request-level failures are never attributed to a document, so they never reach the DLQ;
 * item-level outcomes are either final (indexed, stale, DLQ) or a bounded item retry. Rules are
 * evaluated top to bottom and the first match wins; the tables are unit-tested against captured
 * Elasticsearch 8.19 responses in {@code src/test/resources/sink/es/}.
 */
final class ErrorClassifier {

  /** What to do with a whole bulk request, by HTTP status. */
  enum RequestVerdict {
    /** 2xx: parse the items. */
    PARSE_ITEMS,
    /** 413: split in halves; a single action is DLQ {@code too_large}. */
    SPLIT,
    /** 401/403: configuration failure; retry forever with backoff, never DLQ. */
    AUTH_FAILURE,
    /** 429, 5xx, transport error: retry forever with backoff; counts against the breaker. */
    RETRY_CLUSTER,
    /**
     * Any other status (for example a 400 or 404 from a proxy or a wrong path): retry forever with
     * backoff and report it; it is not a cluster-health signal and it is not attributable to one
     * document, so it neither opens the breaker nor sends anything to the DLQ.
     */
    RETRY_UNEXPECTED
  }

  /** What to do with one item of a 2xx bulk response. */
  enum ItemVerdict {
    /** Done. */
    INDEXED,
    /** External-version conflict under {@code on-version-conflict: ignore}: done, counted stale. */
    STALE,
    /** DLQ now with {@link SinkErrorType#ES_ERROR}. */
    DLQ_ES_ERROR,
    /** DLQ now with {@link SinkErrorType#VERSION_CONFLICT}. */
    DLQ_VERSION_CONFLICT,
    /** Retry the item; counts against {@code max-attempts}/{@code max-elapsed}, then DLQ. */
    RETRY,
    /** The identity may not write this index: retry forever, never DLQ, report an auth failure. */
    AUTH_FAILURE
  }

  /** One row of the item table. */
  record ItemRule(String description, ItemCondition condition, ItemVerdict verdict) {}

  /** The condition of an item rule. */
  @FunctionalInterface
  interface ItemCondition {
    boolean test(Item item);
  }

  /**
   * The facts an item rule looks at.
   *
   * @param status item status
   * @param errorType {@code error.type} or {@code null}
   * @param op the action's op
   * @param versioned whether the action carried an external version
   * @param onVersionConflict the pipeline's policy
   */
  record Item(
      int status,
      @Nullable String errorType,
      Op op,
      boolean versioned,
      OnVersionConflict onVersionConflict) {}

  static final String VERSION_CONFLICT = "version_conflict_engine_exception";
  static final String INDEX_NOT_FOUND = "index_not_found_exception";

  /** The item table, first match wins. */
  static final List<ItemRule> ITEM_RULES =
      List.of(
          new ItemRule(
              "2xx: indexed", i -> i.status() >= 200 && i.status() < 300, ItemVerdict.INDEXED),
          new ItemRule(
              "409 external version, on-version-conflict: ignore → stale",
              i ->
                  isVersionConflict(i)
                      && i.versioned()
                      && i.onVersionConflict() == OnVersionConflict.IGNORE,
              ItemVerdict.STALE),
          new ItemRule(
              "409 external version, on-version-conflict: dlq → DLQ version_conflict",
              i -> isVersionConflict(i) && i.versioned(),
              ItemVerdict.DLQ_VERSION_CONFLICT),
          new ItemRule(
              "409 op: create, document already exists (a retry that landed) → indexed",
              i -> isVersionConflict(i) && i.op() == Op.CREATE,
              ItemVerdict.INDEXED),
          new ItemRule(
              "401/403: the identity may not write here → auth failure, retry, never DLQ",
              i -> i.status() == 401 || i.status() == 403,
              ItemVerdict.AUTH_FAILURE),
          new ItemRule(
              "429 (es_rejected_execution_exception): retry, poison-bounded",
              i -> i.status() == 429,
              ItemVerdict.RETRY),
          new ItemRule(
              "404 index_not_found_exception: provisioning gap or race; retry, poison-bounded",
              i -> i.status() == 404 && INDEX_NOT_FOUND.equals(i.errorType()),
              ItemVerdict.RETRY),
          new ItemRule("5xx: retry, poison-bounded", i -> i.status() >= 500, ItemVerdict.RETRY),
          new ItemRule(
              "other 4xx (mapper_parsing, document_parsing, strict_dynamic_mapping,"
                  + " illegal_argument, …) → DLQ es_error",
              i -> i.status() >= 400,
              ItemVerdict.DLQ_ES_ERROR),
          new ItemRule("anything else: retry, poison-bounded", i -> true, ItemVerdict.RETRY));

  private ErrorClassifier() {}

  /** Classifies a bulk request by its HTTP status. */
  static RequestVerdict request(int status) {
    if (status >= 200 && status < 300) {
      return RequestVerdict.PARSE_ITEMS;
    }
    if (status == 401 || status == 403) {
      return RequestVerdict.AUTH_FAILURE;
    }
    if (status == 413) {
      return RequestVerdict.SPLIT;
    }
    if (status == 429 || status >= 500) {
      return RequestVerdict.RETRY_CLUSTER;
    }
    return RequestVerdict.RETRY_UNEXPECTED;
  }

  /** Classifies one item of a 2xx bulk response. */
  static ItemVerdict item(Item item) {
    for (ItemRule rule : ITEM_RULES) {
      if (rule.condition().test(item)) {
        return rule.verdict();
      }
    }
    throw new IllegalStateException("the item table has a catch-all rule");
  }

  /** Classifies one parsed item for an action. */
  static ItemVerdict item(
      BulkResponse.Item result, BulkAction<?> action, OnVersionConflict onVersionConflict) {
    return item(
        new Item(
            result.status(),
            result.errorType(),
            action.op(),
            action.version() != null,
            onVersionConflict));
  }

  private static boolean isVersionConflict(Item item) {
    return item.status() == 409 && VERSION_CONFLICT.equals(item.errorType());
  }
}
