package io.github.matabar.kafkafim.sink.es;

import java.time.Instant;
import java.util.Objects;

/**
 * The final outcome of one {@link BulkAction}: exactly one is reported per action, and only then
 * may the caller count the document as done (DESIGN §4.3). An action without an outcome (the sink
 * was closed before it completed) is not done; its record is re-delivered.
 */
public sealed interface ActionOutcome {

  /** Elasticsearch stored the document, or it already existed for {@code op: create}. */
  ActionOutcome INDEXED = new Indexed();

  /** The document was indexed (or, for {@code op: create}, already existed). */
  record Indexed() implements ActionOutcome {}

  /**
   * A newer external version was already stored and {@code on-version-conflict} is {@code ignore};
   * counted as {@code stale} and treated as success (DESIGN §4.4).
   *
   * @param reason the Elasticsearch item error JSON
   */
  record Stale(String reason) implements ActionOutcome {
    /** Requires a reason. */
    public Stale {
      Objects.requireNonNull(reason, "reason");
    }
  }

  /**
   * The document cannot be indexed and goes to the DLQ (DESIGN §7, §9).
   *
   * @param errorType {@code kafkafim.error.type}
   * @param reason the Elasticsearch item error JSON (or a short text for {@code too_large}); not
   *     truncated here, the DLQ writer truncates it to the header limit
   * @param status HTTP status of the item (or of the request for {@code too_large})
   * @param attempts how many times the document was sent and got an answer for it
   * @param firstFailure when the first failure for this document was seen
   */
  record Dlq(SinkErrorType errorType, String reason, int status, int attempts, Instant firstFailure)
      implements ActionOutcome {
    /** Requires every field. */
    public Dlq {
      Objects.requireNonNull(errorType, "errorType");
      Objects.requireNonNull(reason, "reason");
      Objects.requireNonNull(firstFailure, "firstFailure");
    }
  }
}
