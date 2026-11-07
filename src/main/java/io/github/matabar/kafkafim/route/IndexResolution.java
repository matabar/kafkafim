package io.github.matabar.kafkafim.route;

/**
 * The outcome of {@link IndexPattern#resolve}: an index name, or why there is none. Failures are
 * values, never exceptions, so the hot path stays branch-only; each failure sends the document to
 * the DLQ with {@code error.type=}{@value #UNRESOLVED_INDEX} and {@link Failure#reason()} as {@code
 * error.reason} (DESIGN §5.3, §9).
 */
public sealed interface IndexResolution {

  /** DLQ {@code error.type} for every {@link Failure}. */
  String UNRESOLVED_INDEX = "unresolved_index";

  /** The pattern resolved to a valid, lower-cased index name. */
  record Resolved(String index) implements IndexResolution {}

  /** A pattern that did not resolve to a usable index name. */
  sealed interface Failure extends IndexResolution {

    /** Human-readable cause, for {@code kafkafim.error.reason}. */
    String reason();

    /** DLQ {@code error.type}: always {@value IndexResolution#UNRESOLVED_INDEX}. */
    default String errorType() {
      return UNRESOLVED_INDEX;
    }
  }

  /**
   * A {@code %{...}} reference produced no value.
   *
   * @param reference the text between {@code %{} and {@code }}, e.g. {@code region}, {@code /a/b}
   *     or {@code +yyyy.MM.dd}
   * @param cause what was wrong with it, e.g. {@code missing}, {@code null}, {@code an object}
   */
  record Unresolved(String reference, String cause) implements Failure {
    @Override
    public String reason() {
      return "index pattern field '%{" + reference + "}' is " + cause;
    }
  }

  /**
   * Every reference resolved, but Elasticsearch would reject the resulting name.
   *
   * @param index the lower-cased name that was produced
   * @param problem which index-name rule it breaks
   */
  record InvalidName(String index, String problem) implements Failure {
    @Override
    public String reason() {
      return "resolved index name '" + index + "' is invalid: " + problem;
    }
  }
}
