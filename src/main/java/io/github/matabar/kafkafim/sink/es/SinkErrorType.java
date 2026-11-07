package io.github.matabar.kafkafim.sink.es;

/**
 * Why Elasticsearch did not take a document. {@link #wireName()} is the DLQ {@code
 * kafkafim.error.type} header value and the {@code error_type} metric label (DESIGN §7, §9). The
 * set is small and fixed on purpose: the Elasticsearch error type itself travels in the reason.
 */
public enum SinkErrorType {
  /**
   * A non-retryable item error: the 400 family ({@code mapper_parsing_exception}, {@code
   * document_parsing_exception}, {@code strict_dynamic_mapping_exception}, {@code
   * illegal_argument_exception}) and any other 4xx item status not listed elsewhere.
   */
  ES_ERROR("es_error"),
  /**
   * A retryable item error (item 429, item 5xx, {@code index_not_found_exception}) that still
   * failed after {@code dlq.max-attempts} or {@code dlq.max-elapsed}: the poison bound (ADR-0011).
   */
  RETRIES_EXHAUSTED("es_retries_exhausted"),
  /** An external-version conflict while {@code document.on-version-conflict} is {@code dlq}. */
  VERSION_CONFLICT("version_conflict"),
  /** A single-document bulk request was still rejected with 413. */
  TOO_LARGE("too_large");

  private final String wireName;

  SinkErrorType(String wireName) {
    this.wireName = wireName;
  }

  /** The {@code kafkafim.error.type} header value. */
  public String wireName() {
    return wireName;
  }
}
