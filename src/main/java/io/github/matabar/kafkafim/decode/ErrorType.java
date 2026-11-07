package io.github.matabar.kafkafim.decode;

/**
 * Why a record or a document could not be decoded. {@link #wireName()} is the DLQ {@code
 * kafkafim.error.type} header value and the {@code error_type} metric label (DESIGN §7, §9).
 */
public enum ErrorType {
  /**
   * The value is not a JSON object or array of objects, an array element is not an object, the
   * value is larger than {@code max-record-bytes}, or a configured version is unusable.
   */
  DECODE("decode"),
  /** The {@code _id} pointer resolves to nothing usable and {@code id-missing} is {@code dlq}. */
  MISSING_ID("missing_id"),
  /** The timestamp is missing or unparsable and {@code on-bad-timestamp} is {@code dlq}. */
  BAD_TIMESTAMP("bad_timestamp");

  private final String wireName;

  ErrorType(String wireName) {
    this.wireName = wireName;
  }

  /** The {@code kafkafim.error.type} header value. */
  public String wireName() {
    return wireName;
  }
}
