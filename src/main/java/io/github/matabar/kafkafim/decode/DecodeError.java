package io.github.matabar.kafkafim.decode;

import java.util.Objects;

/**
 * A typed decode failure.
 *
 * @param type the DLQ {@code error.type}
 * @param reason human-readable detail for {@code kafkafim.error.reason}; quotes at most a short
 *     excerpt of the offending value
 */
public record DecodeError(ErrorType type, String reason) {

  /** Rejects nulls. */
  public DecodeError {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(reason, "reason");
  }
}
