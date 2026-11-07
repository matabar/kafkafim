package io.github.matabar.kafkafim.route;

import java.util.Map;
import java.util.Objects;

/**
 * Read access to a record's Kafka headers, so that {@code route} does not depend on kafka-clients.
 * The Kafka adapter is {@code name -> { Header h = headers.lastHeader(name); return h == null ?
 * null : h.value(); }}.
 */
@FunctionalInterface
public interface HeaderLookup {

  /** A record without headers. */
  HeaderLookup NONE = name -> null;

  /**
   * The value of the last header called {@code name} (Kafka allows repeats; the last one wins, as
   * {@code Headers.lastHeader} does), or {@code null} if there is none or its value is null. The
   * returned array is only read.
   *
   * @param name header name, compared exactly
   * @return the raw header value, or {@code null}
   */
  byte[] lastValue(String name);

  /**
   * A lookup over a map of header names to values.
   *
   * @param headers header values by name; not copied
   * @return a lookup reading {@code headers}
   */
  static HeaderLookup of(Map<String, byte[]> headers) {
    Objects.requireNonNull(headers, "headers");
    return headers::get;
  }
}
