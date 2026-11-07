package io.github.matabar.kafkafim.decode;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import tools.jackson.databind.JsonNode;

/** One document of a record: decoded, or failed on its own without affecting its siblings. */
public sealed interface DocumentOutcome {

  /** Position inside the record: 0 for an object record, the array index otherwise. */
  int index();

  /**
   * A document ready for routing and indexing.
   *
   * @param index position inside the record
   * @param node the parsed document, read by routing and index-pattern resolution; shared, so
   *     callers must not mutate it
   * @param source the bytes to index as {@code _source}
   * @param id the {@code _id}; empty only with {@code id-missing: auto} (ES generates one)
   * @param version the external version, when {@code version-field} is configured
   * @param timestamp the event time for {@code %{+...}} index patterns
   * @param timestampSource where {@code timestamp} came from
   */
  record Decoded(
      int index,
      JsonNode node,
      DocumentSource source,
      Optional<String> id,
      OptionalLong version,
      Instant timestamp,
      TimestampSource timestampSource)
      implements DocumentOutcome {

    /** Rejects nulls. */
    public Decoded {
      Objects.requireNonNull(node, "node");
      Objects.requireNonNull(source, "source");
      Objects.requireNonNull(id, "id");
      Objects.requireNonNull(version, "version");
      Objects.requireNonNull(timestamp, "timestamp");
      Objects.requireNonNull(timestampSource, "timestampSource");
    }
  }

  /**
   * A document that goes to the DLQ (with {@code kafkafim.doc-index} when the record is an array).
   *
   * @param index position inside the record
   * @param error why
   */
  record Failed(int index, DecodeError error) implements DocumentOutcome {

    /** Rejects nulls. */
    public Failed {
      Objects.requireNonNull(error, "error");
    }
  }

  /** Where a decoded document's timestamp came from. */
  enum TimestampSource {
    /** Parsed from {@code timestamp-field}. */
    FIELD,
    /** No {@code timestamp-field} is configured: the record's ingest time, by design. */
    INGEST_TIME,
    /**
     * {@code timestamp-field} was missing or unparsable and {@code on-bad-timestamp: ingest-time}
     * substituted the ingest time.
     */
    INGEST_TIME_FALLBACK
  }
}
