package io.github.matabar.kafkafim.decode;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import tools.jackson.databind.JsonNode;

/**
 * One document of a record, on its own without affecting its siblings: decoded, parsed but not
 * indexable, or failed.
 *
 * <p>Routing comes before the id, version and timestamp checks (DESIGN §4.2): a {@link Routable}
 * outcome is routed first, and an {@link Unindexable} one fails only once routing has decided that
 * the document goes to Elasticsearch. A {@link Failed} one is not an object and is never routed.
 */
public sealed interface DocumentOutcome {

  /** Position inside the record: 0 for an object record, the array index otherwise. */
  int index();

  /** An outcome with a parsed object that routing can read. */
  sealed interface Routable extends DocumentOutcome {

    /** The parsed document; shared, so callers must not mutate it. */
    JsonNode node();
  }

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
      implements Routable {

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
   * An object whose id, version or timestamp is unusable ({@link ErrorType#MISSING_ID}, {@link
   * ErrorType#DECODE} for the version, {@link ErrorType#BAD_TIMESTAMP}). It is still routed; the
   * error applies only if routing sends the document to Elasticsearch, and then once per document,
   * not once per routed copy. The first failing check wins, in the order id, version, timestamp.
   *
   * @param index position inside the record
   * @param node the parsed document, for routing; shared, so callers must not mutate it
   * @param error why the document cannot be indexed
   */
  record Unindexable(int index, JsonNode node, DecodeError error) implements Routable {

    /** Rejects nulls. */
    public Unindexable {
      Objects.requireNonNull(node, "node");
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * An array element that is not an object: it goes to the DLQ with {@code error.type=decode} and
   * {@code kafkafim.doc-index}, without routing.
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
