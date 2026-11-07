package io.github.matabar.kafkafim.dlq;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.header.Header;
import org.jspecify.annotations.Nullable;

/**
 * One document (or one whole record) that cannot be indexed, as the DLQ writer's input (DESIGN §9).
 *
 * <p>This type is the {@code dlq} package's own: it carries the original record's bytes and
 * coordinates plus the failure, so that {@code dlq} needs neither {@code source} nor {@code sink}
 * (ADR-0003). The supervisor builds it from a source record and a decode, route or sink failure.
 *
 * <p>The key and value arrays are not copied: the caller must not modify them after handing the
 * request over. A record yielding several failed documents yields one request per document (each
 * with its {@code docIndex}); the caller completes one part of the source record per successful
 * send (ADR-0025).
 *
 * @param sourceTopic topic the record was consumed from
 * @param sourcePartition partition the record was consumed from
 * @param sourceOffset offset of the record
 * @param sourceTimestamp the record's timestamp; {@code null} when the record has none
 * @param key original key bytes; {@code null} for a record without a key
 * @param value original value bytes; {@code null} for a tombstone
 * @param headers the record's original headers, in order (copied)
 * @param errorType {@code error.type} (DESIGN §7), e.g. {@code decode} or {@code es_error}
 * @param reason human- or machine-readable reason (ES item error JSON); {@code null} for none
 * @param targetCluster Elasticsearch cluster the document was routed to; {@code null} if unrouted
 * @param targetIndex resolved index; {@code null} if unrouted or unresolved
 * @param attempts indexing attempts made, at least 1
 * @param firstFailure when the document first failed
 * @param docIndex position inside a JSON array record; {@code null} for a whole-record failure
 */
public record DlqRequest(
    String sourceTopic,
    int sourcePartition,
    long sourceOffset,
    @Nullable Instant sourceTimestamp,
    byte @Nullable [] key,
    byte @Nullable [] value,
    List<Header> headers,
    String errorType,
    @Nullable String reason,
    @Nullable String targetCluster,
    @Nullable String targetIndex,
    int attempts,
    Instant firstFailure,
    @Nullable Integer docIndex) {

  /**
   * Validates the request and snapshots the headers.
   *
   * @throws NullPointerException if a required component is {@code null}
   * @throws IllegalArgumentException if a number is out of range or {@code errorType} is blank
   */
  public DlqRequest {
    Objects.requireNonNull(sourceTopic, "sourceTopic");
    Objects.requireNonNull(errorType, "errorType");
    Objects.requireNonNull(firstFailure, "firstFailure");
    if (sourceTopic.isBlank() || errorType.isBlank()) {
      throw new IllegalArgumentException("sourceTopic and errorType must not be blank");
    }
    if (sourcePartition < 0 || sourceOffset < 0) {
      throw new IllegalArgumentException(
          "partition and offset must not be negative: " + sourcePartition + "/" + sourceOffset);
    }
    if (attempts < 1) {
      throw new IllegalArgumentException("attempts must be at least 1: " + attempts);
    }
    if (docIndex != null && docIndex < 0) {
      throw new IllegalArgumentException("docIndex must not be negative: " + docIndex);
    }
    headers = headers == null ? List.of() : Collections.unmodifiableList(copyNonNull(headers));
  }

  private static List<Header> copyNonNull(List<Header> headers) {
    List<Header> copy = new ArrayList<>(headers.size());
    for (Header header : headers) {
      copy.add(Objects.requireNonNull(header, "headers must not contain null"));
    }
    return copy;
  }

  @Override
  public String toString() {
    // Never the key, value or header values: they are document data (DESIGN §10, logs).
    return "DlqRequest[source="
        + sourceTopic
        + "-"
        + sourcePartition
        + "@"
        + sourceOffset
        + (docIndex == null ? "" : "#" + docIndex)
        + ", errorType="
        + errorType
        + ", attempts="
        + attempts
        + "]";
  }
}
