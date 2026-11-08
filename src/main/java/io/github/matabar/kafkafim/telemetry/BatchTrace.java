package io.github.matabar.kafkafim.telemetry;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One finished bulk batch, as the input of a {@code pipeline.batch} span (DESIGN §10, ADR-0029).
 *
 * @param pipeline pipeline name
 * @param kafkaCluster source Kafka cluster
 * @param esCluster Elasticsearch cluster
 * @param identity Elasticsearch identity, or {@code null}
 * @param flushReason which limit sealed the batch ({@code docs}, {@code bytes}, {@code age}, ...)
 * @param docs actions in the batch
 * @param bytes uncompressed body size
 * @param started when the batch was sealed (the span's start)
 * @param ended when its last request was dealt with
 * @param requests its bulk requests, in order; each becomes an {@code es.bulk} child span
 * @param traceParents W3C {@code traceparent} values of the batch's source records; each valid one
 *     becomes a span link (duplicates and anything past the link limit are dropped)
 */
public record BatchTrace(
    String pipeline,
    String kafkaCluster,
    String esCluster,
    @Nullable String identity,
    String flushReason,
    int docs,
    long bytes,
    Instant started,
    Instant ended,
    List<BulkRequest> requests,
    List<String> traceParents) {

  /** Copies the lists. */
  public BatchTrace {
    Objects.requireNonNull(pipeline, "pipeline");
    Objects.requireNonNull(started, "started");
    Objects.requireNonNull(ended, "ended");
    requests = List.copyOf(requests);
    traceParents = List.copyOf(traceParents);
  }

  /**
   * One bulk request of the batch.
   *
   * @param started when it was sent
   * @param ended when it was answered or failed
   * @param docs actions in it
   * @param bytes uncompressed body size
   * @param status HTTP status, 0 without a response
   * @param error transport or parse failure, or {@code null}
   * @param retries failed attempts of this request before this one
   * @param itemErrors items of the response that did not succeed
   */
  public record BulkRequest(
      Instant started,
      Instant ended,
      int docs,
      long bytes,
      int status,
      @Nullable String error,
      int retries,
      int itemErrors) {

    /**
     * The OpenTelemetry {@code error.type} of the request, or {@code null} if it succeeded: the
     * failure for a transport or parse error, the HTTP status for a non-2xx answer, {@code
     * item_errors} when only items failed.
     */
    public @Nullable String errorType() {
      if (error != null) {
        return status == 0 ? "io_error" : "malformed_response";
      }
      if (status < 200 || status >= 300) {
        return Integer.toString(status);
      }
      return itemErrors > 0 ? "item_errors" : null;
    }
  }

  /** The first failing request's {@code error.type}, or {@code null} if every request succeeded. */
  public @Nullable String errorType() {
    for (BulkRequest request : requests) {
      String type = request.errorType();
      if (type != null) {
        return type;
      }
    }
    return null;
  }
}
