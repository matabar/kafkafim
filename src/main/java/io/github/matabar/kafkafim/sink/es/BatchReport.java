package io.github.matabar.kafkafim.sink.es;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * What one sealed batch did, from the moment a sender took it until every one of its actions had an
 * outcome or went back into the batcher for an item retry (DESIGN §10, traces). Reported once per
 * batch through {@link SinkListener#onBatchSent}, which turns it into a {@code pipeline.batch} span
 * with one {@code es.bulk} child per request. A batch that never ends (Elasticsearch stays down) is
 * never reported; metrics cover that case.
 *
 * @param reason which limit sealed the batch
 * @param docs actions in the batch
 * @param bytes uncompressed body size of the batch
 * @param sealed when the batch was sealed
 * @param started when a sender took it (after waiting for an in-flight permit)
 * @param ended when its last request was dealt with
 * @param requests every bulk request sent for it, in order: retries of the whole request and the
 *     halves of a 413 split each count as one
 * @param handles the completion handles of the batch's actions, in order, for span links; the sink
 *     never looks at them
 */
public record BatchReport(
    FlushReason reason,
    int docs,
    long bytes,
    Instant sealed,
    Instant started,
    Instant ended,
    List<Request> requests,
    List<?> handles) {

  /** Copies the lists. */
  public BatchReport {
    Objects.requireNonNull(reason, "reason");
    requests = List.copyOf(requests);
    handles = List.copyOf(handles);
  }

  /** Whether any request failed, or any item of an answered request did not succeed. */
  public boolean failed() {
    return requests.stream().anyMatch(Request::failed);
  }

  /**
   * One bulk request.
   *
   * @param started when it was sent
   * @param ended when its answer was parsed or the transport failed
   * @param docs actions in the request
   * @param bytes uncompressed body size
   * @param status HTTP status, or 0 when there was no response (transport error)
   * @param error the transport or parse failure, or {@code null}; names only, never a body
   * @param retries failed attempts of this request before this one
   * @param itemErrors items of the response that did not succeed (retried, dead-lettered or
   *     rejected for auth)
   */
  public record Request(
      Instant started,
      Instant ended,
      int docs,
      long bytes,
      int status,
      @Nullable String error,
      int retries,
      int itemErrors) {

    /** Whether the request failed as a whole or had item errors. */
    public boolean failed() {
      return error != null || status < 200 || status >= 300 || itemErrors > 0;
    }
  }
}
