package io.github.matabar.kafkafim.sink.es;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.time.Duration;
import org.jspecify.annotations.Nullable;

/**
 * Hook for metrics and for the supervisor: the telemetry slice binds meters to it and the
 * supervisor pauses a pipeline on an auth failure (DESIGN §5.2.1, §10). Every method has a no-op
 * default; implementations must be quick and thread-safe, and are called from sender threads.
 * Events carry names, statuses and error types only: never credentials, documents or
 * Elasticsearch's raw response text, which can quote field values (ADR-0030).
 */
public interface SinkListener {

  /** A listener that ignores every event. */
  SinkListener NONE = new SinkListener() {};

  /**
   * A batch was sealed and handed to a sender ({@code kafkafim_batch_flush_total}).
   *
   * @param key the bulk stream
   * @param reason which limit sealed it
   * @param docs actions in the batch
   * @param bytes uncompressed body size
   */
  default void onFlush(SinkKey key, FlushReason reason, int docs, long bytes) {}

  /**
   * A bulk request got an HTTP response ({@code kafkafim_bulk_requests_total{status}}, {@code
   * kafkafim_bulk_latency_seconds}).
   *
   * @param key the bulk stream
   * @param status HTTP status
   * @param docs actions in the request
   * @param bytes uncompressed body size
   * @param latency time from sending to the parsed response
   */
  default void onBulkResponse(SinkKey key, int status, int docs, long bytes, Duration latency) {}

  /**
   * A bulk request failed as a whole and will be retried after {@code delay}: a connection error
   * ({@code status} is 0 and {@code error} is set), 429, 5xx or an unexpected status. Never DLQ.
   *
   * @param key the bulk stream
   * @param status HTTP status, or 0 for a transport error
   * @param error the transport error, or {@code null} for any answer from Elasticsearch
   * @param attempt consecutive failed attempts for this request, starting at 1
   * @param delay backoff before the next attempt
   */
  default void onRequestRetry(
      SinkKey key, int status, @Nullable Throwable error, int attempt, Duration delay) {}

  /**
   * Elasticsearch rejected the identity with 401 or 403, for the request or for an item: a
   * configuration failure. The sink keeps retrying with backoff and never sends the documents to
   * the DLQ; the supervisor pauses the pipeline and the telemetry slice counts {@code
   * kafkafim_auth_failures_total} (DESIGN §4.2, §5.2.1).
   *
   * <p>Called once per answered bulk request that carries a 401/403: once for a request-level
   * 401/403, and once for a response with one or more 401/403 items (the first such item's status
   * and reason), never once per item. It is not deduplicated: every retry that is rejected again
   * reports again, so the count is the number of responses carrying a 401/403. Called on the sender
   * thread that got the response, after {@link BulkSink#authFailing()} already reflects it and
   * before any outcome of that response reaches the {@link OutcomeCallback}.
   *
   * <p>Senders run concurrently, so this event and {@link #onAuthRecovered} of the same stream can
   * arrive in any order relative to each other: a failure event may come after the stream has
   * recovered on another sender, and a sink that is closing or closed can still report one. Callers
   * treat both events as hints that the state may have changed and re-read {@link
   * BulkSink#authFailing()} of the current sink to decide whether to pause, as the supervisor does.
   *
   * @param key the bulk stream
   * @param status 401 or 403
   * @param reason safe to log: {@code type=<error.type>, root_cause=[...], reason=<error.reason>}
   *     as the sink builds it, or {@code "status <n>"}, {@code "non-JSON, <n> bytes"} and the like
   *     when the body has no error object. The reason of a security error names the user, roles,
   *     action and index, never document data; the raw body is never passed on
   */
  default void onAuthFailure(SinkKey key, int status, String reason) {}

  /**
   * The stream left the auth failure state ({@link BulkSink#authFailing()} turned false): a request
   * was answered with a bulk response, or with a 413 that dead-letters a single document, and no
   * action still waits to retry after an item-level 401/403. A 429, 5xx, transport error or other
   * unexpected status leaves the state as it was. Called at most once per failure episode, on the
   * sender thread that ended it, before any outcome of that response. A hint, like {@link
   * #onAuthFailure}: it can race a failure event from another sender.
   *
   * @param key the bulk stream
   */
  default void onAuthRecovered(SinkKey key) {}

  /**
   * A retryable item error was seen; the document will be sent again unless the poison bound is
   * reached ({@code kafkafim_item_errors_total{retryable=true}}, {@code kafkafim_retries_total}).
   *
   * @param key the bulk stream
   * @param status item status
   * @param errorType Elasticsearch error type, for example {@code es_rejected_execution_exception}
   * @param attempts answered attempts for this document so far
   */
  default void onItemRetry(SinkKey key, int status, String errorType, int attempts) {}

  /**
   * An item failed with an error that is not retried: the document goes to the DLQ ({@code
   * es_error} or {@code version_conflict}; {@code kafkafim_item_errors_total{retryable=false}}).
   *
   * @param key the bulk stream
   * @param status item status
   * @param errorType Elasticsearch error type, for example {@code mapper_parsing_exception}, or
   *     {@code status_<n>} when the item carried none
   */
  default void onItemError(SinkKey key, int status, String errorType) {}

  /**
   * A sealed batch is done: every action has an outcome or went back into the batcher for an item
   * retry. Called on the sender thread, once per batch, for the {@code pipeline.batch} span.
   *
   * @param key the bulk stream
   * @param report what the batch did
   */
  default void onBatchSent(SinkKey key, BatchReport report) {}

  /**
   * A cluster's circuit breaker changed state ({@code kafkafim_breaker_state{es_cluster}}).
   *
   * @param cluster Elasticsearch cluster name
   * @param from previous state
   * @param to new state
   */
  default void onBreakerStateChange(
      String cluster, CircuitBreaker.State from, CircuitBreaker.State to) {}
}
