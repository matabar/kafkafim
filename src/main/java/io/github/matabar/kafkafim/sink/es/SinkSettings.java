package io.github.matabar.kafkafim.sink.es;

import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.config.Pipeline;
import io.github.matabar.kafkafim.config.Pipeline.OnVersionConflict;
import java.time.Duration;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Everything one {@link BulkSink} needs besides its client and breaker: the cluster's batching
 * limits (with the pipeline's {@code latency.max-age} override already applied), the pipeline's
 * poison bound and version-conflict policy, and the backoff.
 *
 * @param maxDocs actions per bulk request
 * @param maxBytes uncompressed body bytes per bulk request; an action larger than this is sent
 *     alone
 * @param maxAge how long the oldest action of a batch may wait
 * @param inFlight concurrent bulk requests for this stream
 * @param maxAttempts item-level retryable errors per document before the DLQ (poison bound)
 * @param maxElapsed time since a document's first item-level retryable error before the DLQ; zero
 *     sends a document to the DLQ on its first retryable error
 * @param onVersionConflict {@code ignore} (stale, success) or {@code dlq}
 * @param backoff delays between retries
 */
public record SinkSettings(
    int maxDocs,
    long maxBytes,
    Duration maxAge,
    int inFlight,
    int maxAttempts,
    Duration maxElapsed,
    OnVersionConflict onVersionConflict,
    Backoff backoff) {

  /** Validates the settings. */
  public SinkSettings {
    Objects.requireNonNull(maxAge, "maxAge");
    Objects.requireNonNull(maxElapsed, "maxElapsed");
    Objects.requireNonNull(onVersionConflict, "onVersionConflict");
    Objects.requireNonNull(backoff, "backoff");
    if (maxDocs < 1 || maxBytes < 1 || inFlight < 1 || maxAttempts < 1) {
      throw new IllegalArgumentException("maxDocs, maxBytes, inFlight and maxAttempts must be > 0");
    }
    if (maxAge.isNegative() || maxAge.isZero() || maxElapsed.isNegative()) {
      throw new IllegalArgumentException("maxAge must be positive and maxElapsed not negative");
    }
  }

  /**
   * Builds the settings from the bound configuration (DESIGN §5.1, §5.2).
   *
   * @param bulk the cluster's {@code bulk} block
   * @param latencyMaxAge the pipeline's {@code latency.max-age}, or {@code null} to use the
   *     cluster's {@code bulk.max-age}
   * @param dlq the pipeline's {@code dlq} block (poison bound)
   * @param onVersionConflict the pipeline's {@code document.on-version-conflict}
   * @return the settings with the default {@link Backoff}
   */
  public static SinkSettings of(
      ElasticsearchCluster.Bulk bulk,
      @Nullable Duration latencyMaxAge,
      Pipeline.Dlq dlq,
      OnVersionConflict onVersionConflict) {
    return new SinkSettings(
        bulk.maxDocs(),
        bulk.maxBytes().toBytes(),
        latencyMaxAge != null ? latencyMaxAge : bulk.maxAge(),
        bulk.inFlight(),
        dlq.maxAttempts(),
        dlq.maxElapsed(),
        onVersionConflict,
        Backoff.DEFAULT);
  }
}
