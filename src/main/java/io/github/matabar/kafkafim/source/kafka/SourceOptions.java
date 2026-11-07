package io.github.matabar.kafkafim.source.kafka;

import java.time.Duration;

/**
 * Runtime knobs of a {@link KafkaPipelineSource} that are not part of the pipeline configuration.
 *
 * @param desiredConcurrency consumers wanted per pod; capped at the partition count of the
 *     pipeline's topics when the source starts (DESIGN §4.1)
 * @param resumeFraction resume fetching once in-flight bytes drop to this fraction of {@code
 *     max-in-flight-bytes} (hysteresis), in {@code [0, 1)}
 * @param drainTimeout on stop, how long to wait for in-flight records to complete before leaving
 *     the group; whatever is left is re-delivered to the next owner
 * @param shutdownTimeout how long the container waits for its consumer threads to stop
 * @param metadataTimeout bound on the topic metadata fetch at start
 * @param commitTimeout bound on the synchronous commit when partitions are revoked
 * @param maxPollTimeout upper bound of the poll timeout; the effective timeout is the smaller of
 *     this and {@code commit-interval}, so idle and paused consumers still commit on time
 * @param authRetryInterval how long a consumer waits before retrying after an authentication or
 *     authorization failure instead of stopping
 */
public record SourceOptions(
    int desiredConcurrency,
    double resumeFraction,
    Duration drainTimeout,
    Duration shutdownTimeout,
    Duration metadataTimeout,
    Duration commitTimeout,
    Duration maxPollTimeout,
    Duration authRetryInterval) {

  /**
   * Defaults: one consumer per available processor, resume at half the budget, 30s drain and 10s
   * container shutdown (together within the 60s shutdown phase, DESIGN §7), 30s metadata, 10s
   * commit, 1s poll, 10s auth retry.
   */
  public static SourceOptions defaults() {
    return new SourceOptions(
        Runtime.getRuntime().availableProcessors(),
        0.5,
        Duration.ofSeconds(30),
        Duration.ofSeconds(10),
        Duration.ofSeconds(30),
        Duration.ofSeconds(10),
        Duration.ofSeconds(1),
        Duration.ofSeconds(10));
  }

  /** Validates the values. */
  public SourceOptions {
    if (desiredConcurrency < 1) {
      throw new IllegalArgumentException("desiredConcurrency must be >= 1");
    }
    if (!(resumeFraction >= 0 && resumeFraction < 1)) {
      throw new IllegalArgumentException("resumeFraction must be in [0, 1)");
    }
  }

  /** A copy with another desired concurrency. */
  public SourceOptions withDesiredConcurrency(int concurrency) {
    return new SourceOptions(
        concurrency,
        resumeFraction,
        drainTimeout,
        shutdownTimeout,
        metadataTimeout,
        commitTimeout,
        maxPollTimeout,
        authRetryInterval);
  }

  /** A copy with another drain timeout. */
  public SourceOptions withDrainTimeout(Duration timeout) {
    return new SourceOptions(
        desiredConcurrency,
        resumeFraction,
        timeout,
        shutdownTimeout,
        metadataTimeout,
        commitTimeout,
        maxPollTimeout,
        authRetryInterval);
  }
}
