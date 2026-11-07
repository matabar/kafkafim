package io.github.matabar.kafkafim.dlq;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Timing of the DLQ writers; not configurable in the MVP ({@link #DEFAULTS}).
 *
 * @param initialBackoff delay before the second attempt
 * @param maxBackoff upper bound of the doubling delay
 * @param topicCheckTimeout how long a DLQ topic check waits for the broker
 * @param closeTimeout how long closing waits for in-flight sends to be acknowledged
 */
public record DlqOptions(
    Duration initialBackoff,
    Duration maxBackoff,
    Duration topicCheckTimeout,
    Duration closeTimeout) {

  /** 100ms doubling to 30s, ±25% jitter; 10s topic check; 10s close. */
  public static final DlqOptions DEFAULTS =
      new DlqOptions(
          Duration.ofMillis(100),
          Duration.ofSeconds(30),
          Duration.ofSeconds(10),
          Duration.ofSeconds(10));

  /**
   * Validates the options.
   *
   * @throws IllegalArgumentException if a duration is not positive or the backoff bounds are
   *     reversed
   */
  public DlqOptions {
    for (Duration duration :
        new Duration[] {initialBackoff, maxBackoff, topicCheckTimeout, closeTimeout}) {
      Objects.requireNonNull(duration, "durations must be set");
      if (duration.isNegative() || duration.isZero()) {
        throw new IllegalArgumentException("durations must be positive: " + duration);
      }
    }
    if (maxBackoff.compareTo(initialBackoff) < 0) {
      throw new IllegalArgumentException("maxBackoff must not be below initialBackoff");
    }
  }

  /**
   * The delay after a failed attempt: {@code initialBackoff · 2^(attempt-1)}, capped at {@code
   * maxBackoff}, then ±25% jitter.
   *
   * @param attempt the failed attempt's number, starting at 1
   * @return the delay before the next attempt
   */
  Duration backoff(int attempt) {
    long initial = initialBackoff.toMillis();
    long max = maxBackoff.toMillis();
    int shift = Math.min(Math.max(attempt - 1, 0), 30);
    long base = initial << shift;
    if (base <= 0 || base > max) {
      base = max;
    }
    double jitter = 0.75 + ThreadLocalRandom.current().nextDouble() * 0.5;
    return Duration.ofMillis(Math.max(1, Math.round(base * jitter)));
  }
}
