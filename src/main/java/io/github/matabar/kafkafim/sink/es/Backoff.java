package io.github.matabar.kafkafim.sink.es;

import io.github.resilience4j.core.IntervalFunction;
import java.time.Duration;
import java.util.Objects;

/**
 * Exponential backoff with jitter, computed by Resilience4j's {@link IntervalFunction} (ADR-0016).
 * Used for request-level retries (which never stop) and for item-level retries (bounded by the
 * poison bound).
 *
 * @param initial delay before the first retry
 * @param max cap on any delay
 * @param multiplier growth per attempt
 * @param jitter randomisation factor in {@code [0, 1)}: a delay {@code d} becomes a uniform value
 *     in {@code [d(1-jitter), d(1+jitter)]}, then capped at {@code max}
 */
public record Backoff(Duration initial, Duration max, double multiplier, double jitter) {

  /** 100ms doubling to 30s, ±50%. */
  public static final Backoff DEFAULT =
      new Backoff(Duration.ofMillis(100), Duration.ofSeconds(30), 2.0, 0.5);

  /**
   * Attempts beyond this all get {@code max}; avoids computing huge powers when retrying forever.
   */
  private static final int ATTEMPT_CAP = 64;

  /** Validates the parameters. */
  public Backoff {
    Objects.requireNonNull(initial, "initial");
    Objects.requireNonNull(max, "max");
    if (initial.isNegative() || initial.isZero() || max.compareTo(initial) < 0) {
      throw new IllegalArgumentException("need 0 < initial <= max");
    }
    if (multiplier < 1.0 || jitter < 0.0 || jitter >= 1.0) {
      throw new IllegalArgumentException("need multiplier >= 1 and 0 <= jitter < 1");
    }
  }

  /**
   * The delay before retry number {@code attempt}.
   *
   * @param attempt 1 for the first retry
   * @return the jittered delay, at most {@link #max()}
   */
  public Duration delay(int attempt) {
    IntervalFunction function =
        jitter == 0.0
            ? IntervalFunction.ofExponentialBackoff(initial, multiplier, max)
            : IntervalFunction.ofExponentialRandomBackoff(initial, multiplier, jitter, max);
    long millis = function.apply(Math.clamp(attempt, 1, ATTEMPT_CAP));
    return Duration.ofMillis(Math.min(millis, max.toMillis()));
  }
}
