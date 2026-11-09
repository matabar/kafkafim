package io.github.matabar.kafkafim.sink.es;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.time.Duration;
import java.util.Objects;

/**
 * The per-Elasticsearch-cluster circuit breaker (DESIGN §5.2.1, §7). Only request-level 429, 5xx
 * and transport errors count as failures; auth failures, 413 and item errors do not, since they say
 * nothing about the cluster's health. While the breaker is open every sender for the cluster waits
 * (backpressure); nothing is dropped.
 *
 * @param slidingWindowSize bulk requests in the count-based window
 * @param minimumCalls requests needed before the failure rate is evaluated
 * @param failureRateThreshold percentage of failures that opens the breaker
 * @param waitInOpen time the breaker stays open before letting probe requests through
 * @param probesInHalfOpen requests allowed while half-open
 * @param pollWhileOpen how often a waiting sender asks the breaker again
 */
public record BreakerSettings(
    int slidingWindowSize,
    int minimumCalls,
    float failureRateThreshold,
    Duration waitInOpen,
    int probesInHalfOpen,
    Duration pollWhileOpen) {

  /** Window 20, at least 10 calls, 50% failures, open for 10s, 2 probes, poll every 200ms. */
  public static final BreakerSettings DEFAULT =
      new BreakerSettings(20, 10, 50f, Duration.ofSeconds(10), 2, Duration.ofMillis(200));

  /** Validates the settings. */
  public BreakerSettings {
    Objects.requireNonNull(waitInOpen, "waitInOpen");
    Objects.requireNonNull(pollWhileOpen, "pollWhileOpen");
    if (slidingWindowSize < 1 || minimumCalls < 1 || probesInHalfOpen < 1) {
      throw new IllegalArgumentException("window, minimum calls and probes must be > 0");
    }
  }

  /**
   * The Resilience4j configuration for a cluster.
   *
   * @param responseTimeout the cluster's client response timeout; the half-open safety net is
   *     {@code waitInOpen} plus twice this
   */
  CircuitBreakerConfig toConfig(Duration responseTimeout) {
    return CircuitBreakerConfig.custom()
        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
        .slidingWindowSize(slidingWindowSize)
        .minimumNumberOfCalls(minimumCalls)
        .failureRateThreshold(failureRateThreshold)
        .waitDurationInOpenState(waitInOpen)
        .permittedNumberOfCallsInHalfOpenState(probesInHalfOpen)
        // Safety net: a half-open breaker whose probes never report back reopens instead of
        // holding every sender of the cluster forever. Twice the response timeout, so a slow
        // but legitimate probe is not cut short (ADR-0031).
        .maxWaitDurationInHalfOpenState(halfOpenLimit(responseTimeout))
        .automaticTransitionFromOpenToHalfOpenEnabled(false)
        .slowCallRateThreshold(100f)
        .slowCallDurationThreshold(Duration.ofDays(1))
        .build();
  }

  /**
   * How long the breaker may stay half-open: {@code waitInOpen} plus twice the response timeout.
   */
  Duration halfOpenLimit(Duration responseTimeout) {
    return waitInOpen.plus(responseTimeout.multipliedBy(2));
  }
}
