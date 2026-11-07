package io.github.matabar.kafkafim.source.kafka;

/**
 * The in-flight byte budget of one pipeline (DESIGN §7, ADR-0010): bytes delivered to the handler
 * and not yet completed or abandoned. Crossing {@code max} asks for a pause; dropping to {@code
 * resumeAt} or below asks for a resume, so a pipeline near its budget does not flap.
 *
 * <p>The budget never blocks: a poll that crosses it is delivered whole, and the pause takes effect
 * before the next poll. The overshoot is bounded by one poll per consumer.
 */
final class ByteBudget {

  /** Receives pause and resume requests; called with the budget's lock held. */
  interface Listener {

    /**
     * The budget asks for a pause ({@code true}) or allows a resume ({@code false}).
     *
     * @param exhausted whether the budget is exhausted
     */
    void exhausted(boolean exhausted);
  }

  private final long max;
  private final long resumeAt;
  private final Listener listener;
  private long outstanding;
  private boolean exhausted;

  /**
   * Creates a budget.
   *
   * @param max outstanding bytes at which to pause; positive
   * @param resumeAt outstanding bytes at or below which to resume; {@code 0 <= resumeAt < max}
   * @param listener receives pause and resume requests
   */
  ByteBudget(long max, long resumeAt, Listener listener) {
    if (max <= 0 || resumeAt < 0 || resumeAt >= max) {
      throw new IllegalArgumentException(
          "Need 0 <= resumeAt < max, got max=" + max + ", resumeAt=" + resumeAt);
    }
    this.max = max;
    this.resumeAt = resumeAt;
    this.listener = listener;
  }

  /** Adds delivered bytes. */
  synchronized void acquire(long bytes) {
    outstanding += bytes;
    if (!exhausted && outstanding >= max) {
      exhausted = true;
      listener.exhausted(true);
    }
  }

  /** Removes completed or abandoned bytes. */
  synchronized void release(long bytes) {
    outstanding -= bytes;
    if (exhausted && outstanding <= resumeAt) {
      exhausted = false;
      listener.exhausted(false);
    }
  }

  synchronized long outstanding() {
    return outstanding;
  }

  synchronized boolean isExhausted() {
    return exhausted;
  }
}
