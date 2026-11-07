package io.github.matabar.kafkafim.source.kafka;

import java.util.Set;
import java.util.TreeSet;

/**
 * Independent reasons to pause one pipeline (byte budget, a breaker, an auth failure, draining).
 * The pipeline is paused while at least one reason is active; {@code pause} and {@code resume} on
 * the target run only on transitions between none and some.
 */
final class PauseReasons {

  /** What gets paused and resumed. */
  interface Target {

    /** Stop fetching. */
    void pause();

    /** Fetch again. */
    void resume();
  }

  private final Target target;
  private final Set<String> active = new TreeSet<>();

  PauseReasons(Target target) {
    this.target = target;
  }

  /**
   * Activates a reason.
   *
   * @return whether the reason was not active before
   */
  synchronized boolean add(String reason) {
    boolean wasPaused = !active.isEmpty();
    boolean added = active.add(reason);
    if (!wasPaused && added) {
      target.pause();
    }
    return added;
  }

  /**
   * Clears a reason.
   *
   * @return whether the reason was active
   */
  synchronized boolean remove(String reason) {
    boolean removed = active.remove(reason);
    if (removed && active.isEmpty()) {
      target.resume();
    }
    return removed;
  }

  synchronized Set<String> active() {
    return Set.copyOf(active);
  }

  /** Re-applies the current state, e.g. to a container created after reasons were set. */
  synchronized void apply() {
    if (!active.isEmpty()) {
      target.pause();
    }
  }
}
