package io.github.matabar.kafkafim.supervisor;

import java.util.Locale;

/**
 * The state of one pipeline, the {@code state} label of {@code kafkafim_pipeline_state} (DESIGN
 * §4.6, §10).
 */
public enum PipelineState {
  /** Preflight is running, or waiting to retry after a transient failure (broker unreachable). */
  STARTING,
  /** Consuming. */
  RUNNING,
  /**
   * Started, but fetching is paused for a supervisor reason: an Elasticsearch breaker is open, an
   * identity is rejected, or the DLQ writer is blocked. The byte-budget pause is ordinary
   * backpressure and does not count.
   */
  PAUSED,
  /** A preflight check found a configuration problem; stays stopped until reload or restart. */
  FAILED_PREFLIGHT,
  /** Not started, or stopped. */
  STOPPED;

  /** The metric label value, e.g. {@code failed_preflight}. */
  public String label() {
    return name().toLowerCase(Locale.ROOT);
  }

  /** Whether the pipeline's source is started (running or paused). */
  public boolean isStarted() {
    return this == RUNNING || this == PAUSED;
  }
}
