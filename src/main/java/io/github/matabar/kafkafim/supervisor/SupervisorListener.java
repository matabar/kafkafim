package io.github.matabar.kafkafim.supervisor;

import java.util.Set;

/**
 * Hook for telemetry: pipeline state changes and supervisor pause reasons. Beans of this type are
 * picked up by the supervisor. Called from supervisor, sink and DLQ threads; must be quick and must
 * not throw (an exception is logged and ignored).
 */
public interface SupervisorListener {

  /**
   * A pipeline's state changed.
   *
   * @param pipeline pipeline name
   * @param from previous state
   * @param to new state
   */
  default void onStateChange(String pipeline, PipelineState from, PipelineState to) {}

  /**
   * The supervisor's pause reasons of a pipeline changed (for example {@code es-breaker:<cluster>},
   * {@code es-auth:<cluster>/<identity>}, {@code dlq-blocked}).
   *
   * @param pipeline pipeline name
   * @param reasons the active reasons, possibly empty
   */
  default void onPauseReasonsChange(String pipeline, Set<String> reasons) {}
}
