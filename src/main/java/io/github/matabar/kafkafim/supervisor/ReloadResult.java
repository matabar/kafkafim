package io.github.matabar.kafkafim.supervisor;

import java.util.List;

/**
 * The outcome of one reload, as {@code POST /actuator/reload} returns it (DESIGN §5.6). Names and
 * violations only, never a value of the configuration, so no secret can appear.
 *
 * @param result {@code applied} (pipelines changed), {@code unchanged} (valid, nothing to do) or
 *     {@code rejected} (invalid or unreadable; the running configuration stays)
 * @param generation the configuration generation running after the reload (1 at startup, +1 per
 *     applied change)
 * @param added pipelines started
 * @param removed pipelines drained and stopped
 * @param restarted pipelines drained and started again, each with why
 * @param retried unchanged pipelines that were in {@code failed_preflight} or {@code failed} and
 *     were started once more; their outcome shows in {@code kafkafim_pipeline_state}, not here
 * @param unchanged pipelines left untouched
 * @param errors every violation of a rejected tree, as {@code <property path>: <problem>}
 */
public record ReloadResult(
    String result,
    long generation,
    List<String> added,
    List<String> removed,
    List<Restart> restarted,
    List<String> retried,
    List<String> unchanged,
    List<String> errors) {

  /** {@link #result()} of an applied reload. */
  public static final String APPLIED = "applied";

  /** {@link #result()} of a valid reload with nothing to do. */
  public static final String UNCHANGED = "unchanged";

  /** {@link #result()} of a rejected reload. */
  public static final String REJECTED = "rejected";

  /**
   * A restarted pipeline.
   *
   * @param pipeline its name
   * @param reasons what changed, by name (no values)
   */
  public record Restart(String pipeline, List<String> reasons) {}

  /** Whether the tree was accepted (applied or unchanged). */
  public boolean accepted() {
    return !REJECTED.equals(result);
  }
}
