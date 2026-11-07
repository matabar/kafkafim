package io.github.matabar.kafkafim.sink.es;

/**
 * Receives the final outcome of every action submitted to a {@link BulkSink}, exactly once per
 * action, from a sender thread. It must be quick and thread-safe; an exception it throws is logged
 * and otherwise ignored.
 *
 * @param <H> type of the actions' completion handle
 */
@FunctionalInterface
public interface OutcomeCallback<H> {

  /**
   * Called once per action when its outcome is final.
   *
   * @param action the submitted action
   * @param outcome its final outcome
   */
  void onOutcome(BulkAction<H> action, ActionOutcome outcome);
}
