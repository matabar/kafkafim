package io.github.matabar.kafkafim.source.kafka;

import java.util.List;

/**
 * A pipeline's source topics do not exist (or are not visible to its identity) when the source
 * starts. kafkafim never creates them (ADR-0021); the pipeline stays stopped.
 */
public final class MissingTopicsException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  /** The missing topics. */
  private final List<String> topics;

  /**
   * Creates the exception.
   *
   * @param pipeline the pipeline name
   * @param topics the missing topics
   */
  public MissingTopicsException(String pipeline, List<String> topics) {
    super("Pipeline '" + pipeline + "': source topics not found or not visible: " + topics);
    this.topics = List.copyOf(topics);
  }

  /** The missing topics. */
  public List<String> topics() {
    return topics;
  }
}
