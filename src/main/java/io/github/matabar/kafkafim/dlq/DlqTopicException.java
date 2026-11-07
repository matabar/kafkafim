package io.github.matabar.kafkafim.dlq;

import org.apache.kafka.common.KafkaException;

/**
 * A send attempt that did not reach the producer because the DLQ topic check failed: the topic is
 * missing, the identity may not describe it, or the broker did not answer. Reported through {@link
 * DlqFailure#cause()}; never thrown at the caller.
 */
public final class DlqTopicException extends KafkaException {

  private static final long serialVersionUID = 1L;

  private final TopicStatus status;

  DlqTopicException(TopicStatus status, String message) {
    super(message);
    this.status = status;
  }

  /** What the check found. */
  public TopicStatus status() {
    return status;
  }
}
