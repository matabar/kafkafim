package io.github.matabar.kafkafim.dlq;

/**
 * Result of describing a DLQ topic with the DLQ producer's identity (ADR-0021: kafkafim checks,
 * never creates).
 */
public enum TopicStatus {
  /** The topic exists and the identity may describe it. */
  EXISTS,
  /** The broker says the topic does not exist. A configuration failure. */
  MISSING,
  /**
   * The identity may not describe the topic, or does not authenticate. A configuration failure;
   * without {@code Describe} the broker does not say whether the topic exists.
   */
  NOT_AUTHORIZED,
  /** No answer in time (broker unreachable, timeout). Transient. */
  UNREACHABLE;

  /** Whether the status is a configuration problem rather than a transient one. */
  public boolean isConfigurationFailure() {
    return this == MISSING || this == NOT_AUTHORIZED;
  }
}
