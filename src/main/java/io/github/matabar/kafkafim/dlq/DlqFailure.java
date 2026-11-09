package io.github.matabar.kafkafim.dlq;

import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.jspecify.annotations.Nullable;

/**
 * One failed DLQ send attempt. The send is retried after {@link #retryIn()}; it never completes
 * successfully without a broker acknowledgement, and never gives up on its own (DESIGN §7: losing
 * data is worse than lag).
 *
 * @param kind configuration problem or transient problem
 * @param cause what failed; Kafka's exception, or a {@link DlqTopicException}
 * @param attempt the failed attempt's number, starting at 1
 * @param retryIn delay before the next attempt
 */
public record DlqFailure(Kind kind, Throwable cause, int attempt, Duration retryIn) {

  /** How a failure is handled operationally. Both kinds are retried forever. */
  public enum Kind {
    /**
     * Retrying alone will not fix it: the DLQ topic is missing, the identity is not authenticated
     * or not authorized ({@code Write}/{@code Describe}), the topic name is invalid, the record is
     * larger than the producer's {@code max.request.size} or the topic's {@code max.message.bytes},
     * or the broker does not support the request. Alert; somebody has to fix provisioning or
     * configuration, after which the next attempt succeeds.
     */
    CONFIGURATION,
    /** Everything else: broker unavailable, timeouts, leader changes, not enough replicas. */
    TRANSIENT
  }

  /**
   * Classifies a send failure.
   *
   * @param failure the exception from the producer or the topic check
   * @return its kind
   */
  public static Kind classify(Throwable failure) {
    // The whole cause chain: a producer in a fatal state throws a plain KafkaException whose cause
    // is the authorization or authentication error that put it there.
    for (Throwable cause = failure; cause != null; cause = next(cause)) {
      if (cause instanceof DlqTopicException topic) {
        return topic.status().isConfigurationFailure() ? Kind.CONFIGURATION : Kind.TRANSIENT;
      }
      if (cause instanceof AuthorizationException
          || cause instanceof AuthenticationException
          || cause instanceof InvalidTopicException
          || cause instanceof UnknownTopicOrPartitionException
          || cause instanceof RecordTooLargeException
          || cause instanceof RecordBatchTooLargeException
          || cause instanceof UnsupportedVersionException
          || cause instanceof ConfigException) {
        return Kind.CONFIGURATION;
      }
    }
    return Kind.TRANSIENT;
  }

  /**
   * Whether the failure may have left kafka-clients' idempotent producer unusable, so that it must
   * be replaced. {@code TransactionManager} puts even an idempotent producer into a fatal error
   * state when an authentication failure hits its pending {@code InitProducerId}, or when one of
   * the others reaches {@code maybeTransitionToErrorState} through a failed batch; every later send
   * then fails with the original cause attached.
   *
   * <p>Not every occurrence is fatal. In kafka-clients 4.2 a {@link ClusterAuthorizationException}
   * on {@code InitProducerId} (an identity without a {@code Write} ACL) is abortable: the producer
   * fails its pending batches with it, resets to {@code UNINITIALIZED} and recovers on its own once
   * the ACL is granted. The batch callbacks cannot tell the two paths apart, so the exception stays
   * here: replacing a producer that would have recovered costs a reconnect, while keeping a fatal
   * one would block the DLQ until a restart.
   *
   * @param failure a send failure
   * @return whether the producer must be recreated
   */
  public static boolean isFatalForProducer(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = next(cause)) {
      if (cause instanceof ClusterAuthorizationException
          || cause instanceof AuthenticationException
          || cause instanceof UnsupportedVersionException
          || cause instanceof ProducerFencedException
          || cause instanceof InvalidPidMappingException) {
        return true;
      }
    }
    return false;
  }

  static Throwable unwrap(Throwable failure) {
    Throwable cause = failure;
    while ((cause instanceof CompletionException || cause instanceof ExecutionException)
        && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  private static @Nullable Throwable next(Throwable cause) {
    Throwable parent = cause.getCause();
    return parent == cause ? null : parent;
  }
}
