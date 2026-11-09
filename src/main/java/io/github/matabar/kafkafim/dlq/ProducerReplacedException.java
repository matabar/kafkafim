package io.github.matabar.kafkafim.dlq;

import org.apache.kafka.common.KafkaException;

/**
 * A send that failed because {@link KafkaDlqClient} was closing its producer to replace it. The
 * cause is the failure that triggered the replacement, so {@link DlqFailure#classify} reports the
 * send by that failure (for example {@code CONFIGURATION} for a missing {@code Write} ACL) rather
 * than by the side effect of the close; the send's own failure is attached as suppressed.
 */
final class ProducerReplacedException extends KafkaException {

  ProducerReplacedException(Throwable trigger, Throwable failure) {
    super(
        "DLQ producer was being replaced after "
            + trigger.getClass().getSimpleName()
            + ": "
            + trigger.getMessage()
            + "; this send failed with "
            + failure.getClass().getSimpleName()
            + ": "
            + failure.getMessage(),
        trigger);
    addSuppressed(failure);
  }
}
