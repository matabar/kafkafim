package io.github.matabar.kafkafim.dlq;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One {@link KafkaProducer} (thread-safe, shared by every pipeline that dead-letters through the
 * same {@code (cluster, identity)}) and one {@link Admin} for the topic check, created lazily on
 * the first check.
 *
 * <p>An idempotent producer in kafka-clients' fatal error state fails every later send, even after
 * the cause is fixed. A send failure that can put it there ({@link DlqFailure#isFatalForProducer})
 * therefore closes and replaces the producer, so the next attempt runs on a fresh one.
 *
 * <p>A missing {@code Write} ACL is usually not such a case in kafka-clients 4.2: {@code
 * CLUSTER_AUTHORIZATION_FAILED} on {@code InitProducerId} is abortable there. The producer fails
 * its pending batches, returns to {@code UNINITIALIZED} and retries {@code InitProducerId} from its
 * I/O thread on its own, so it recovers once the ACL is granted. It turns fatal only when the error
 * reaches {@code TransactionManager.maybeTransitionToErrorState} through a failed batch. The
 * aborted batches still fail with {@code ClusterAuthorizationException}, so the producer is
 * replaced on the abortable path too: a cheap safety net, not a requirement. kafka-clients logs
 * "Aborting producer batches due to fatal error" at {@code ERROR} on that path, and this class's
 * WARN says the producer may be in a fatal state; neither means data was lost (DESIGN §7).
 *
 * <p>Each send completes exactly once, although kafka-clients may report one send twice: {@code
 * KafkaProducer.send} appends the record and only then throws when its transaction manager is in an
 * error state, so the appended batch still fails or succeeds later through the callback. The first
 * report wins (a thrown exception is claimed before the replacement closes the producer) and later
 * ones are dropped; a dropped success means the writer resends a record that was written, a
 * duplicate the DLQ contract allows. A send that fails as a side effect of the replacement (the
 * close aborts its batch, or it reached the producer after the close began) is reported as a {@link
 * ProducerReplacedException} caused by the failure that triggered the replacement, so it is
 * classified by that failure and not as a transient close.
 */
final class KafkaDlqClient implements DlqClient {

  private static final Logger log = LoggerFactory.getLogger(KafkaDlqClient.class);

  private final Map<String, Object> producerSettings;
  private final Function<Map<String, Object>, Producer<byte[], byte[]>> producers;
  private final Object producerLock = new Object();
  private volatile Generation generation;
  private final Map<String, Object> adminSettings;
  private final Object adminLock = new Object();
  private @Nullable Admin admin;
  private volatile boolean closed;
  private boolean replacing;
  private volatile boolean needsProducer;

  KafkaDlqClient(Map<String, Object> producerSettings) {
    this(producerSettings, KafkaProducer::new);
  }

  KafkaDlqClient(
      Map<String, Object> producerSettings,
      Function<Map<String, Object>, Producer<byte[], byte[]>> producers) {
    log.info("Creating DLQ producer with settings {}", ProducerSettings.redacted(producerSettings));
    this.producerSettings = producerSettings;
    this.producers = producers;
    this.generation = new Generation(producers.apply(producerSettings));
    this.adminSettings = ProducerSettings.adminSettings(producerSettings);
  }

  @Override
  public void send(ProducerRecord<byte[], byte[]> record, Callback callback) {
    Generation current = currentGeneration();
    AtomicBoolean reported = new AtomicBoolean();
    try {
      current.producer.send(
          record,
          (metadata, exception) -> {
            boolean first = reported.compareAndSet(false, true);
            Exception failure = exception == null ? null : attributed(current, exception);
            if (failure != null) {
              replaceIfFatal(current, failure);
            }
            if (first) {
              callback.onCompletion(metadata, failure);
            }
          });
    } catch (RuntimeException e) {
      // Claimed before the replacement: closing the producer aborts the batch kafka-clients may
      // have appended before it threw, and that callback must not win.
      boolean first = reported.compareAndSet(false, true);
      RuntimeException failure = attributed(current, e);
      replaceIfFatal(current, failure);
      if (first) {
        throw failure;
      }
    }
  }

  /**
   * The failure to report for a send on {@code generation}: wrapped in a {@link
   * ProducerReplacedException} when that producer is being replaced and the failure would otherwise
   * count as transient, which is then a side effect of the close.
   */
  private static Exception attributed(Generation generation, Exception failure) {
    ProducerReplacedException replaced = replaced(generation, failure);
    return replaced == null ? failure : replaced;
  }

  private static RuntimeException attributed(Generation generation, RuntimeException failure) {
    ProducerReplacedException replaced = replaced(generation, failure);
    return replaced == null ? failure : replaced;
  }

  private static @Nullable ProducerReplacedException replaced(
      Generation generation, Exception failure) {
    Throwable trigger = generation.replacedBecause;
    if (trigger == null
        || trigger == failure
        || DlqFailure.classify(failure) != DlqFailure.Kind.TRANSIENT) {
      return null;
    }
    return new ProducerReplacedException(trigger, failure);
  }

  /**
   * The producer to send with, first creating the replacement of a fatal one if that has not
   * succeeded yet. A creation failure is thrown as is (a {@code ConfigException} or {@code
   * KafkaException}), so the writer classifies it by its cause and retries with backoff.
   */
  private Generation currentGeneration() {
    if (!needsProducer) {
      return generation;
    }
    synchronized (producerLock) {
      if (closed) {
        throw new IllegalStateException("DLQ producer is closed");
      }
      if (needsProducer) {
        generation = new Generation(producers.apply(producerSettings));
        needsProducer = false;
      }
      return generation;
    }
  }

  /** The producer in use; a new instance after a fatal error. */
  Producer<byte[], byte[]> producer() {
    return generation.producer;
  }

  /**
   * Replaces the producer after a fatal error. Any {@code AuthenticationException} counts, so a
   * healthy producer that only saw a transient SASL failure is replaced too; its aborted batches
   * are retried on the new one (possible duplicates, never loss), at most once per producer.
   *
   * <p>The old producer is closed before the new one is created, so the two never share the {@code
   * client.id}'s app-info MBean. The close happens outside the lock: on the synchronous path it
   * joins the old I/O thread, whose aborted-batch callbacks come back here and must not wait. If
   * creating the new one fails, every later send tries again until one succeeds.
   *
   * <p>kafka-clients cannot say whether the producer really is fatal ({@link
   * DlqFailure#isFatalForProducer}), hence "may be" in the log line.
   */
  private void replaceIfFatal(Generation failed, Throwable failure) {
    if (!DlqFailure.isFatalForProducer(failure)) {
      return;
    }
    synchronized (producerLock) {
      if (closed || replacing || needsProducer || generation != failed) {
        return;
      }
      replacing = true;
      // Before the close, so that the batches it aborts are reported by this failure.
      failed.replacedBecause = failure;
    }
    log.warn(
        "DLQ producer {} may be in a fatal state ({}); replacing it",
        producerSettings.get("client.id"),
        DlqFailure.unwrap(failure).getClass().getSimpleName());
    try {
      // Possibly on the failed producer's own I/O thread, where kafka-clients forces a zero
      // timeout.
      failed.producer.close(Duration.ZERO);
    } catch (RuntimeException e) {
      log.warn("Closing the failed DLQ producer failed", e);
    } finally {
      synchronized (producerLock) {
        replacing = false;
        // From here every send first creates the replacement; the first attempt is now.
        needsProducer = !closed;
      }
    }
    try {
      currentGeneration();
    } catch (RuntimeException e) {
      log.warn("Recreating the DLQ producer failed; the next send tries again", e);
    }
  }

  @Override
  public TopicStatus describe(String topic, Duration timeout) {
    Admin client = admin();
    if (client == null) {
      return TopicStatus.UNREACHABLE;
    }
    try {
      client
          .describeTopics(
              List.of(topic), new DescribeTopicsOptions().timeoutMs((int) timeout.toMillis()))
          .allTopicNames()
          .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      return TopicStatus.EXISTS;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof UnknownTopicOrPartitionException) {
        return TopicStatus.MISSING;
      }
      if (cause instanceof AuthorizationException || cause instanceof AuthenticationException) {
        return TopicStatus.NOT_AUTHORIZED;
      }
      return TopicStatus.UNREACHABLE;
    } catch (TimeoutException e) {
      return TopicStatus.UNREACHABLE;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return TopicStatus.UNREACHABLE;
    }
  }

  private @Nullable Admin admin() {
    synchronized (adminLock) {
      if (closed) {
        return null;
      }
      if (admin == null) {
        admin = Admin.create(adminSettings);
      }
      return admin;
    }
  }

  @Override
  public void close(Duration timeout) {
    Admin toClose;
    synchronized (adminLock) {
      toClose = admin;
      synchronized (producerLock) {
        closed = true;
      }
    }
    try {
      generation.producer.close(timeout);
    } finally {
      if (toClose != null) {
        toClose.close(Duration.ZERO);
      }
    }
  }

  /** One producer instance, and the failure that made this client replace it. */
  private static final class Generation {
    final Producer<byte[], byte[]> producer;
    volatile @Nullable Throwable replacedBecause;

    Generation(Producer<byte[], byte[]> producer) {
      this.producer = producer;
    }
  }
}
