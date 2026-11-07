package io.github.matabar.kafkafim.dlq;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 * <p>An idempotent producer that hits a fatal error ({@link DlqFailure#isFatalForProducer}, e.g.
 * {@code CLUSTER_AUTHORIZATION_FAILED} on its first {@code InitProducerId} because the identity has
 * no {@code Write} ACL yet) fails every later send, even after the ACL is fixed. Such a producer is
 * closed and replaced, so the next attempt runs on a fresh one.
 */
final class KafkaDlqClient implements DlqClient {

  private static final Logger log = LoggerFactory.getLogger(KafkaDlqClient.class);

  private final Map<String, Object> producerSettings;
  private final Function<Map<String, Object>, Producer<byte[], byte[]>> producers;
  private final Object producerLock = new Object();
  private volatile Producer<byte[], byte[]> producer;
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
    this.producer = producers.apply(producerSettings);
    this.adminSettings = ProducerSettings.adminSettings(producerSettings);
  }

  @Override
  public void send(ProducerRecord<byte[], byte[]> record, Callback callback) {
    Producer<byte[], byte[]> current = currentProducer();
    try {
      current.send(
          record,
          (metadata, exception) -> {
            if (exception != null) {
              replaceIfFatal(current, exception);
            }
            callback.onCompletion(metadata, exception);
          });
    } catch (RuntimeException e) {
      replaceIfFatal(current, e);
      throw e;
    }
  }

  /**
   * The producer to send with, first creating the replacement of a fatal one if that has not
   * succeeded yet. A creation failure is thrown as is (a {@code ConfigException} or {@code
   * KafkaException}), so the writer classifies it by its cause and retries with backoff.
   */
  private Producer<byte[], byte[]> currentProducer() {
    if (!needsProducer) {
      return producer;
    }
    synchronized (producerLock) {
      if (closed) {
        throw new IllegalStateException("DLQ producer is closed");
      }
      if (needsProducer) {
        producer = producers.apply(producerSettings);
        needsProducer = false;
      }
      return producer;
    }
  }

  /** The producer in use; a new instance after a fatal error. */
  Producer<byte[], byte[]> producer() {
    return producer;
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
   */
  private void replaceIfFatal(Producer<byte[], byte[]> failed, Throwable failure) {
    if (!DlqFailure.isFatalForProducer(failure)) {
      return;
    }
    synchronized (producerLock) {
      if (closed || replacing || producer != failed) {
        return;
      }
      replacing = true;
    }
    log.warn(
        "DLQ producer {} is in a fatal state ({}); replacing it",
        producerSettings.get("client.id"),
        DlqFailure.unwrap(failure).getClass().getSimpleName());
    try {
      // Possibly on the failed producer's own I/O thread, where kafka-clients forces a zero
      // timeout.
      failed.close(Duration.ZERO);
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
      currentProducer();
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
      producer.close(timeout);
    } finally {
      if (toClose != null) {
        toClose.close(Duration.ZERO);
      }
    }
  }
}
