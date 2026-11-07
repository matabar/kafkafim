package io.github.matabar.kafkafim.dlq;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 */
final class KafkaDlqClient implements DlqClient {

  private static final Logger log = LoggerFactory.getLogger(KafkaDlqClient.class);

  private final Producer<byte[], byte[]> producer;
  private final Map<String, Object> adminSettings;
  private final Object adminLock = new Object();
  private @Nullable Admin admin;
  private boolean closed;

  KafkaDlqClient(Map<String, Object> producerSettings) {
    log.info("Creating DLQ producer with settings {}", ProducerSettings.redacted(producerSettings));
    this.producer = new KafkaProducer<>(producerSettings);
    this.adminSettings = ProducerSettings.adminSettings(producerSettings);
  }

  @Override
  public void send(ProducerRecord<byte[], byte[]> record, Callback callback) {
    producer.send(record, callback);
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
      closed = true;
      toClose = admin;
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
