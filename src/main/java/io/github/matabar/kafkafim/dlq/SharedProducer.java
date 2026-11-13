package io.github.matabar.kafkafim.dlq;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;

/**
 * The DLQ producer of one {@code (cluster, identity)}, shared by every pipeline's {@link DlqWriter}
 * on it, plus which DLQ topics it has seen exist.
 *
 * <p>Before the first record to a topic, the topic is described: kafka-clients' producer always
 * asks the broker to auto-create a topic it sends to, so producing blind would create a missing DLQ
 * topic on a broker with {@code auto.create.topics.enable=true} (ADR-0021 forbids it), and would
 * otherwise only surface as a metadata timeout. A missing, forbidden or unreachable topic is
 * remembered for one second, so that many sends blocked on it do not describe it once each.
 */
final class SharedProducer {

  private static final long NEGATIVE_TTL_NANOS = Duration.ofSeconds(1).toNanos();

  private final String cluster;
  private final @Nullable String identity;
  private final DlqClient client;
  private final Map<String, Checked> topics = new ConcurrentHashMap<>();

  SharedProducer(String cluster, @Nullable String identity, DlqClient client) {
    this.cluster = cluster;
    this.identity = identity;
    this.client = client;
  }

  String cluster() {
    return cluster;
  }

  @Nullable String identity() {
    return identity;
  }

  DlqClient client() {
    return client;
  }

  /** The topic's status, describing it unless it is known to exist or was just found not to. */
  TopicStatus ensureTopic(String topic, Duration timeout) {
    Checked last = topics.get(topic);
    long now = System.nanoTime();
    if (last != null
        && (last.status == TopicStatus.EXISTS || now - last.atNanos < NEGATIVE_TTL_NANOS)) {
      return last.status;
    }
    return check(topic, timeout);
  }

  /**
   * Describes the topic now and remembers the result, unless the calling thread was interrupted:
   * {@link DlqClient#describe} then answers {@link TopicStatus#UNREACHABLE} without any broker
   * saying so, and other pipelines' sends on this shared producer must not see that for the
   * negative TTL.
   */
  TopicStatus check(String topic, Duration timeout) {
    TopicStatus status = client.describe(topic, timeout);
    if (status == TopicStatus.UNREACHABLE && Thread.currentThread().isInterrupted()) {
      return status;
    }
    topics.put(topic, new Checked(status, System.nanoTime()));
    return status;
  }

  /** Forgets that the topic exists, so that the next send checks again (it may be deleted). */
  void forget(String topic) {
    topics.remove(topic);
  }

  /** {@code identity 'x' on Kafka cluster 'y'}, for messages. */
  String describeIdentity() {
    return (identity == null ? "no identity" : "identity '" + identity + "'")
        + " on Kafka cluster '"
        + cluster
        + "'";
  }

  private record Checked(TopicStatus status, long atNanos) {}
}
