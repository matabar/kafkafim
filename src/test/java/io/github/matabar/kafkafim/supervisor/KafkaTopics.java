package io.github.matabar.kafkafim.supervisor;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Topic creation and the seeding producer of the supervisor integration tests. */
final class KafkaTopics {

  private static final Duration READY_TIMEOUT = Duration.ofSeconds(90);

  private KafkaTopics() {}

  /**
   * Creates the topics and returns once the broker serves every partition of them.
   *
   * <p>{@code createTopics} returns when the controller accepted the topics, and the broker's
   * metadata then names a leader before the broker itself has brought the partition up. A produce
   * request in that window is answered {@code NOT_LEADER_OR_FOLLOWER} (#147), so a leader in {@code
   * describeTopics} is not enough to wait for. {@code listOffsets} is: the broker answers it from
   * the partition itself, with the same error until the partition is up, and the admin client
   * retries that error by itself. It does not retry a topic the broker has not heard of yet, which
   * fails the call, so the call is repeated until it succeeds.
   *
   * @param topics topics with an explicit partition count
   */
  static void create(Admin admin, List<NewTopic> topics)
      throws ExecutionException, InterruptedException {
    Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
    for (NewTopic topic : topics) {
      if (topic.numPartitions() < 1) {
        throw new IllegalArgumentException(topic.name() + ": needs an explicit partition count");
      }
      for (int partition = 0; partition < topic.numPartitions(); partition++) {
        partitions.put(new TopicPartition(topic.name(), partition), OffsetSpec.latest());
      }
    }
    admin.createTopics(topics).all().get();
    await()
        .atMost(READY_TIMEOUT)
        .ignoreExceptions()
        .until(() -> admin.listOffsets(partitions).all().get().size() == partitions.size());
  }

  /**
   * The configuration of a producer that seeds test topics.
   *
   * <p>One request in flight per connection. With the default of five, a batch that is retried can
   * be overtaken by the next one. On a partition that has seen nothing from this producer yet, the
   * broker takes the overtaking batch whatever its sequence number, and then refuses the retried
   * one with {@code OUT_OF_ORDER_SEQUENCE_NUMBER} on every attempt until the delivery timeout: the
   * 120 s "Expiring 7 record(s)" of #147. With one request in flight nothing can overtake.
   */
  static Map<String, Object> seedingProducerConfig(String bootstrapServers) {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    return config;
  }
}
