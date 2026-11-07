package io.github.matabar.kafkafim.source.kafka;

import io.github.matabar.kafkafim.config.KafkaCluster;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/**
 * One {@link ConsumerFactory} per {@code (Kafka cluster, identity)} for one bound configuration
 * (DESIGN §5.2.1). A factory holds settings only; every container still creates and owns its own
 * {@code KafkaConsumer}s. A reload builds a new instance from the new configuration, so nothing
 * here is ever invalidated in place.
 */
public final class ConsumerFactories {

  private final KafkafimProperties properties;
  private final Map<Key, ConsumerFactory<byte[], byte[]>> factories = new ConcurrentHashMap<>();

  /**
   * Creates an empty cache over a bound configuration.
   *
   * @param properties the validated configuration
   */
  public ConsumerFactories(KafkafimProperties properties) {
    this.properties = properties;
  }

  /**
   * The factory for a cluster and identity, built on first use.
   *
   * @param cluster name of a {@code kafkafim.kafka.clusters} entry
   * @param identity name of one of its identities, or {@code null} for none
   * @return the cached factory
   * @throws IllegalArgumentException if the cluster or identity is unknown, or the cluster's
   *     passthrough sets a key kafkafim owns
   */
  public ConsumerFactory<byte[], byte[]> get(String cluster, @Nullable String identity) {
    return factories.computeIfAbsent(new Key(cluster, identity), this::create);
  }

  /** Number of distinct factories built so far. */
  int size() {
    return factories.size();
  }

  private ConsumerFactory<byte[], byte[]> create(Key key) {
    KafkaCluster cluster = properties.kafka().clusters().get(key.cluster());
    if (cluster == null) {
      throw new IllegalArgumentException("Unknown Kafka cluster '" + key.cluster() + "'");
    }
    KafkaCluster.KafkaIdentity identity = null;
    if (key.identity() != null) {
      identity = cluster.identities().get(key.identity());
      if (identity == null) {
        throw new IllegalArgumentException(
            "Unknown identity '" + key.identity() + "' on Kafka cluster '" + key.cluster() + "'");
      }
    }
    return new DefaultKafkaConsumerFactory<>(ConsumerSettings.forIdentity(cluster, identity));
  }

  private record Key(String cluster, @Nullable String identity) {}
}
