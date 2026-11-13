package io.github.matabar.kafkafim.dlq;

import io.github.matabar.kafkafim.config.KafkaClientSecurity;
import io.github.matabar.kafkafim.config.KafkaCluster;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jspecify.annotations.Nullable;

/**
 * Builds kafka-clients producer (and admin) settings for the DLQ producer of one {@code (cluster,
 * identity)} from the {@code config} model (DESIGN §5.1, §5.2.1).
 *
 * <p>Three layers, later ones winning: kafkafim's {@link #DEFAULTS}; the cluster's {@code
 * properties} passthrough, restricted to keys a producer knows (or that neither a producer nor a
 * consumer knows, such as plugin settings), because the same map also feeds the consumers; and
 * kafkafim's own settings. The passthrough may not set a key kafkafim owns: {@link #OWNED_KEYS}
 * always, plus the security keys derived from the cluster's {@code ssl} block or the identity's
 * {@code sasl}/{@code ssl} blocks when those blocks are present. Such a passthrough is rejected
 * with {@link IllegalArgumentException} naming only the keys, never the values.
 *
 * <p>The security keys come from {@link KafkaClientSecurity}, which the consumers share.
 */
public final class ProducerSettings {

  /** Fixed: every DLQ record is acknowledged by all in-sync replicas. */
  static final String ACKS = "all";

  /** Fixed: kafka-clients' retries of one send stop after this; kafkafim then resends. */
  static final int DELIVERY_TIMEOUT_MS = 120_000;

  /** Fixed: the idempotent producer's upper bound, so its internal retries keep the order. */
  static final int MAX_IN_FLIGHT = 5;

  /**
   * Keys the passthrough can never set: durability, idempotence and serialization of DLQ records
   * depend on them, or kafkafim sets them per {@code (cluster, identity)}. {@code transactional.id}
   * is owned so that it stays unset: a transactional producer would need fencing kafkafim does not
   * do; {@code interceptor.classes} likewise, since an interceptor could change DLQ records.
   */
  public static final Set<String> OWNED_KEYS =
      Set.of(
          CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
          ProducerConfig.CLIENT_ID_CONFIG,
          ProducerConfig.ACKS_CONFIG,
          ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
          ProducerConfig.RETRIES_CONFIG,
          ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
          ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          ProducerConfig.TRANSACTIONAL_ID_CONFIG,
          ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);

  /**
   * Defaults the passthrough may override.
   *
   * <ul>
   *   <li>{@code compression.type=lz4}: cheap, and compression changes neither order nor
   *       durability.
   *   <li>{@code max.request.size} of 16 MiB: a record up to twice the default {@code
   *       max-record-bytes} plus kafkafim's headers fits; the DLQ topic's {@code max.message.bytes}
   *       must allow it too.
   *   <li>{@code max.block.ms} of 10s: bounds how long one send attempt waits for metadata or
   *       buffer space before kafkafim retries it.
   * </ul>
   */
  public static final Map<String, Object> DEFAULTS =
      Map.of(
          ProducerConfig.COMPRESSION_TYPE_CONFIG,
          "lz4",
          ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
          16 * 1024 * 1024,
          ProducerConfig.MAX_BLOCK_MS_CONFIG,
          10_000);

  private ProducerSettings() {}

  /**
   * The DLQ producer's {@code client.id}: {@code kafkafim-<cluster>-<identity>-dlq}, or {@code
   * kafkafim-<cluster>-dlq} without an identity.
   *
   * @param clusterName the Kafka cluster's name
   * @param identityName the identity's name, or {@code null}
   * @return the client id
   */
  public static String clientId(String clusterName, @Nullable String identityName) {
    return "kafkafim-" + clusterName + (identityName == null ? "" : "-" + identityName) + "-dlq";
  }

  /**
   * The producer settings for one {@code (cluster, identity)}.
   *
   * @param clusterName the Kafka cluster's name
   * @param cluster the Kafka cluster
   * @param identityName the identity's name, or {@code null} when the pipeline resolves none (only
   *     valid on a cluster that does not require SASL, which the config validator guarantees)
   * @param identity the identity, or {@code null}
   * @return mutable settings map
   * @throws IllegalArgumentException if the passthrough sets a key kafkafim owns
   */
  public static Map<String, Object> forIdentity(
      String clusterName,
      KafkaCluster cluster,
      @Nullable String identityName,
      KafkaCluster.@Nullable KafkaIdentity identity) {
    Map<String, Object> owned = new LinkedHashMap<>();
    owned.put(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, String.join(",", cluster.bootstrapServers()));
    owned.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, cluster.securityProtocol().name());
    owned.put(ProducerConfig.CLIENT_ID_CONFIG, clientId(clusterName, identityName));
    owned.put(ProducerConfig.ACKS_CONFIG, ACKS);
    owned.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    owned.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    owned.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT);
    owned.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, DELIVERY_TIMEOUT_MS);
    owned.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    owned.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    owned.putAll(KafkaClientSecurity.properties(cluster, identity));

    Set<String> rejected = new TreeSet<>();
    for (String key : cluster.properties().keySet()) {
      if (OWNED_KEYS.contains(key) || owned.containsKey(key)) {
        rejected.add(key);
      }
    }
    if (!rejected.isEmpty()) {
      throw new IllegalArgumentException(
          "Kafka cluster '"
              + clusterName
              + "' properties must not set keys the DLQ producer owns: "
              + rejected);
    }
    Map<String, Object> settings = new LinkedHashMap<>(DEFAULTS);
    cluster
        .properties()
        .forEach((key, value) -> {
          if (isProducerKey(key)) {
            settings.put(key, value);
          }
        });
    settings.putAll(owned);
    return settings;
  }

  /**
   * Admin settings for the DLQ topic check, derived from producer settings: the keys an admin
   * client knows, with the client id suffixed {@code -admin}.
   *
   * @param producerSettings settings from {@link #forIdentity}
   * @return mutable admin settings
   */
  public static Map<String, Object> adminSettings(Map<String, Object> producerSettings) {
    Set<String> adminKeys = AdminClientConfig.configNames();
    Map<String, Object> admin = new LinkedHashMap<>();
    producerSettings.forEach((key, value) -> {
      if (adminKeys.contains(key)) {
        admin.put(key, value);
      }
    });
    admin.put(
        AdminClientConfig.CLIENT_ID_CONFIG,
        producerSettings.get(ProducerConfig.CLIENT_ID_CONFIG) + "-admin");
    return admin;
  }

  /**
   * A printable copy of settings: values of keys that may carry secrets are replaced, so the result
   * is safe to log.
   *
   * @param settings producer or admin settings
   * @return the settings with secret values masked
   */
  public static Map<String, Object> redacted(Map<String, ?> settings) {
    return KafkaClientSecurity.redacted(settings);
  }

  /** Producer keys, plus keys neither client knows (plugins); consumer-only keys are dropped. */
  private static boolean isProducerKey(String key) {
    return ProducerConfig.configNames().contains(key)
        || !ConsumerConfig.configNames().contains(key);
  }
}
