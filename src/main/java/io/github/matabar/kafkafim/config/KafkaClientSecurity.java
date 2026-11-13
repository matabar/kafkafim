package io.github.matabar.kafkafim.config;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * The kafka-clients security properties kafkafim derives from the {@code config} model (DESIGN
 * §5.2.1), shared by every Kafka client it builds: the consumers ({@code source.kafka}) and the DLQ
 * producers and their admin clients ({@code dlq}), which may not depend on each other (ADR-0003).
 *
 * <p>The keys are written out because {@code config} does not depend on kafka-clients; a test pins
 * each one to its {@code SslConfigs}/{@code SaslConfigs} constant.
 */
public final class KafkaClientSecurity {

  static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
  static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
  static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";
  static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
  static final String SSL_KEYSTORE_PASSWORD = "ssl.keystore.password";
  static final String SSL_KEY_PASSWORD = "ssl.key.password";
  static final String SSL_KEYSTORE_TYPE = "ssl.keystore.type";
  static final String SASL_MECHANISM = "sasl.mechanism";
  static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

  /** Printed by {@link #redacted} in place of a value that may be a secret. */
  static final String HIDDEN = "[hidden]";

  /** Login modules of the mechanisms kafkafim builds {@code sasl.jaas.config} for. */
  private static final Map<String, String> PASSWORD_LOGIN_MODULES =
      Map.of(
          "PLAIN", "org.apache.kafka.common.security.plain.PlainLoginModule",
          "SCRAM-SHA-256", "org.apache.kafka.common.security.scram.ScramLoginModule",
          "SCRAM-SHA-512", "org.apache.kafka.common.security.scram.ScramLoginModule");

  private KafkaClientSecurity() {}

  /**
   * The security properties for one {@code (cluster, identity)}: the truststore of the cluster's
   * {@code ssl} block, the keystore of the identity's {@code ssl} block, and the mechanism and JAAS
   * configuration of its {@code sasl} block. Only blocks that are present contribute keys, an unset
   * or empty password or store type contributes none, and {@code sasl.jaas.config} is built for
   * PLAIN and SCRAM only (another mechanism sets it through the {@code properties} passthrough).
   * {@code security.protocol} is not included; each client sets it with its own keys.
   *
   * @param cluster the Kafka cluster
   * @param identity the identity, or {@code null} when the pipeline resolves none
   * @return a new mutable map in a stable order; its values may be secrets
   */
  public static Map<String, Object> properties(
      KafkaCluster cluster, KafkaCluster.@Nullable KafkaIdentity identity) {
    Map<String, Object> target = new LinkedHashMap<>();
    KafkaCluster.Trust trust = cluster.ssl();
    if (trust != null) {
      target.put(SSL_TRUSTSTORE_LOCATION, trust.truststoreLocation());
      putIfSet(target, SSL_TRUSTSTORE_PASSWORD, trust.truststorePassword());
      putIfSet(target, SSL_TRUSTSTORE_TYPE, trust.truststoreType());
    }
    if (identity == null) {
      return target;
    }
    KafkaCluster.Keystore keystore = identity.ssl();
    if (keystore != null) {
      target.put(SSL_KEYSTORE_LOCATION, keystore.keystoreLocation());
      putIfSet(target, SSL_KEYSTORE_PASSWORD, keystore.keystorePassword());
      putIfSet(target, SSL_KEY_PASSWORD, keystore.keyPassword());
      putIfSet(target, SSL_KEYSTORE_TYPE, keystore.keystoreType());
    }
    KafkaCluster.Sasl sasl = identity.sasl();
    if (sasl != null) {
      target.put(SASL_MECHANISM, sasl.mechanism());
      String loginModule = PASSWORD_LOGIN_MODULES.get(sasl.mechanism());
      if (loginModule != null) {
        target.put(
            SASL_JAAS_CONFIG,
            loginModule
                + " required username=\""
                + jaasEscape(sasl.username())
                + "\" password=\""
                + jaasEscape(sasl.password())
                + "\";");
      }
    }
    return target;
  }

  /**
   * A printable copy of kafka-clients settings: the values of keys that may carry secrets are
   * replaced, so the result is safe to log.
   *
   * @param settings consumer, producer or admin settings
   * @return the settings as strings, with secret values masked
   */
  public static Map<String, Object> redacted(Map<String, ?> settings) {
    Map<String, Object> copy = new LinkedHashMap<>();
    settings.forEach(
        (key, value) -> copy.put(key, isSecretKey(key) ? HIDDEN : String.valueOf(value)));
    return copy;
  }

  /**
   * Broader than the inline-secret rule on purpose: a log line hides anything that might be one.
   */
  static boolean isSecretKey(String key) {
    String lower = key.toLowerCase(Locale.ROOT);
    return lower.contains("password")
        || lower.contains("jaas")
        || lower.contains("secret")
        || lower.contains("token")
        || lower.contains("credential")
        || lower.contains("user.info")
        || lower.endsWith(".key");
  }

  private static void putIfSet(Map<String, Object> target, String key, @Nullable String value) {
    if (value != null && !value.isEmpty()) {
      target.put(key, value);
    }
  }

  /** JAAS quoted-string escaping: backslash and double quote. */
  static String jaasEscape(@Nullable String value) {
    if (value == null) {
      return "";
    }
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
