package io.github.matabar.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.matabar.kafkafim.config.KafkaCluster;
import io.github.matabar.kafkafim.config.KafkaCluster.KafkaIdentity;
import io.github.matabar.kafkafim.config.KafkaCluster.Keystore;
import io.github.matabar.kafkafim.config.KafkaCluster.Sasl;
import io.github.matabar.kafkafim.config.KafkaCluster.SecurityProtocol;
import io.github.matabar.kafkafim.config.KafkaCluster.Trust;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.util.unit.DataSize;

class ConsumerSettingsTest {

  private static final Sasl SCRAM = new Sasl("SCRAM-SHA-256", "orders-reader", "s3cr3t");

  private static KafkaCluster cluster(
      SecurityProtocol protocol, Trust trust, Map<String, String> properties) {
    return new KafkaCluster(
        List.of("k1:9093", "k2:9093"), protocol, trust, Map.of(), null, properties);
  }

  @Test
  void plaintextClusterGetsKafkafimDefaults() {
    Map<String, Object> settings =
        ConsumerSettings.forIdentity(cluster(null, null, Map.of()), null);

    assertThat(settings)
        .containsEntry("bootstrap.servers", "k1:9093,k2:9093")
        .containsEntry("security.protocol", "PLAINTEXT")
        .containsEntry("enable.auto.commit", false)
        .containsEntry("allow.auto.create.topics", false)
        .containsEntry("group.protocol", "classic")
        .containsEntry(
            "partition.assignment.strategy", List.of(CooperativeStickyAssignor.class.getName()))
        .containsEntry("key.deserializer", ByteArrayDeserializer.class)
        .containsEntry("value.deserializer", ByteArrayDeserializer.class)
        .doesNotContainKeys("group.id", "client.id", "sasl.jaas.config", "ssl.truststore.location");
    assertThat(new ConsumerConfig(settings).getBoolean("enable.auto.commit")).isFalse();
  }

  @Test
  void saslSslClusterWithScramIdentity() {
    Trust trust = new Trust("/etc/trust.p12", "trust-pass", null);
    Map<String, Object> settings =
        ConsumerSettings.forIdentity(
            cluster(SecurityProtocol.SASL_SSL, trust, Map.of()), new KafkaIdentity(SCRAM, null));

    assertThat(settings)
        .containsEntry("security.protocol", "SASL_SSL")
        .containsEntry("ssl.truststore.location", "/etc/trust.p12")
        .containsEntry("ssl.truststore.password", "trust-pass")
        .doesNotContainKey("ssl.truststore.type")
        .containsEntry("sasl.mechanism", "SCRAM-SHA-256")
        .containsEntry(
            "sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required"
                + " username=\"orders-reader\" password=\"s3cr3t\";");
    // kafka-clients accepts the result.
    assertThat(new ConsumerConfig(settings).getString("sasl.mechanism")).isEqualTo("SCRAM-SHA-256");
  }

  @Test
  void jaasValuesAreEscaped() {
    Map<String, Object> settings =
        ConsumerSettings.forIdentity(
            cluster(SecurityProtocol.SASL_PLAINTEXT, null, Map.of()),
            new KafkaIdentity(new Sasl("PLAIN", "u\"x", "p\\\"; evil"), null));
    assertThat((String) settings.get("sasl.jaas.config"))
        .startsWith("org.apache.kafka.common.security.plain.PlainLoginModule required")
        .contains("username=\"u\\\"x\"")
        .contains("password=\"p\\\\\\\"; evil\"");
  }

  @Test
  void keystoreIdentityForMutualTls() {
    KafkaIdentity cert =
        new KafkaIdentity(null, new Keystore("/etc/ks.p12", "ks-pass", "key-pass", "PKCS12"));
    Map<String, Object> settings =
        ConsumerSettings.forIdentity(
            cluster(SecurityProtocol.SSL, new Trust("/etc/t.p12", null, "JKS"), Map.of()), cert);
    assertThat(settings)
        .containsEntry("ssl.keystore.location", "/etc/ks.p12")
        .containsEntry("ssl.keystore.password", "ks-pass")
        .containsEntry("ssl.key.password", "key-pass")
        .containsEntry("ssl.keystore.type", "PKCS12")
        .containsEntry("ssl.truststore.type", "JKS")
        .doesNotContainKeys("ssl.truststore.password", "sasl.mechanism");
  }

  @Test
  void passthroughIsAppliedUnderKafkafimSettings() {
    Map<String, Object> settings =
        ConsumerSettings.forIdentity(
            cluster(
                null, null, Map.of("fetch.max.bytes", "1048576", "session.timeout.ms", "30000")),
            null);
    assertThat(settings)
        .containsEntry("fetch.max.bytes", "1048576")
        .containsEntry("session.timeout.ms", "30000");
  }

  static List<String> ownedKeys() {
    return ConsumerSettings.OWNED_KEYS.stream().sorted().toList();
  }

  @ParameterizedTest
  @MethodSource("ownedKeys")
  void passthroughCannotOverrideAnOwnedKey(String key) {
    assertThatThrownBy(
            () -> ConsumerSettings.forIdentity(cluster(null, null, Map.of(key, "x")), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key)
        .hasMessageNotContaining("=x");
  }

  @Test
  void passthroughCannotOverrideDerivedSecurityKeysButMaySetTheOthers() {
    Map<String, String> jaas = Map.of("sasl.jaas.config", "secret-jaas");
    KafkaCluster sasl = cluster(SecurityProtocol.SASL_SSL, null, jaas);
    assertThatThrownBy(() -> ConsumerSettings.forIdentity(sasl, new KafkaIdentity(SCRAM, null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sasl.jaas.config")
        .hasMessageNotContaining("secret-jaas");

    // OAUTHBEARER: kafkafim sets only the mechanism; the JAAS config comes from the passthrough.
    Map<String, Object> oauth =
        ConsumerSettings.forIdentity(
            sasl, new KafkaIdentity(new Sasl("OAUTHBEARER", null, null), null));
    assertThat(oauth)
        .containsEntry("sasl.mechanism", "OAUTHBEARER")
        .containsEntry("sasl.jaas.config", "secret-jaas");

    KafkaCluster trustInPassthrough =
        cluster(
            SecurityProtocol.SSL,
            new Trust("/etc/t.p12", null, null),
            Map.of("ssl.truststore.location", "/other"));
    assertThatThrownBy(() -> ConsumerSettings.forIdentity(trustInPassthrough, null))
        .hasMessageContaining("ssl.truststore.location");

    assertThat(
            ConsumerSettings.derivedSecurityKeys(
                cluster(SecurityProtocol.SASL_SSL, new Trust("/t", "p", null), Map.of()),
                new KafkaIdentity(SCRAM, null)))
        .containsExactly(
            "sasl.jaas.config",
            "sasl.mechanism",
            "ssl.truststore.location",
            "ssl.truststore.password");
  }

  @Test
  void pipelineOverridesComeFromTheSourceBlock() {
    Pipeline.Source source =
        new Pipeline.Source(
            "k",
            List.of("t"),
            "g",
            null,
            Pipeline.OffsetReset.EARLIEST,
            42,
            DataSize.ofMegabytes(1),
            Duration.ofSeconds(1),
            DataSize.ofKilobytes(1));
    Properties overrides = ConsumerSettings.forPipeline(source);
    assertThat(overrides)
        .containsEntry("auto.offset.reset", "earliest")
        .containsEntry("max.poll.records", "42")
        .hasSize(2);

    Pipeline.Source defaults =
        new Pipeline.Source("k", List.of("t"), "g", null, null, null, null, null, null);
    assertThat(ConsumerSettings.forPipeline(defaults))
        .containsEntry("auto.offset.reset", "latest")
        .containsEntry("max.poll.records", "500");
  }

  @Test
  void factoriesAreCachedPerClusterAndIdentity() {
    KafkaCluster cluster =
        new KafkaCluster(
            List.of("k1:9093"),
            SecurityProtocol.SASL_PLAINTEXT,
            null,
            Map.of(
                "reader", new KafkaIdentity(SCRAM, null), "writer", new KafkaIdentity(SCRAM, null)),
            null,
            Map.of());
    ConsumerFactories factories =
        new ConsumerFactories(
            new KafkafimProperties(
                false, new KafkafimProperties.Kafka(Map.of("k", cluster)), null, null));

    assertThat(factories.get("k", "reader")).isSameAs(factories.get("k", "reader"));
    assertThat(factories.get("k", "writer")).isNotSameAs(factories.get("k", "reader"));
    assertThat(factories.size()).isEqualTo(2);
    assertThat(factories.get("k", "reader").getConfigurationProperties())
        .containsEntry("sasl.mechanism", "SCRAM-SHA-256");
    assertThatThrownBy(() -> factories.get("k", "nobody"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> factories.get("missing", null))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
