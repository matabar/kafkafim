package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

class KafkaClientSecurityTest {

  private static final String INLINE = "kafkafim.allow-inline-secrets=true";

  private static KafkaCluster cluster(String body) {
    String yaml =
        ConfigTestSupport.pipeline(
            "k:\n  bootstrap-servers: [k1:9093]\n" + body.indent(2),
            ConfigTestSupport.ES_CLUSTERS,
            "source: { cluster: k, topics: [t] }\n" + ConfigTestSupport.DEFAULT_ROUTE);
    return ConfigTestSupport.bind(yaml, INLINE).kafka().clusters().get("k");
  }

  @Test
  void keysAreTheKafkaClientsConstants() {
    assertThat(KafkaClientSecurity.SSL_TRUSTSTORE_LOCATION)
        .isEqualTo(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    assertThat(KafkaClientSecurity.SSL_TRUSTSTORE_PASSWORD)
        .isEqualTo(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
    assertThat(KafkaClientSecurity.SSL_TRUSTSTORE_TYPE)
        .isEqualTo(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
    assertThat(KafkaClientSecurity.SSL_KEYSTORE_LOCATION)
        .isEqualTo(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    assertThat(KafkaClientSecurity.SSL_KEYSTORE_PASSWORD)
        .isEqualTo(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
    assertThat(KafkaClientSecurity.SSL_KEY_PASSWORD).isEqualTo(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
    assertThat(KafkaClientSecurity.SSL_KEYSTORE_TYPE)
        .isEqualTo(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
    assertThat(KafkaClientSecurity.SASL_MECHANISM).isEqualTo(SaslConfigs.SASL_MECHANISM);
    assertThat(KafkaClientSecurity.SASL_JAAS_CONFIG).isEqualTo(SaslConfigs.SASL_JAAS_CONFIG);
  }

  @Test
  void plaintextClusterWithoutIdentityDerivesNothing() {
    assertThat(KafkaClientSecurity.properties(cluster(""), null)).isEmpty();
  }

  @Test
  void derivesTrustKeystoreAndScramInOrder() {
    Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("ssl.truststore.location", "/t.p12");
    expected.put("ssl.truststore.password", "tp");
    expected.put("ssl.truststore.type", "PKCS12");
    expected.put("ssl.keystore.location", "/k.p12");
    expected.put("ssl.keystore.password", "kp");
    expected.put("ssl.key.password", "kk");
    expected.put("ssl.keystore.type", "PKCS12");
    expected.put("sasl.mechanism", "SCRAM-SHA-512");
    expected.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.scram.ScramLoginModule required"
            + " username=\"u\\\"1\" password=\"p\\\\2\";");
    KafkaCluster cluster =
        cluster(
            """
            security-protocol: SASL_SSL
            ssl: { truststore-location: /t.p12, truststore-password: tp, truststore-type: PKCS12 }
            default-identity: r
            identities:
              r:
                sasl: { mechanism: SCRAM-SHA-512, username: 'u"1', password: 'p\\2' }
                ssl: { keystore-location: /k.p12, keystore-password: kp, key-password: kk, keystore-type: PKCS12 }
            """);

    assertThat(KafkaClientSecurity.properties(cluster, cluster.identities().get("r")))
        .containsExactlyEntriesOf(expected);
  }

  @Test
  void withoutIdentityOnlyTheClusterTrustIsDerived() {
    KafkaCluster cluster =
        cluster(
            """
            security-protocol: SSL
            ssl: { truststore-location: /t.jks }
            """);

    assertThat(KafkaClientSecurity.properties(cluster, null))
        .containsExactly(Map.entry("ssl.truststore.location", "/t.jks"));
  }

  @Test
  void emptyPasswordsAndTypesContributeNoKey() {
    KafkaCluster cluster =
        cluster(
            """
            security-protocol: SSL
            ssl: { truststore-location: /t.jks, truststore-password: "", truststore-type: "" }
            default-identity: r
            identities:
              r:
                ssl: { keystore-location: /k.jks, keystore-password: "", key-password: "" }
            """);

    assertThat(KafkaClientSecurity.properties(cluster, cluster.identities().get("r")))
        .containsOnlyKeys("ssl.truststore.location", "ssl.keystore.location");
  }

  @Test
  void plainUsesThePlainLoginModule() {
    KafkaCluster cluster =
        cluster(
            """
            security-protocol: SASL_PLAINTEXT
            default-identity: r
            identities:
              r: { sasl: { mechanism: PLAIN, username: u, password: p } }
            """);

    assertThat(KafkaClientSecurity.properties(cluster, cluster.identities().get("r")))
        .containsEntry(
            "sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required"
                + " username=\"u\" password=\"p\";");
  }

  @Test
  void otherMechanismsGetNoJaasConfig() {
    KafkaCluster cluster =
        cluster(
            """
            security-protocol: SASL_SSL
            default-identity: r
            identities:
              r: { sasl: { mechanism: OAUTHBEARER } }
            """);

    assertThat(KafkaClientSecurity.properties(cluster, cluster.identities().get("r")))
        .containsExactly(Map.entry("sasl.mechanism", "OAUTHBEARER"));
  }

  @Test
  void jaasEscapeQuotesBackslashAndDoubleQuote() {
    assertThat(KafkaClientSecurity.jaasEscape(null)).isEmpty();
    assertThat(KafkaClientSecurity.jaasEscape("plain")).isEqualTo("plain");
    assertThat(KafkaClientSecurity.jaasEscape("a\\b\"c")).isEqualTo("a\\\\b\\\"c");
  }

  @Test
  void redactedHidesEveryKeyThatMayCarrySecrets() {
    Map<String, Object> settings = new LinkedHashMap<>();
    settings.put("bootstrap.servers", "k1:9093");
    settings.put("ssl.truststore.password", "s1");
    settings.put("sasl.jaas.config", "s2");
    settings.put("sasl.oauthbearer.client.secret", "s3");
    settings.put("sasl.login.token.x", "s4");
    settings.put("some.credential.file", "s5");
    settings.put("basic.auth.user.info", "s6");
    settings.put("ssl.keystore.key", "s7");
    settings.put("max.poll.records", 500);

    Map<String, Object> redacted = KafkaClientSecurity.redacted(settings);

    assertThat(redacted)
        .containsEntry("bootstrap.servers", "k1:9093")
        .containsEntry("max.poll.records", "500")
        .hasSize(settings.size());
    assertThat(redacted.toString()).doesNotContain("s1", "s2", "s3", "s4", "s5", "s6", "s7");
    assertThat(redacted.values()).filteredOn("[hidden]"::equals).hasSize(7);
  }
}
