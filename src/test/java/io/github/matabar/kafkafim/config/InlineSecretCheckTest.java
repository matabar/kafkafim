package io.github.matabar.kafkafim.config;

import static io.github.matabar.kafkafim.config.ConfigTestSupport.MINIMAL;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.env.ConfigTreePropertySource;

class InlineSecretCheckTest {

  private static final String K = "kafkafim.kafka.clusters.k";
  private static final String ES = "kafkafim.elasticsearch.clusters.es";
  private static final String SASL = K + ".security-protocol=SASL_SSL";
  private static final String READER =
      K
          + ".identities.r.sasl.mechanism=SCRAM-SHA-256\n"
          + K
          + ".identities.r.sasl.username=u\n"
          + K
          + ".default-identity=r";

  /** {@link ConfigTestSupport#MINIMAL} plus a second YAML document with the SASL password. */
  private static String yamlWithSaslPassword(String value) {
    String document =
        """
        ---
        kafkafim:
          kafka:
            clusters:
              k:
                identities:
                  r:
                    sasl:
                      password: %s
        """;
    return MINIMAL + document.formatted(value);
  }

  @Test
  void rejectsLiteralPasswordWithItsOriginButNotItsValue() {
    List<String> errors = ConfigTestSupport.errors(yamlWithSaslPassword("hunter2"), SASL, READER);

    assertThat(errors).hasSize(1);
    assertThat(errors.get(0))
        .startsWith(
            K
                + ".identities.r.sasl.password: literal secret value (Byte array resource"
                + " [test.yaml] - ")
        .contains("${...} placeholder", "kafkafim.allow-inline-secrets=true")
        .doesNotContain("hunter2");
  }

  @Test
  void acceptsPlaceholder() {
    KafkafimProperties properties =
        ConfigTestSupport.bind(
            yamlWithSaslPassword("${kafka.k.r.password}"),
            Map.of("kafka.k.r.password", "hunter2"),
            SASL,
            READER);

    assertThat(properties.kafka().clusters().get("k").identities().get("r").sasl().password())
        .isEqualTo("hunter2");
  }

  @Test
  void rejectsPlaceholderWithLiteralDefault() {
    assertThat(
            ConfigTestSupport.errors(
                yamlWithSaslPassword("\"${kafka.k.r.password:hunter2}\""), SASL, READER))
        .singleElement()
        .asString()
        .contains("literal secret value");
  }

  @Test
  void rejectsLiteralsWhenOnlyPartOfTheValueIsPlaceholder() {
    assertThat(
            ConfigTestSupport.errors(
                yamlWithSaslPassword("\"x${kafka.k.r.password}\""),
                Map.of("kafka.k.r.password", "y"),
                SASL,
                READER))
        .hasSize(1);
  }

  @Test
  void acceptsLiteralsWhenInlineSecretsAreAllowed() {
    ConfigTestSupport.bind(
        yamlWithSaslPassword("hunter2"), SASL, READER, "kafkafim.allow-inline-secrets=true");
  }

  @Test
  void acceptsValueReadStraightFromConfigTree(@TempDir Path tree) throws IOException {
    Path file = tree.resolve("kafkafim/kafka/clusters/k/identities/r/sasl/password");
    Files.createDirectories(file.getParent());
    Files.writeString(file, "hunter2");

    ConfigTestSupport.runner(MINIMAL, Map.of())
        .withInitializer(context ->
            context
                .getEnvironment()
                .getPropertySources()
                .addFirst(new ConfigTreePropertySource("configtree", tree)))
        .withPropertyValues(SASL)
        .withPropertyValues(READER.split("\n"))
        .run(context ->
            assertThat(
                    context
                        .getBean(KafkafimProperties.class)
                        .kafka()
                        .clusters()
                        .get("k")
                        .identities()
                        .get("r")
                        .sasl()
                        .password())
                .isEqualTo("hunter2"));
  }

  @Test
  void checksEverySecretField() {
    List<String> errors =
        ConfigTestSupport.errors(
            MINIMAL,
            "kafkafim.kafka.clusters.k.security-protocol=SSL",
            K + ".ssl.truststore-location=/t",
            K + ".ssl.truststore-password=lit",
            K + ".identities.c.ssl.keystore-location=/k",
            K + ".identities.c.ssl.keystore-password=lit",
            K + ".identities.c.ssl.key-password=lit",
            K + ".properties.sasl.jaas.config=lit",
            K + ".properties[ssl.key.password]=lit",
            K + ".properties.client.id=not-a-secret",
            ES + ".identities.b.username=u",
            ES + ".identities.b.password=lit",
            ES + ".identities.a.api-key=lit");

    assertThat(errors)
        .extracting(e -> e.substring(0, e.indexOf(':')))
        .containsExactlyInAnyOrder(
            K + ".ssl.truststore-password",
            K + ".identities.c.ssl.keystore-password",
            K + ".identities.c.ssl.key-password",
            K + ".properties.sasl.jaas.config",
            K + ".properties[ssl.key.password]",
            ES + ".identities.b.password",
            ES + ".identities.a.api-key");
  }

  @Test
  void classifiesPassthroughKeys() {
    assertThat(InlineSecretCheck.isSecretPassthroughKey("sasl.jaas.config")).isTrue();
    assertThat(InlineSecretCheck.isSecretPassthroughKey("ssl.truststore.password")).isTrue();
    assertThat(InlineSecretCheck.isSecretPassthroughKey("sasl.oauthbearer.client.secret")).isTrue();
    assertThat(InlineSecretCheck.isSecretPassthroughKey("client.id")).isFalse();
  }
}
