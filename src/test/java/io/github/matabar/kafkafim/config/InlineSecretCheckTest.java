package io.github.matabar.kafkafim.config;

import static io.github.matabar.kafkafim.config.ConfigTestSupport.MINIMAL;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.ConfigTreePropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.validation.ObjectError;

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

  /** Binds with {@code sources} added first (highest precedence) and returns the violations. */
  private static List<String> errorsWith(
      String yaml, List<PropertySource<?>> sources, String... overrides) {
    List<String> errors = new ArrayList<>();
    ConfigTestSupport.runner(yaml, Map.of())
        .withInitializer(
            context -> sources.forEach(context.getEnvironment().getPropertySources()::addFirst))
        .withPropertyValues(SASL)
        .withPropertyValues(READER.split("\n"))
        .withPropertyValues(overrides)
        .run(context -> {
          if (context.getStartupFailure() == null) {
            return;
          }
          for (Throwable t = context.getStartupFailure(); t != null; t = t.getCause()) {
            if (t instanceof BindValidationException validation) {
              validation.getValidationErrors().getAllErrors().stream()
                  .map(ObjectError::getDefaultMessage)
                  .forEach(errors::add);
              return;
            }
          }
          throw new AssertionError(context.getStartupFailure());
        });
    return errors;
  }

  @Test
  void rejectsPlaceholderThatResolvesToLiteralInTheSameFile() {
    String yaml = yamlWithSaslPassword("${my.literal}") + "my:\n  literal: hunter2\n";

    assertThat(ConfigTestSupport.errors(yaml, SASL, READER))
        .singleElement()
        .asString()
        .startsWith(
            K + ".identities.r.sasl.password: placeholder ${my.literal} resolves to a literal")
        .contains("test.yaml")
        .doesNotContain("hunter2");
  }

  @Test
  void acceptsPlaceholderChainEndingInConfigTree() {
    String yaml = yamlWithSaslPassword("${alias}") + "alias: ${kafka.k.r.password}\n";

    ConfigTestSupport.bind(yaml, Map.of("kafka.k.r.password", "hunter2"), SASL, READER);
  }

  @Test
  void rejectsSecretFileWhoseContentIsPlaceholder() {
    // The binder would substitute ${my.lit} from YAML, so a literal would reach the password.
    List<String> errors =
        ConfigTestSupport.errors(
            yamlWithSaslPassword("${kafka.k.r.password}") + "my:\n  lit: hunter2\n",
            Map.of("kafka.k.r.password", "${my.lit}"),
            SASL,
            READER);

    assertThat(errors)
        .singleElement()
        .asString()
        .startsWith(K + ".identities.r.sasl.password: ")
        .contains("placeholder ${kafka.k.r.password} reaches a secret file", "contains \"${\"")
        .doesNotContain("hunter2");
  }

  @Test
  void rejectsSecretFileWithPlaceholderInsideEvenWhenInlineSecretsAllowed() {
    assertThat(
            ConfigTestSupport.errors(
                yamlWithSaslPassword("${kafka.k.r.password}"),
                Map.of("kafka.k.r.password", "pa${ss}word"),
                SASL,
                READER,
                "kafkafim.allow-inline-secrets=true"))
        .singleElement()
        .asString()
        .contains("contains \"${\"")
        .doesNotContain("pa${ss}word");
  }

  @Test
  void rejectsSecretFileReadStraightWhoseContentHasPlaceholder(@TempDir Path tree)
      throws IOException {
    Path file = tree.resolve("kafkafim/kafka/clusters/k/identities/r/sasl/password");
    Files.createDirectories(file.getParent());
    Files.writeString(file, "${my.lit}");
    List<String> errors = new ArrayList<>();

    ConfigTestSupport.runner(MINIMAL + "my:\n  lit: hunter2\n", Map.of())
        .withInitializer(context ->
            context
                .getEnvironment()
                .getPropertySources()
                .addFirst(new ConfigTreePropertySource("configtree", tree)))
        .withPropertyValues(SASL)
        .withPropertyValues(READER.split("\n"))
        .run(context -> errors.addAll(ConfigTestSupport.messages(context)));

    assertThat(errors)
        .singleElement()
        .asString()
        .contains("the secret file", "contains \"${\"")
        .doesNotContain("hunter2");
  }

  @Test
  void rejectsUnresolvedPlaceholderEvenWhenInlineSecretsAreAllowed() {
    assertThat(
            ConfigTestSupport.errors(
                yamlWithSaslPassword("${kafka.k.r.pasword}"),
                SASL,
                READER,
                "kafkafim.allow-inline-secrets=true"))
        .containsExactly(
            K
                + ".identities.r.sasl.password: placeholder ${kafka.k.r.pasword} does not resolve;"
                + " is its secret mounted under the configtree import?");
  }

  @Test
  void rejectsUnresolvedUsernamePlaceholder() {
    assertThat(
            ConfigTestSupport.errors(
                yamlWithSaslPassword("${kafka.k.r.password}"),
                Map.of("kafka.k.r.password", "p"),
                SASL,
                READER.replace("sasl.username=u", "sasl.username=${kafka.k.r.username}")))
        .singleElement()
        .asString()
        .startsWith(K + ".identities.r.sasl.username: placeholder ${kafka.k.r.username} does not");
  }

  @Test
  void rejectsLiteralFromEnvironmentVariable() {
    SystemEnvironmentPropertySource environment =
        new SystemEnvironmentPropertySource(
            StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
            Map.of("KAFKAFIM_KAFKA_CLUSTERS_K_IDENTITIES_R_SASL_PASSWORD", "hunter2"));

    assertThat(errorsWith(MINIMAL, List.of(environment)))
        .singleElement()
        .asString()
        .startsWith(K + ".identities.r.sasl.password: literal secret value")
        .doesNotContain("hunter2");
  }

  @Test
  void rejectsPlaceholderThatResolvesToEnvironmentVariable() {
    SystemEnvironmentPropertySource environment =
        new SystemEnvironmentPropertySource(
            StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
            Map.of("ORDERS_READER_PASSWORD", "hunter2"));

    assertThat(errorsWith(yamlWithSaslPassword("${ORDERS_READER_PASSWORD}"), List.of(environment)))
        .singleElement()
        .asString()
        .contains("placeholder ${ORDERS_READER_PASSWORD} resolves to a literal value")
        .contains(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
  }

  /** Reader {@code reader-a}: its secret's environment form drops the dash. */
  private static final String DASHED =
      K
          + ".identities.reader-a.sasl.mechanism=SCRAM-SHA-256\n"
          + K
          + ".identities.reader-a.sasl.username=u\n"
          + K
          + ".identities.reader-a.sasl.password=${kafka.k.reader-a.password}\n"
          + K
          + ".default-identity=reader-a";

  private static SystemEnvironmentPropertySource dashDroppedEnvironmentVariable() {
    return new SystemEnvironmentPropertySource(
        StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
        Map.of("KAFKA_K_READERA_PASSWORD", "hunter2"));
  }

  private static ConfigTreePropertySource mountedDashedSecret() {
    return new ConfigTreePropertySource(
        "configtree", ConfigTestSupport.configTree(Map.of("kafka.k.reader-a.password", "file")));
  }

  /**
   * Adds {@code sources} first, one by one (the last one wins), and attaches Boot's {@code
   * configurationProperties} source as {@code SpringApplication} does; relaxed placeholder names
   * resolve only through it.
   */
  private static void addFirstAndAttach(
      ConfigurableEnvironment environment, List<PropertySource<?>> sources) {
    sources.forEach(environment.getPropertySources()::addFirst);
    ConfigurationPropertySources.attach(environment);
  }

  /** The password the binder gives {@code reader-a} with {@code sources} added first. */
  private static String boundDashedPassword(List<PropertySource<?>> sources, String... overrides) {
    String[] password = new String[1];
    ConfigTestSupport.runner(MINIMAL, Map.of())
        .withInitializer(context -> addFirstAndAttach(context.getEnvironment(), sources))
        .withPropertyValues(SASL)
        .withPropertyValues(DASHED.split("\n"))
        .withPropertyValues(overrides)
        .run(context -> {
          assertThat(context).hasNotFailed();
          password[0] =
              context
                  .getBean(KafkafimProperties.class)
                  .kafka()
                  .clusters()
                  .get("k")
                  .identities()
                  .get("reader-a")
                  .sasl()
                  .password();
        });
    return password[0];
  }

  private static List<String> dashedErrors(List<PropertySource<?>> sources, String... overrides) {
    List<String> errors = new ArrayList<>();
    ConfigTestSupport.runner(MINIMAL, Map.of())
        .withInitializer(context -> addFirstAndAttach(context.getEnvironment(), sources))
        .withPropertyValues(SASL)
        .withPropertyValues(DASHED.split("\n"))
        .withPropertyValues(overrides)
        .run(context -> {
          assertThat(context).hasFailed();
          for (Throwable t = context.getStartupFailure(); t != null; t = t.getCause()) {
            if (t instanceof BindValidationException validation) {
              validation.getValidationErrors().getAllErrors().stream()
                  .map(ObjectError::getDefaultMessage)
                  .forEach(errors::add);
              return;
            }
          }
          throw new AssertionError(context.getStartupFailure());
        });
    return errors;
  }

  @Test
  void binderTakesTheDashDroppedEnvironmentVariableOverTheMountedFile() {
    // What the check has to mirror: with inline secrets allowed the tree binds, and the value is
    // the environment variable's, although a file with the exact name is mounted.
    assertThat(
            boundDashedPassword(
                List.of(mountedDashedSecret(), dashDroppedEnvironmentVariable()),
                "kafkafim.allow-inline-secrets=true"))
        .isEqualTo("hunter2");
  }

  @Test
  void rejectsRelaxedEnvironmentVariableThatWinsOverTheMountedFile() {
    // Added first one by one, so the environment variable ends up before the secret file.
    assertThat(dashedErrors(List.of(mountedDashedSecret(), dashDroppedEnvironmentVariable())))
        .singleElement()
        .asString()
        .contains(
            "sasl.password: placeholder ${kafka.k.reader-a.password} resolves to a literal value",
            StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME)
        .doesNotContain("hunter2");
  }

  @Test
  void acceptsTheMountedFileThatWinsOverTheRelaxedEnvironmentVariable() {
    // boundDashedPassword asserts that the context started, so the check accepted the tree.
    assertThat(
            boundDashedPassword(List.of(dashDroppedEnvironmentVariable(), mountedDashedSecret())))
        .isEqualTo("file");
  }

  @Test
  void rejectsRelaxedEnvironmentVariableWithoutMountedFile() {
    assertThat(dashedErrors(List.of(dashDroppedEnvironmentVariable())))
        .singleElement()
        .asString()
        .contains("placeholder ${kafka.k.reader-a.password} resolves to a literal value")
        .doesNotContain("hunter2");
  }

  @Test
  void rejectsEnvironmentVariableInCanonicalFormForLowerCasePlaceholder() {
    SystemEnvironmentPropertySource environment =
        new SystemEnvironmentPropertySource(
            StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
            Map.of("KAFKA_K_READER_A_PASSWORD", "hunter2"));

    assertThat(dashedErrors(List.of(mountedDashedSecret(), environment)))
        .singleElement()
        .asString()
        .contains(
            "placeholder ${kafka.k.reader-a.password} resolves to a literal value",
            "KAFKA_K_READER_A_PASSWORD",
            StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME)
        .doesNotContain("hunter2");
  }

  @Test
  void rejectsAliasChainEndingInRelaxedEnvironmentVariable() {
    MapPropertySource alias =
        new MapPropertySource("alias", Map.of("my.alias", "${kafka.k.reader-a.password}"));

    assertThat(
            dashedErrors(
                List.of(mountedDashedSecret(), alias, dashDroppedEnvironmentVariable()),
                K + ".identities.reader-a.sasl.password=${my.alias}"))
        .singleElement()
        .asString()
        .contains("placeholder ${kafka.k.reader-a.password} resolves to a literal value")
        .doesNotContain("hunter2");
  }

  @Test
  void rejectsPlaceholderResolvedByKeyInAnotherCase() {
    String yaml = yamlWithSaslPassword("${kafka.k.r.password}") + "Kafka.K.R.Password: hunter2\n";

    assertThat(ConfigTestSupport.errors(yaml, SASL, READER))
        .singleElement()
        .asString()
        .contains("placeholder ${kafka.k.r.password} resolves to a literal value")
        .doesNotContain("hunter2");
  }

  @Test
  void acceptsSecretFileNamedInAnotherCase() {
    KafkafimProperties properties =
        ConfigTestSupport.bind(
            yamlWithSaslPassword("${kafka.k.r.password}"),
            Map.of("Kafka.K.R.Password", "file"),
            SASL,
            READER);

    assertThat(properties.kafka().clusters().get("k").identities().get("r").sasl().password())
        .isEqualTo("file");
  }

  @Test
  void rejectsLiteralFromSystemProperty() {
    MapPropertySource systemProperties =
        new MapPropertySource(
            StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME,
            Map.of(K + ".identities.r.sasl.password", "hunter2"));

    assertThat(errorsWith(MINIMAL, List.of(systemProperties)))
        .singleElement()
        .asString()
        .startsWith(K + ".identities.r.sasl.password: literal secret value");
  }

  @Test
  void classifiesPassthroughKeys() {
    assertThat(InlineSecretCheck.isSecretPassthroughKey("sasl.jaas.config")).isTrue();
    assertThat(InlineSecretCheck.isSecretPassthroughKey("ssl.truststore.password")).isTrue();
    assertThat(InlineSecretCheck.isSecretPassthroughKey("sasl.oauthbearer.client.secret")).isTrue();
    assertThat(InlineSecretCheck.isSecretPassthroughKey("ssl.keystore.key")).isTrue();
    assertThat(InlineSecretCheck.isSecretPassthroughKey("basic.auth.user.info")).isTrue();
    assertThat(InlineSecretCheck.isSecretPassthroughKey("client.id")).isFalse();
  }
}
