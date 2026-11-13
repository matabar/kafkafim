package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.ConfigTreePropertySource;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

/** Binding and validating a reloaded environment with the startup rules (DESIGN §5.6). */
class ConfigLoaderTest {

  private static StandardEnvironment environment(String yaml, Map<String, Object> secrets) {
    StandardEnvironment environment = new StandardEnvironment();
    var sources = environment.getPropertySources();
    if (!secrets.isEmpty()) {
      sources.addLast(
          new ConfigTreePropertySource("configtree", ConfigTestSupport.configTree(secrets)));
    }
    ConfigTestSupport.load(yaml).forEach(sources::addLast);
    return environment;
  }

  @Test
  void validTreeBindsWithDefaults() {
    ConfigLoader.Result result =
        ConfigLoader.load(environment(ConfigTestSupport.MINIMAL, Map.of()));
    assertThat(result.problems()).isEmpty();
    assertThat(result.isValid()).isTrue();
    assertThat(result.properties()).isEqualTo(ConfigTestSupport.bind(ConfigTestSupport.MINIMAL));
    assertThat(result.properties().reload().pollInterval()).isEqualTo(Duration.ofSeconds(10));
    assertThat(result.properties().reload().debounce()).isEqualTo(Duration.ofSeconds(2));
  }

  @Test
  void everyViolationIsReportedAtOnce() {
    String yaml =
        ConfigTestSupport.pipeline(
                """
                routes:
                  - { default: true, cluster: nope, index: idx }
                """)
            + "  reload: { poll-interval: 0s }\n";
    ConfigLoader.Result result = ConfigLoader.load(environment(yaml, Map.of()));
    assertThat(result.isValid()).isFalse();
    assertThat(result.properties()).isNull();
    assertThat(result.problems())
        .anySatisfy(p -> assertThat(p).contains("routes[0].cluster").contains("nope"))
        .anySatisfy(
            p -> assertThat(p).isEqualTo("kafkafim.reload.poll-interval: must be positive"));
  }

  @Test
  void beanValidationRunsToo() {
    String yaml =
        ConfigTestSupport.pipeline(
            ConfigTestSupport.KAFKA_CLUSTERS,
            ConfigTestSupport.ES_CLUSTERS,
            """
            source: { cluster: "", topics: [t] }
            routes:
              - { default: true, cluster: es, index: idx }
            """);
    assertThat(ConfigLoader.load(environment(yaml, Map.of())).problems())
        .anySatisfy(p -> assertThat(p).contains("cluster").contains("must not be blank"));
  }

  @Test
  void unknownKeysAreRejected() {
    String yaml = ConfigTestSupport.MINIMAL + "  nonsense: 1\n";
    ConfigLoader.Result result = ConfigLoader.load(environment(yaml, Map.of()));
    assertThat(result.isValid()).isFalse();
    assertThat(result.problems()).singleElement().asString().contains("kafkafim");
  }

  @Test
  void literalSecretsAreRejectedAndConfigtreeSecretsAccepted() {
    String literal =
        ConfigTestSupport.pipeline(
            ConfigTestSupport.KAFKA_CLUSTERS,
            """
            es:
              uris: [http://es:9200]
              identities:
                w: { api-key: plain-text }
              default-identity: w
            """,
            "source: { cluster: k, topics: [t] }\n" + ConfigTestSupport.DEFAULT_ROUTE);
    assertThat(ConfigLoader.load(environment(literal, Map.of())).problems())
        .singleElement()
        .asString()
        .contains("api-key: literal secret value")
        .doesNotContain("plain-text");

    String placeholder = literal.replace("plain-text", "\"${elasticsearch.es.w.api-key}\"");
    assertThat(
            ConfigLoader.load(
                    environment(placeholder, Map.of("elasticsearch.es.w.api-key", "s3cr3t")))
                .isValid())
        .isTrue();
    // A secret file that disappeared leaves the placeholder unresolved: rejected, not bound.
    assertThat(ConfigLoader.load(environment(placeholder, Map.of())).problems())
        .singleElement()
        .asString()
        .contains("does not resolve");
  }

  @Test
  void inlineSecretsAreRefusedOnKubernetes() {
    StandardEnvironment environment =
        environment(ConfigTestSupport.MINIMAL + "  allow-inline-secrets: true\n", Map.of());
    environment
        .getPropertySources()
        .addFirst(new MapPropertySource("k8s", Map.of("spring.main.cloud-platform", "kubernetes")));
    assertThat(ConfigLoader.load(environment).problems())
        .singleElement()
        .asString()
        .contains("allow-inline-secrets=true is set on Kubernetes");
  }

  @Test
  void attachesTheRelaxedSourceSoTheCheckSeesWhatTheBinderBinds() {
    // Not attached by the caller. A secret file that matches the placeholder only under relaxed
    // matching sits before a literal with the exact name: once load attaches, the binder takes
    // the file, which is what the inline-secret check accepts. Unattached, it bound the literal.
    String yaml =
        ConfigTestSupport.pipeline(
            """
            k:
              bootstrap-servers: [k1:9092]
              security-protocol: SASL_SSL
              default-identity: r
              identities:
                r:
                  sasl: { mechanism: PLAIN, username: u, password: "${kafka.k.r.password}" }
            """,
            ConfigTestSupport.ES_CLUSTERS,
            "source: { cluster: k, topics: [t] }\n" + ConfigTestSupport.DEFAULT_ROUTE);
    StandardEnvironment environment = new StandardEnvironment();
    ConfigTestSupport.load(yaml).forEach(environment.getPropertySources()::addLast);
    environment
        .getPropertySources()
        .addFirst(new MapPropertySource("literal", Map.of("kafka.k.r.password", "hunter2")));
    environment
        .getPropertySources()
        .addFirst(
            new ConfigTreePropertySource(
                "configtree", ConfigTestSupport.configTree(Map.of("Kafka.K.R.Password", "file"))));

    ConfigLoader.Result result = ConfigLoader.load(environment);

    assertThat(result.problems()).isEmpty();
    assertThat(
            result.properties().kafka().clusters().get("k").identities().get("r").sasl().password())
        .isEqualTo("file");
  }
}
