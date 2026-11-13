package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.matabar.kafkafim.KafkafimApplication;
import io.github.matabar.kafkafim.supervisor.PipelineSupervisor;
import io.github.matabar.kafkafim.telemetry.KafkafimMeters;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.context.WebApplicationContext;

/**
 * Starts the real application under the {@code validate} profile (DESIGN §5.6). The configuration
 * names hosts that do not exist and secrets that are not mounted; nothing connects, so no Docker is
 * needed.
 */
class ValidateProfileIntegrationTest {

  private static final String K = "kafkafim.kafka.clusters.k";
  private static final String ES = "kafkafim.elasticsearch.clusters.es";
  private static final String P = "kafkafim.pipelines.p";

  private static final String[] VALID = {
    K + ".bootstrap-servers=kafka.invalid:9092",
    K + ".security-protocol=SASL_SSL",
    K + ".default-identity=r",
    K + ".identities.r.sasl.mechanism=SCRAM-SHA-256",
    K + ".identities.r.sasl.username=${kafka.k.r.username}",
    K + ".identities.r.sasl.password=${kafka.k.r.password}",
    ES + ".uris=https://es.invalid:9200",
    ES + ".default-identity=w",
    ES + ".identities.w.api-key=${elasticsearch.es.w.api-key}",
    P + ".source.cluster=k",
    P + ".source.topics=t",
    P + ".routes[0].default=true",
    P + ".routes[0].cluster=es",
    P + ".routes[0].index=idx"
  };

  private static SpringApplicationBuilder application(String... extra) {
    return new SpringApplicationBuilder(KafkafimApplication.class)
        .profiles("validate", "kafkafim")
        .properties(VALID)
        .properties(extra);
  }

  @Test
  void validConfigurationStartsNothingAndExitsZero() {
    try (ConfigurableApplicationContext context = application().run()) {
      assertThat(context).isNotInstanceOf(WebApplicationContext.class);
      assertThat(context.getBeanNamesForType(PipelineSupervisor.class)).isEmpty();
      assertThat(context.getBeanNamesForType(KafkafimMeters.class)).isEmpty();
      assertThat(context.getBean(ValidateProfile.Summary.class).text())
          .contains(
              "1 pipeline(s)",
              "pipeline p: Kafka cluster k as r",
              "kafka.k.r.password (not mounted)",
              "elasticsearch.es.w.api-key (not mounted)");
      assertThat(SpringApplication.exit(context)).isZero();
    }
  }

  @Test
  void invalidConfigurationFailsStartupWithEveryViolation() {
    SpringApplicationBuilder application =
        application(P + ".routes[0].cluster=nope", P + ".source.cluster=missing");

    assertThatThrownBy(application::run)
        .hasStackTraceContaining("unknown Elasticsearch cluster 'nope'")
        .hasStackTraceContaining("unknown Kafka cluster 'missing'");
  }

  @Test
  void literalSecretFailsStartup() {
    SpringApplicationBuilder application = application(K + ".identities.r.sasl.password=hunter2");

    assertThatThrownBy(application::run).hasStackTraceContaining("literal secret value");
  }
}
