package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.validation.ObjectError;

class ValidateProfileTest {

  private static final String VALIDATE = "spring.profiles.active=validate";

  /** SASL reader {@code r} on cluster {@code k}, API-key writer {@code w} on cluster {@code es}. */
  private static final String WITH_IDENTITIES =
      ConfigTestSupport.pipeline(
          """
          k:
            bootstrap-servers: [k1:9092]
            security-protocol: SASL_SSL
            default-identity: r
            identities:
              r:
                sasl:
                  mechanism: SCRAM-SHA-256
                  username: ${kafka.k.r.username}
                  password: ${kafka.k.r.password}
          """,
          """
          es:
            uris: [https://es:9200]
            default-identity: w
            identities:
              w: { api-key: "${elasticsearch.es.w.api-key}" }
          """,
          """
          source: { cluster: k, topics: [t] }
          route-mode: all
          routes:
            - { name: audit, when: [{ field: /kind, equals: audit }], cluster: es, index: audit }
            - { when: [{ field: /kind, equals: noise }], drop: true }
            - { default: true, cluster: es, index: "idx-%{+yyyy.MM}", pipeline: enrich }
          """);

  private static final Map<String, Object> ALL_SECRETS =
      Map.of(
          "kafka.k.r.username", "reader",
          "kafka.k.r.password", "hunter2",
          "elasticsearch.es.w.api-key", "key-value");

  private static List<String> failures(String yaml, Map<String, Object> secrets, String... props) {
    List<String> messages = new ArrayList<>();
    ConfigTestSupport.runner(yaml, secrets)
        .withPropertyValues(props)
        .run(context -> {
          assertThat(context).hasFailed();
          Throwable failure = context.getStartupFailure();
          while (failure != null && !(failure instanceof BindValidationException)) {
            failure = failure.getCause();
          }
          assertThat(failure).isNotNull();
          for (ObjectError error :
              ((BindValidationException) failure).getValidationErrors().getAllErrors()) {
            messages.add(String.valueOf(error.getDefaultMessage()));
          }
        });
    return messages;
  }

  @Test
  void isActiveOnlyUnderTheProfile() {
    StandardEnvironment environment = new StandardEnvironment();
    assertThat(ValidateProfile.isActive(environment)).isFalse();
    environment.setActiveProfiles("validate", "kafkafim");
    assertThat(ValidateProfile.isActive(environment)).isTrue();
  }

  @Test
  void unmountedSecretsFailNormalStartup() {
    assertThat(failures(WITH_IDENTITIES, Map.of()))
        .hasSize(3)
        .allMatch(message -> message.contains("does not resolve"));
  }

  @Test
  void unmountedSecretsAreAcceptedAndListedUnderTheProfile() {
    ConfigTestSupport.runner(WITH_IDENTITIES, Map.of())
        .withPropertyValues(VALIDATE)
        .run(context -> {
          assertThat(context).hasNotFailed();
          assertThat(context.getBean(ValidateProfile.Summary.class).text())
              .isEqualTo(
                  """
                  Configuration is valid; nothing was started. 1 pipeline(s), 1 Kafka cluster(s),\
                   1 Elasticsearch cluster(s)
                    pipeline p: Kafka cluster k as r, topics [t], group kafkafim.p; DLQ topic\
                   t.kafkafim-dlq as r; route-mode all
                      route audit: Elasticsearch cluster es as w, index audit
                      route 1: drop
                      route 2 (default): Elasticsearch cluster es as w, index idx-%{+yyyy.MM},\
                   ingest pipeline enrich
                    expected secrets (3), 3 not mounted here
                      elasticsearch.es.w.api-key (not mounted)
                      kafka.k.r.password (not mounted)
                      kafka.k.r.username (not mounted)\
                  """
                      .replace("\\\n", ""));
        });
  }

  @Test
  void mountedSecretsAreListedWithoutTheirValues() {
    ConfigTestSupport.runner(WITH_IDENTITIES, ALL_SECRETS)
        .withPropertyValues(VALIDATE)
        .run(context -> {
          assertThat(context).hasNotFailed();
          assertThat(context.getBean(ValidateProfile.Summary.class).text())
              .endsWith(
                  "\n  expected secrets (3)\n    elasticsearch.es.w.api-key\n   "
                      + " kafka.k.r.password\n    kafka.k.r.username")
              .doesNotContain("hunter2", "key-value", "reader");
        });
  }

  @Test
  void onlyTheMissingSecretsAreMarked() {
    ConfigTestSupport.runner(WITH_IDENTITIES, Map.of("kafka.k.r.username", "reader"))
        .withPropertyValues(VALIDATE)
        .run(context -> {
          assertThat(context).hasNotFailed();
          assertThat(context.getBean(ValidateProfile.Summary.class).text())
              .contains(
                  "expected secrets (3), 2 not mounted here",
                  "elasticsearch.es.w.api-key (not mounted)",
                  "kafka.k.r.password (not mounted)")
              .endsWith("\n    kafka.k.r.username");
        });
  }

  @Test
  void literalSecretsAreStillRejectedUnderTheProfile() {
    String literal = WITH_IDENTITIES.replace("${kafka.k.r.password}", "hunter2");

    assertThat(failures(literal, Map.of(), VALIDATE))
        .singleElement()
        .asString()
        .contains("sasl.password: literal secret value")
        .doesNotContain("hunter2");
  }

  @Test
  void placeholderResolvingToLiteralIsStillRejectedUnderTheProfile() {
    assertThat(failures(WITH_IDENTITIES, Map.of(), VALIDATE, "kafka.k.r.password=hunter2"))
        .singleElement()
        .asString()
        .contains("resolves to a literal value")
        .doesNotContain("hunter2");
  }

  @Test
  void placeholderResolvedOnlyByRelaxedNameIsStillLiteralUnderTheProfile() {
    // The binder resolves ${kafka.k.r.password} from this key; an exact lookup would miss it.
    assertThat(failures(WITH_IDENTITIES, Map.of(), VALIDATE, "Kafka.K.R.Password=hunter2"))
        .singleElement()
        .asString()
        .contains("sasl.password: placeholder ${kafka.k.r.password} resolves to a literal value")
        .doesNotContain("hunter2");
  }

  @Test
  void sharedSecretIsListedOnceAndDirectSecretFileByItsPropertyName() {
    String direct = "kafkafim.elasticsearch.clusters.es.identities.w.api-key";
    String yaml =
        WITH_IDENTITIES
            .replace("${kafka.k.r.username}", "${kafka.k.r.password}")
            .replace("w: { api-key: \"${elasticsearch.es.w.api-key}\" }", "");

    ConfigTestSupport.runner(yaml, Map.of(direct, "file-value", "kafka.k.r.password", "x"))
        .withPropertyValues(VALIDATE)
        .run(context -> {
          assertThat(context).hasNotFailed();
          assertThat(context.getBean(ValidateProfile.Summary.class).text())
              .endsWith("\n  expected secrets (2)\n    kafka.k.r.password\n    " + direct)
              .doesNotContain("file-value");
        });
  }

  @Test
  void otherViolationsStillFailUnderTheProfile() {
    assertThat(
            failures(
                WITH_IDENTITIES, Map.of(), VALIDATE, "kafkafim.pipelines.p.routes[0].cluster=nope"))
        .anyMatch(message -> message.contains("unknown Elasticsearch cluster 'nope'"));
  }

  @Test
  void noSummaryOutsideTheProfile() {
    ConfigTestSupport.runner(WITH_IDENTITIES, ALL_SECRETS)
        .run(context -> {
          assertThat(context).hasNotFailed();
          assertThat(context).doesNotHaveBean(ValidateProfile.Summary.class);
        });
  }

  @Test
  void summarizesTreeWithoutPipelinesOrSecrets() {
    ConfigTestSupport.runner("kafkafim: {}", Map.of())
        .withPropertyValues(VALIDATE)
        .run(context -> {
          assertThat(context).hasNotFailed();
          assertThat(context.getBean(ValidateProfile.Summary.class).text())
              .isEqualTo(
                  "Configuration is valid; nothing was started. 0 pipeline(s), 0 Kafka"
                      + " cluster(s), 0 Elasticsearch cluster(s)\n  expected secrets (0)");
        });
  }
}
