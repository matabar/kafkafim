package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.github.matabar.kafkafim.dlq.TopicStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

class PreflightTest {

  private static final Supplier<Preflight.SourceAccess> GROUP_OK =
      () -> new Preflight.SourceAccess(Preflight.Status.OK, Map.of());

  private final CompiledPipeline compiled =
      PipelineMapping.compile(
          "p",
          TestConfigs.withPipelines(
              """
              p:
                source: { cluster: k, topics: [t] }
                route-mode: all
                routes:
                  - { cluster: es, index: a }
                  - { cluster: es2, index: b }
                  - { cluster: es, identity: writer, index: c }
              """));

  @Test
  void passesWhenEverythingExistsAndAuthenticates() {
    List<String> checked = new ArrayList<>();
    Preflight.Result result =
        Preflight.run(
            compiled,
            () -> TopicStatus.EXISTS,
            GROUP_OK,
            (cluster, identity) -> {
              checked.add(cluster + "/" + identity);
              return Preflight.Status.OK;
            });
    assertThat(result.passed()).isTrue();
    // One request per (cluster, identity), not per route.
    assertThat(checked).containsExactly("es/writer", "es2/null");
  }

  @Test
  void skipsDropRoutesWhichHaveNoCluster() {
    CompiledPipeline withDrops =
        PipelineMapping.compile(
            "p",
            TestConfigs.withPipelines(
                """
                p:
                  source: { cluster: k, topics: [t] }
                  routes:
                    - { when: [ { field: /level, equals: debug } ], drop: true }
                    - { when: [ { field: /level, equals: info } ], cluster: es2, index: b }
                    - { default: true, drop: true }
                """));
    List<String> checked = new ArrayList<>();
    Preflight.Result result =
        Preflight.run(
            withDrops,
            () -> TopicStatus.EXISTS,
            GROUP_OK,
            (cluster, identity) -> {
              checked.add(cluster + "/" + identity);
              return Preflight.Status.OK;
            });

    assertThat(result.passed()).isTrue();
    assertThat(checked).containsExactly("es2/null");
    assertThat(withDrops.streams()).containsOnlyKeys(new RouteTarget.StreamKey("es2", null));
    assertThat(withDrops.router().routes())
        .extracting(r -> r.isDrop(), r -> r.target())
        .containsExactly(
            org.assertj.core.groups.Tuple.tuple(true, RouteTarget.drop("0")),
            org.assertj.core.groups.Tuple.tuple(false, new RouteTarget("es2", null, null, "1")),
            org.assertj.core.groups.Tuple.tuple(true, RouteTarget.drop("2")));
  }

  @Test
  void pipelineThatOnlyDropsChecksNoElasticsearch() {
    CompiledPipeline onlyDrops =
        PipelineMapping.compile(
            "p",
            TestConfigs.withPipelines(
                """
                p:
                  source: { cluster: k, topics: [t] }
                  routes: [ { default: true, drop: true } ]
                """));
    Preflight.Result result =
        Preflight.run(
            onlyDrops,
            () -> TopicStatus.EXISTS,
            GROUP_OK,
            (cluster, identity) -> {
              throw new AssertionError("no cluster to check");
            });

    assertThat(result.passed()).isTrue();
    assertThat(onlyDrops.streams()).isEmpty();
  }

  @Test
  void missingDlqTopicAndRejectedIdentityAreBothReportedWithResourceAndIdentity() {
    Preflight.Result result =
        Preflight.run(
            compiled,
            () -> TopicStatus.MISSING,
            GROUP_OK,
            (cluster, identity) ->
                cluster.equals("es")
                    ? Preflight.Status.rejected("HTTP 401 from _security/_authenticate")
                    : Preflight.Status.OK);
    assertThat(result.passed()).isFalse();
    assertThat(result.configuration())
        .containsExactly(
            "DLQ topic 't.kafkafim-dlq' (identity '<none>' on Kafka cluster 'k') does not exist",
            "Elasticsearch identity 'writer' on cluster 'es' rejected: HTTP 401 from"
                + " _security/_authenticate");
    assertThat(result.transientProblems()).isEmpty();
  }

  @Test
  void unreachableIsTransient() {
    Preflight.Result result =
        Preflight.run(
            compiled,
            () -> TopicStatus.UNREACHABLE,
            GROUP_OK,
            (cluster, identity) -> Preflight.Status.unreachable("connection refused"));
    assertThat(result.configuration()).isEmpty();
    assertThat(result.transientProblems()).hasSize(3);
  }

  @Test
  void notAuthorizedDlqTopicIsConfiguration() {
    Preflight.Result result =
        Preflight.run(
            compiled, () -> TopicStatus.NOT_AUTHORIZED, GROUP_OK, (c, i) -> Preflight.Status.OK);
    assertThat(result.configuration()).singleElement().asString().contains("may not describe");
  }

  @Test
  void deniedConsumerGroupIsConfigurationNamingGroupIdentityAndCluster() {
    Preflight.Result result =
        Preflight.run(
            compiled,
            () -> TopicStatus.EXISTS,
            () ->
                group(Preflight.Status.rejected("identity may describe the group but not read it")),
            (c, i) -> Preflight.Status.OK);
    assertThat(result.configuration())
        .containsExactly(
            "consumer group 'kafkafim.p' (identity '<none>' on Kafka cluster 'k'): identity may"
                + " describe the group but not read it");
    assertThat(result.transientProblems()).isEmpty();
  }

  @Test
  void unreachableOrThrowingGroupCheckIsTransient() {
    Preflight.Result unreachable =
        Preflight.run(
            compiled,
            () -> TopicStatus.EXISTS,
            () -> group(Preflight.Status.unreachable("no answer within PT30S")),
            (c, i) -> Preflight.Status.OK);
    assertThat(unreachable.configuration()).isEmpty();
    assertThat(unreachable.transientProblems())
        .singleElement()
        .asString()
        .startsWith("consumer group 'kafkafim.p'")
        .endsWith("no answer within PT30S");

    Preflight.Result threw =
        Preflight.run(
            compiled,
            () -> TopicStatus.EXISTS,
            () -> {
              throw new IllegalStateException("boom");
            },
            (c, i) -> Preflight.Status.OK);
    assertThat(threw.configuration()).isEmpty();
    assertThat(threw.transientProblems()).singleElement().asString().contains("boom");
  }

  @Test
  void topicWithoutReadIsConfigurationNamingTopicIdentityAndCluster() {
    CompiledPipeline twoTopics =
        PipelineMapping.compile(
            "p",
            TestConfigs.withPipelines(
                """
                p:
                  source: { cluster: k, identity: reader, topics: [t, u, v] }
                  routes: [ { default: true, drop: true } ]
                """));
    Preflight.Result result =
        Preflight.run(
            twoTopics,
            () -> TopicStatus.EXISTS,
            () ->
                new Preflight.SourceAccess(
                    Preflight.Status.OK,
                    Map.of(
                        "v",
                            Preflight.Status.rejected(
                                "identity may describe the topic but not read it"),
                        "t",
                            Preflight.Status.rejected(
                                "identity may describe the topic but not read it"),
                        "u", Preflight.Status.OK)),
            (c, i) -> Preflight.Status.OK);

    // In configuration order; a topic without a verdict is left to the source's start.
    assertThat(result.configuration())
        .containsExactly(
            "source topic 't' (identity 'reader' on Kafka cluster 'k'): identity may describe the"
                + " topic but not read it",
            "source topic 'v' (identity 'reader' on Kafka cluster 'k'): identity may describe the"
                + " topic but not read it");
    assertThat(result.transientProblems()).isEmpty();
  }

  @Test
  void topicOperationsClassification() {
    assertThat(KafkaGroupCheck.classifyTopic(Set.of(AclOperation.READ, AclOperation.DESCRIBE)))
        .isEqualTo(Preflight.Status.OK);
    assertThat(KafkaGroupCheck.classifyTopic(Set.of(AclOperation.ALL)))
        .isEqualTo(Preflight.Status.OK);
    // An old broker lists nothing: it cannot say, so the first fetch is the check.
    assertThat(KafkaGroupCheck.classifyTopic(null)).isEqualTo(Preflight.Status.OK);
    assertThat(
            KafkaGroupCheck.classifyTopic(
                Set.of(AclOperation.DESCRIBE, AclOperation.WRITE, AclOperation.DESCRIBE_CONFIGS)))
        .isEqualTo(Preflight.Status.rejected("identity may describe the topic but not read it"));
  }

  @Test
  void unreachableBrokerLeavesTheTopicsToTheSource() {
    Preflight.SourceAccess access =
        KafkaGroupCheck.check(
            new DefaultKafkaConsumerFactory<>(
                Map.<String, Object>of("bootstrap.servers", "127.0.0.1:9")),
            "kafkafim.p",
            List.of("t", "u"),
            "kafkafim-p-preflight",
            Duration.ofSeconds(1));
    assertThat(access.group().kind()).isEqualTo(Preflight.Kind.TRANSIENT);
    assertThat(access.topics())
        .containsOnly(entry("t", Preflight.Status.OK), entry("u", Preflight.Status.OK));
  }

  @Test
  void groupOperationsClassification() {
    assertThat(KafkaGroupCheck.classify(Set.of(AclOperation.READ, AclOperation.DESCRIBE)).kind())
        .isEqualTo(Preflight.Kind.OK);
    assertThat(KafkaGroupCheck.classify(Set.of(AclOperation.ALL)).kind())
        .isEqualTo(Preflight.Kind.OK);
    // A broker that lists no operations cannot say: the first join is the check.
    assertThat(KafkaGroupCheck.classify(null).kind()).isEqualTo(Preflight.Kind.OK);
    assertThat(KafkaGroupCheck.classify(Set.of(AclOperation.DESCRIBE)).kind())
        .isEqualTo(Preflight.Kind.CONFIGURATION);
  }

  @Test
  void groupFailureClassification() {
    assertThat(KafkaGroupCheck.classifyFailure(new GroupAuthorizationException("denied")).kind())
        .isEqualTo(Preflight.Kind.CONFIGURATION);
    assertThat(KafkaGroupCheck.classifyFailure(new SaslAuthenticationException("bad")).kind())
        .isEqualTo(Preflight.Kind.CONFIGURATION);
    assertThat(KafkaGroupCheck.classifyFailure(new GroupIdNotFoundException("new")).kind())
        .isEqualTo(Preflight.Kind.OK);
    assertThat(KafkaGroupCheck.classifyFailure(new UnsupportedVersionException("old")).kind())
        .isEqualTo(Preflight.Kind.OK);
    // A group id taken by another protocol: no consumer can join it.
    assertThat(
            KafkaGroupCheck.classifyFailure(
                    new IllegalArgumentException("Group g is not a consumer group (connect)."))
                .kind())
        .isEqualTo(Preflight.Kind.CONFIGURATION);
    assertThat(KafkaGroupCheck.classifyFailure(new TimeoutException("slow")).kind())
        .isEqualTo(Preflight.Kind.TRANSIENT);
  }

  @Test
  void unreachableBrokerFailsTheGroupCheckAsTransient() {
    Preflight.Status status =
        KafkaGroupCheck.check(
            new DefaultKafkaConsumerFactory<>(
                Map.<String, Object>of("bootstrap.servers", "127.0.0.1:9")),
            "kafkafim.p",
            "kafkafim-p-preflight",
            Duration.ofSeconds(1));
    assertThat(status.kind()).isEqualTo(Preflight.Kind.TRANSIENT);
  }

  @Test
  void adminSettingsDropOnlyConsumerKeysAndSetTheClientId() {
    assertThat(
            KafkaGroupCheck.adminSettings(
                Map.of(
                    "bootstrap.servers", "b:9092",
                    "security.protocol", "SASL_SSL",
                    "config.providers", "file",
                    "config.providers.file.class",
                        "org.apache.kafka.common.config.provider.FileConfigProvider",
                    "custom.handler.key", "x",
                    "group.protocol", "classic",
                    "max.poll.records", 500,
                    "key.deserializer", "d",
                    "client.id", "consumer"),
                "kafkafim-p-preflight"))
        .containsOnly(
            entry("bootstrap.servers", "b:9092"),
            entry("security.protocol", "SASL_SSL"),
            entry("config.providers", "file"),
            entry(
                "config.providers.file.class",
                "org.apache.kafka.common.config.provider.FileConfigProvider"),
            entry("custom.handler.key", "x"),
            entry("client.id", "kafkafim-p-preflight"));
  }

  @Test
  void authCheckStatusClassification() {
    assertThat(ElasticsearchAuthCheck.classify(401).kind()).isEqualTo(Preflight.Kind.CONFIGURATION);
    assertThat(ElasticsearchAuthCheck.classify(403).kind()).isEqualTo(Preflight.Kind.CONFIGURATION);
    assertThat(ElasticsearchAuthCheck.classify(503).kind()).isEqualTo(Preflight.Kind.TRANSIENT);
    // Elasticsearch with security disabled answers 405 on this path.
    assertThat(ElasticsearchAuthCheck.classify(405).kind()).isEqualTo(Preflight.Kind.OK);
    assertThat(ElasticsearchAuthCheck.classify(404).kind()).isEqualTo(Preflight.Kind.OK);
  }

  @Test
  void tlsHandshakeRejectionIsConfiguration() {
    assertThat(
            ElasticsearchAuthCheck.isTlsRejection(
                new java.io.IOException(
                    "wrapped",
                    new javax.net.ssl.SSLHandshakeException("PKIX path building failed"))))
        .isTrue();
    assertThat(ElasticsearchAuthCheck.isTlsRejection(new java.net.ConnectException("refused")))
        .isFalse();
  }

  @Test
  void unknownIdentityIsConfiguration() {
    ElasticsearchAuthCheck check =
        new ElasticsearchAuthCheck(TestConfigs.withPipelines("{}").elasticsearch().clusters());
    assertThat(check.check("es", "nobody").kind()).isEqualTo(Preflight.Kind.CONFIGURATION);
    assertThat(check.check("nope", null).kind()).isEqualTo(Preflight.Kind.CONFIGURATION);
  }

  @Test
  void unreachableClusterIsTransient() {
    ElasticsearchAuthCheck check =
        new ElasticsearchAuthCheck(
            TestConfigs.bind(
                    """
                    kafkafim:
                      elasticsearch:
                        clusters:
                          down: { uris: ["http://127.0.0.1:1"] }
                    """)
                .elasticsearch()
                .clusters());
    assertThat(check.check("down", null).kind()).isEqualTo(Preflight.Kind.TRANSIENT);
  }

  private static Preflight.SourceAccess group(Preflight.Status status) {
    return new Preflight.SourceAccess(status, Map.of());
  }
}
