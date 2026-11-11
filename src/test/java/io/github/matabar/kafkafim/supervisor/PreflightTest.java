package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.dlq.TopicStatus;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PreflightTest {

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
            (cluster, identity) -> Preflight.Status.unreachable("connection refused"));
    assertThat(result.configuration()).isEmpty();
    assertThat(result.transientProblems()).hasSize(3);
  }

  @Test
  void notAuthorizedDlqTopicIsConfiguration() {
    Preflight.Result result =
        Preflight.run(compiled, () -> TopicStatus.NOT_AUTHORIZED, (c, i) -> Preflight.Status.OK);
    assertThat(result.configuration()).singleElement().asString().contains("may not describe");
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
}
