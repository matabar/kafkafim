package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
import io.github.matabar.kafkafim.decode.DecodeSpec;
import io.github.matabar.kafkafim.route.ConditionSpec;
import io.github.matabar.kafkafim.route.RouteMode;
import io.github.matabar.kafkafim.route.RouteSpec;
import io.github.matabar.kafkafim.sink.es.SinkSettings;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class PipelineMappingTest {

  private static final String FULL =
      """
      orders:
        source: { cluster: k, topics: [a, b], max-record-bytes: 2MB }
        document:
          id-field: /order_id
          id-missing: auto
          version-field: /v
          on-version-conflict: dlq
          timestamp-field: /ts
          timestamp-format: epoch-millis
          on-bad-timestamp: ingest-time
        route-mode: all
        routes:
          - name: eu
            when:
              - { field: /region, equals: eu }
              - { field: /tier, in: [gold, silver] }
              - { field: /flag, exists: false }
              - { field: /code, prefix: X- }
              - { field: /sku, matches: "[a-z]+" }
              - { header: source, equals: web }
              - { topic: b }
            cluster: es
            index: "orders-eu-%{+yyyy.MM}"
            pipeline: enrich
          - when: [ { field: /region, equals: us } ]
            cluster: es2
            index: orders-us
          - when: [ { field: /region, equals: ca } ]
            cluster: es
            identity: writer
            index: orders-ca
        latency: { max-age: 50ms }
        dlq: { max-attempts: 3, max-elapsed: 1m }
      """;

  private final KafkafimProperties properties = TestConfigs.withPipelines(FULL);
  private final Pipeline pipeline = properties.pipelines().get("orders");

  @Test
  void decodeSpecCarriesDocumentSettingsAndMaxRecordBytes() {
    assertThat(PipelineMapping.decodeSpec(pipeline))
        .isEqualTo(
            new DecodeSpec(
                "/order_id",
                DecodeSpec.IdMissing.AUTO,
                "/v",
                "/ts",
                "epoch-millis",
                DecodeSpec.OnBadTimestamp.INGEST_TIME,
                2L * 1024 * 1024));
  }

  @Test
  void defaultsMapToDlqAndNoTimestampField() {
    Pipeline minimal =
        TestConfigs.withPipelines(
                """
                p:
                  source: { cluster: k, topics: [t] }
                  routes: [ { default: true, cluster: es, index: idx } ]
                """)
            .pipelines()
            .get("p");
    DecodeSpec spec = PipelineMapping.decodeSpec(minimal);
    assertThat(spec.idMissing()).isEqualTo(DecodeSpec.IdMissing.DLQ);
    assertThat(spec.onBadTimestamp()).isEqualTo(DecodeSpec.OnBadTimestamp.DLQ);
    assertThat(spec.timestampPointer()).isNull();
    assertThat(spec.idPointer()).isEqualTo("/id");
    assertThat(PipelineMapping.routeMode(minimal.routeMode())).isEqualTo(RouteMode.FIRST);
  }

  @Test
  void everyConditionKindMaps() {
    List<RouteSpec<RouteTarget>> routes = PipelineMapping.routeSpecs(pipeline);
    assertThat(routes.get(0).when())
        .containsExactly(
            new ConditionSpec.FieldEquals("/region", "eu"),
            new ConditionSpec.FieldIn("/tier", List.of("gold", "silver")),
            new ConditionSpec.FieldExists("/flag", false),
            new ConditionSpec.FieldPrefix("/code", "X-"),
            new ConditionSpec.FieldMatches("/sku", "[a-z]+"),
            new ConditionSpec.HeaderEquals("source", "web"),
            new ConditionSpec.TopicEquals("b"));
  }

  @Test
  void targetsCarryClusterResolvedIdentityPipelineAndLabel() {
    List<RouteSpec<RouteTarget>> routes = PipelineMapping.routeSpecs(pipeline);
    assertThat(routes)
        .extracting(RouteSpec::target)
        .containsExactly(
            new RouteTarget("es", "writer", "enrich", "eu"),
            new RouteTarget("es2", null, null, "1"),
            new RouteTarget("es", "writer", null, "2"));
    assertThat(routes.get(0).index()).isEqualTo("orders-eu-%{+yyyy.MM}");
    assertThat(PipelineMapping.routeMode(pipeline.routeMode())).isEqualTo(RouteMode.ALL);
  }

  @Test
  void oneStreamPerClusterAndIdentityWithLatencyOverride() {
    CompiledPipeline compiled = PipelineMapping.compile("orders", properties);
    assertThat(compiled.streams().keySet())
        .containsExactly(
            new RouteTarget.StreamKey("es", "writer"), new RouteTarget.StreamKey("es2", null));
    SinkSettings es = compiled.streams().get(new RouteTarget.StreamKey("es", "writer"));
    assertThat(es.maxDocs()).isEqualTo(10);
    assertThat(es.maxBytes()).isEqualTo(1024 * 1024);
    assertThat(es.maxAge()).isEqualTo(Duration.ofMillis(50));
    assertThat(es.inFlight()).isEqualTo(2);
    assertThat(es.maxAttempts()).isEqualTo(3);
    assertThat(es.maxElapsed()).isEqualTo(Duration.ofMinutes(1));
    assertThat(es.onVersionConflict()).isEqualTo(Pipeline.OnVersionConflict.DLQ);
    SinkSettings es2 = compiled.streams().get(new RouteTarget.StreamKey("es2", null));
    assertThat(es2.maxAge()).isEqualTo(Duration.ofMillis(50));
    assertThat(es2.maxDocs()).isEqualTo(5000);
    assertThat(compiled.timestampFieldSet()).isTrue();
    assertThat(compiled.op()).isEqualTo(Pipeline.Op.INDEX);
  }

  @Test
  void withoutLatencyOverrideTheClusterMaxAgeApplies() {
    KafkafimProperties p =
        TestConfigs.withPipelines(
            """
            p:
              source: { cluster: k, topics: [t] }
              routes: [ { default: true, cluster: es, index: idx } ]
            """);
    CompiledPipeline compiled = PipelineMapping.compile("p", p);
    assertThat(compiled.streams().values())
        .singleElement()
        .extracting(SinkSettings::maxAge)
        .isEqualTo(Duration.ofMillis(300));
    assertThat(compiled.timestampFieldSet()).isFalse();
  }

  @Test
  void createWithExternalVersionIsRejected() {
    KafkafimProperties p =
        TestConfigs.withPipelines(
            """
            p:
              source: { cluster: k, topics: [t] }
              document: { op: create, version-field: /v }
              routes: [ { default: true, cluster: es, index: idx } ]
            """);
    assertThatThrownBy(() -> PipelineMapping.compile("p", p))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("create");
  }

  @Test
  void blankIngestPipelineIsRejected() {
    KafkafimProperties p =
        TestConfigs.withPipelines(
            """
            p:
              source: { cluster: k, topics: [t] }
              routes: [ { default: true, cluster: es, index: idx, pipeline: " " } ]
            """);
    assertThatThrownBy(() -> PipelineMapping.compile("p", p))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ingest pipeline");
  }

  @Test
  void unusableConditionIsRejected() {
    assertThatThrownBy(() ->
            PipelineMapping.condition(
                "0", new Pipeline.Condition("/a", null, null, null, null, null, null, null)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() ->
            PipelineMapping.condition(
                "0", new Pipeline.Condition(null, "h", null, null, null, null, null, null)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void unknownPipelineIsRejected() {
    assertThatThrownBy(() -> PipelineMapping.compile("nope", properties))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
