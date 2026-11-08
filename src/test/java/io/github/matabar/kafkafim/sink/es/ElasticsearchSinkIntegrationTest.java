package io.github.matabar.kafkafim.sink.es;

import static io.github.matabar.kafkafim.sink.es.SinkHarness.call;
import static io.github.matabar.kafkafim.sink.es.SinkHarness.doc;
import static io.github.matabar.kafkafim.sink.es.SinkHarness.raw;
import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.config.Pipeline.OnVersionConflict;
import io.github.matabar.kafkafim.config.Pipeline.Op;
import io.github.matabar.kafkafim.sink.es.SinkHarness.Events;
import io.github.matabar.kafkafim.sink.es.SinkHarness.Outcomes;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * The sink against a real Elasticsearch 8.19 (security off, like the local compose stack): ids,
 * byte-for-byte sources, item errors, external versions and an outage mid-stream. The test creates
 * its indices through its own admin client; kafkafim never does (ADR-0021).
 */
@Testcontainers
class ElasticsearchSinkIntegrationTest {

  @Container
  static final ElasticsearchContainer ES =
      new ElasticsearchContainer(
              DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.19.0"))
          .withEnv("xpack.security.enabled", "false")
          .withEnv("action.auto_create_index", "false")
          .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
          // Parallel Failsafe forks can boot several nodes at once; the 60 s default is tight.
          .withStartupTimeout(Duration.ofMinutes(3));

  static final Duration WAIT = Duration.ofSeconds(60);

  static Rest5Client admin;

  final Outcomes outcomes = new Outcomes();
  final Events events = new Events();
  ElasticsearchSinks sinks;
  String index;

  @BeforeAll
  static void connect() {
    admin = Rest5Client.builder(URI.create("http://" + ES.getHttpHostAddress())).build();
  }

  @AfterAll
  static void disconnect() throws Exception {
    admin.close();
  }

  @BeforeEach
  void setUp() throws Exception {
    index = "kfm-" + UUID.randomUUID();
    call(
        admin,
        "PUT",
        "/" + index,
        "{\"settings\":{\"number_of_replicas\":0},"
            + "\"mappings\":{\"dynamic\":\"strict\",\"properties\":{"
            + "\"n\":{\"type\":\"long\"},\"s\":{\"type\":\"keyword\"}}}}");
  }

  @AfterEach
  void tearDown() {
    if (sinks != null) {
      sinks.close();
    }
  }

  ElasticsearchSinks sinks(ClientSettings clientSettings, BreakerSettings breakerSettings) {
    ElasticsearchCluster cluster =
        new ElasticsearchCluster(
            List.of("http://" + ES.getHttpHostAddress()), null, Map.of(), null, null, true);
    sinks =
        new ElasticsearchSinks(Map.of("local", cluster), clientSettings, breakerSettings, events);
    return sinks;
  }

  static SinkSettings settings(int maxDocs, OnVersionConflict onVersionConflict) {
    return new SinkSettings(
        maxDocs,
        5L << 20,
        Duration.ofMillis(50),
        4,
        10,
        Duration.ofMinutes(10),
        onVersionConflict,
        Backoff.DEFAULT);
  }

  BulkSink<String> open(ElasticsearchSinks registry, SinkSettings settings) {
    return registry.open(new SinkKey("it", "local", null), settings, outcomes);
  }

  long count() throws Exception {
    call(admin, "POST", "/" + index + "/_refresh", null);
    return call(admin, "GET", "/" + index + "/_count", null).get("count").asLong();
  }

  @Test
  void indexesDocumentsWithTheirIdsAndSourcesVerbatim() throws Exception {
    BulkSink<String> sink =
        open(
            sinks(ClientSettings.DEFAULT, BreakerSettings.DEFAULT),
            settings(7, OnVersionConflict.IGNORE));
    for (int i = 0; i < 50; i++) {
      // Odd spacing, key order and number spelling must survive (gzip on, no re-serialisation).
      sink.submit(doc(index, "id-" + i, "{\"s\":\"é-" + i + "\",  \"n\":" + i + "}"));
    }

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.byHandle)
        .hasSize(50)
        .allSatisfy((id, o) -> assertThat(o).isEqualTo(ActionOutcome.INDEXED));
    assertThat(count()).isEqualTo(50);
    assertThat(raw(admin, "GET", "/" + index + "/_source/id-42", null))
        .isEqualTo("{\"s\":\"é-42\",  \"n\":42}");
  }

  @Test
  void mappingErrorItemIsDlqWhileTheOthersAck() throws Exception {
    BulkSink<String> sink =
        open(
            sinks(ClientSettings.DEFAULT, BreakerSettings.DEFAULT),
            settings(10, OnVersionConflict.IGNORE));
    sink.submit(doc(index, "good-1", "{\"n\":1}"));
    sink.submit(doc(index, "bad-type", "{\"n\":\"not-a-number\"}"));
    sink.submit(doc(index, "bad-field", "{\"unknown\":true}"));
    sink.submit(doc(index, "good-2", "{\"n\":2}"));

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.byHandle.get("good-1")).isEqualTo(ActionOutcome.INDEXED);
    assertThat(outcomes.byHandle.get("good-2")).isEqualTo(ActionOutcome.INDEXED);
    assertThat(outcomes.dlq())
        .hasSize(2)
        .allSatisfy(dlq -> {
          assertThat(dlq.errorType()).isEqualTo(SinkErrorType.ES_ERROR);
          assertThat(dlq.status()).isEqualTo(400);
          assertThat(dlq.attempts()).isOne();
        });
    assertThat(((ActionOutcome.Dlq) outcomes.byHandle.get("bad-field")).reason())
        .contains("strict_dynamic_mapping_exception");
    assertThat(count()).isEqualTo(2);
  }

  @Test
  void staleExternalVersionIsIgnoredOrDlqPerPolicy() throws Exception {
    ElasticsearchSinks registry = sinks(ClientSettings.DEFAULT, BreakerSettings.DEFAULT);
    BulkSink<String> sink = open(registry, settings(1, OnVersionConflict.IGNORE));
    sink.submit(doc(index, "v", Op.INDEX, 5L, "{\"n\":5}"));
    assertThat(sink.awaitIdle(WAIT)).isTrue();
    outcomes.byHandle.clear();

    sink.submit(doc(index, "v", Op.INDEX, 3L, "{\"n\":3}"));
    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.byHandle.get("v"))
        .isInstanceOfSatisfying(
            ActionOutcome.Stale.class,
            stale -> assertThat(stale.reason()).contains("version_conflict_engine_exception"));
    assertThat(raw(admin, "GET", "/" + index + "/_source/v", null)).isEqualTo("{\"n\":5}");
    sink.close(WAIT);
    outcomes.byHandle.clear();

    BulkSink<String> strict =
        registry.open(
            new SinkKey("it-strict", "local", null), settings(1, OnVersionConflict.DLQ), outcomes);
    strict.submit(doc(index, "v", Op.INDEX, 4L, "{\"n\":4}"));
    assertThat(strict.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.byHandle.get("v"))
        .isInstanceOfSatisfying(
            ActionOutcome.Dlq.class,
            dlq -> assertThat(dlq.errorType()).isEqualTo(SinkErrorType.VERSION_CONFLICT));
  }

  @Test
  void createOfAnExistingDocumentCountsAsIndexed() throws Exception {
    BulkSink<String> sink =
        open(
            sinks(ClientSettings.DEFAULT, BreakerSettings.DEFAULT),
            settings(1, OnVersionConflict.IGNORE));
    sink.submit(doc(index, "c", Op.CREATE, null, "{\"n\":1}"));
    assertThat(sink.awaitIdle(WAIT)).isTrue();
    outcomes.byHandle.clear();
    sink.submit(doc(index, "c", Op.CREATE, null, "{\"n\":1}"));
    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.byHandle.get("c")).isEqualTo(ActionOutcome.INDEXED);
  }

  @Test
  void elasticsearchPausedMidStreamProducesNoDlqAndEveryActionAcksAfterItReturns()
      throws Exception {
    ClientSettings shortTimeouts =
        new ClientSettings(
            Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(2), 8, 32);
    BreakerSettings quickBreaker =
        new BreakerSettings(4, 2, 50f, Duration.ofSeconds(1), 1, Duration.ofMillis(50));
    SinkSettings settings =
        new SinkSettings(
            20,
            5L << 20,
            Duration.ofMillis(50),
            2,
            1, // the poison bound must not matter: request-level failures never count
            Duration.ZERO,
            OnVersionConflict.IGNORE,
            new Backoff(Duration.ofMillis(100), Duration.ofSeconds(1), 2.0, 0.5));
    BulkSink<String> sink = open(sinks(shortTimeouts, quickBreaker), settings);

    for (int i = 0; i < 100; i++) {
      sink.submit(doc(index, "before-" + i, "{\"n\":" + i + "}"));
    }
    assertThat(sink.awaitIdle(WAIT)).isTrue();

    ES.getDockerClient().pauseContainerCmd(ES.getContainerId()).exec();
    try {
      for (int i = 0; i < 200; i++) {
        sink.submit(doc(index, "during-" + i, "{\"n\":" + i + "}"));
        if (i % 50 == 0) {
          Thread.sleep(500);
        }
      }
      Thread.sleep(6_000);
      assertThat(outcomes.byHandle).hasSize(100);
      assertThat(sink.pendingActions()).isEqualTo(200);
    } finally {
      ES.getDockerClient().unpauseContainerCmd(ES.getContainerId()).exec();
    }

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.byHandle)
        .hasSize(300)
        .allSatisfy((id, o) -> assertThat(o).isEqualTo(ActionOutcome.INDEXED));
    assertThat(outcomes.dlq()).isEmpty();
    assertThat(events.requestRetries.get()).isPositive();
    assertThat(events.breakerStates)
        .contains(io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN);
    assertThat(count()).isEqualTo(300);
  }
}
