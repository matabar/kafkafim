package io.github.matabar.kafkafim.sink.es;

import static io.github.matabar.kafkafim.sink.es.SinkHarness.call;
import static io.github.matabar.kafkafim.sink.es.SinkHarness.doc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.config.ElasticsearchCluster.ElasticsearchIdentity;
import io.github.matabar.kafkafim.config.ElasticsearchCluster.Trust;
import io.github.matabar.kafkafim.config.Pipeline.OnVersionConflict;
import io.github.matabar.kafkafim.sink.es.SinkHarness.Events;
import io.github.matabar.kafkafim.sink.es.SinkHarness.Outcomes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Identities against a secured Elasticsearch 8.19 (TLS with its own CA, basic auth, API keys): the
 * production shape (DESIGN §5.2.1). Bad credentials are a configuration failure: retried and
 * reported, never DLQ.
 */
@Testcontainers
class ElasticsearchSinkSecurityIntegrationTest {

  static final String ADMIN_PASSWORD = "admin-pw-7f3a";
  static final String WRITER_PASSWORD = "writer-pw-91c2";
  static final String WRONG_PASSWORD = "wrong-pw-55d0";
  static final String INDEX = "kfm-secure";
  static final Duration WAIT = Duration.ofSeconds(60);

  @Container
  static final ElasticsearchContainer ES =
      new ElasticsearchContainer(
              DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.19.0"))
          .withPassword(ADMIN_PASSWORD)
          .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
          // Parallel Failsafe forks can boot several nodes at once; the 60 s default is tight.
          .withStartupTimeout(Duration.ofMinutes(3));

  @TempDir static Path dir;
  static Path caFile;
  static String apiKey;

  final Outcomes outcomes = new Outcomes();
  final Events events = new Events();
  ElasticsearchSinks sinks;

  @BeforeAll
  static void provision() throws Exception {
    caFile = Files.write(dir.resolve("ca.crt"), ES.caCertAsBytes().orElseThrow());
    try (Rest5Client admin =
        ElasticsearchClients.create(
            "secure",
            cluster(
                Map.of("admin", new ElasticsearchIdentity("elastic", ADMIN_PASSWORD, null, null))),
            "admin",
            ClientSettings.DEFAULT)) {
      call(admin, "PUT", "/" + INDEX, "{\"settings\":{\"number_of_replicas\":0}}");
      call(
          admin,
          "PUT",
          "/_security/role/kfm-writer",
          "{\"indices\":[{\"names\":[\"" + INDEX + "\"],\"privileges\":[\"write\"]}]}");
      call(
          admin,
          "PUT",
          "/_security/user/kfm-writer",
          "{\"password\":\"" + WRITER_PASSWORD + "\",\"roles\":[\"kfm-writer\"]}");
      call(
          admin,
          "PUT",
          "/_security/user/kfm-viewer",
          "{\"password\":\"" + WRITER_PASSWORD + "\",\"roles\":[\"viewer\"]}");
      apiKey =
          call(
                  admin,
                  "POST",
                  "/_security/api_key",
                  "{\"name\":\"kfm-it\",\"role_descriptors\":{\"w\":{\"indices\":"
                      + "[{\"names\":[\""
                      + INDEX
                      + "\"],\"privileges\":[\"write\"]}]}}}")
              .get("encoded")
              .asString();
    }
  }

  @AfterAll
  static void cleanUp() {
    // The container and temp dir are removed by their extensions.
  }

  @AfterEach
  void tearDown() {
    if (sinks != null) {
      sinks.close();
    }
  }

  static ElasticsearchCluster cluster(Map<String, ElasticsearchIdentity> identities) {
    return new ElasticsearchCluster(
        List.of("https://" + ES.getHttpHostAddress()),
        new Trust(caFile.toString()),
        identities,
        null,
        null,
        true,
        null);
  }

  BulkSink<String> open(String identity) {
    Map<String, ElasticsearchIdentity> identities = new LinkedHashMap<>();
    identities.put("writer", new ElasticsearchIdentity("kfm-writer", WRITER_PASSWORD, null, null));
    identities.put("api-key", new ElasticsearchIdentity(null, null, apiKey, null));
    identities.put("wrong", new ElasticsearchIdentity("kfm-writer", WRONG_PASSWORD, null, null));
    identities.put("viewer", new ElasticsearchIdentity("kfm-viewer", WRITER_PASSWORD, null, null));
    sinks =
        new ElasticsearchSinks(
            Map.of("secure", cluster(identities)),
            ClientSettings.DEFAULT,
            BreakerSettings.DEFAULT,
            events);
    SinkSettings settings =
        new SinkSettings(
            10,
            1 << 20,
            Duration.ofMillis(20),
            2,
            10,
            Duration.ofMinutes(10),
            OnVersionConflict.IGNORE,
            new Backoff(Duration.ofMillis(50), Duration.ofMillis(200), 2.0, 0.0));
    return sinks.open(new SinkKey("it", "secure", identity), settings, outcomes);
  }

  @Test
  void basicAuthOverTlsWithTheClusterCa() throws Exception {
    BulkSink<String> sink = open("writer");
    sink.submit(doc(INDEX, "basic-1", "{\"n\":1}"));
    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.byHandle.get("basic-1")).isEqualTo(ActionOutcome.INDEXED);
  }

  @Test
  void apiKeyOverTls() throws Exception {
    BulkSink<String> sink = open("api-key");
    sink.submit(doc(INDEX, "key-1", "{\"n\":1}"));
    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.byHandle.get("key-1")).isEqualTo(ActionOutcome.INDEXED);
  }

  @Test
  void wrongPasswordIsConfigurationFailureNeverDlq() {
    BulkSink<String> sink = open("wrong");
    sink.submit(doc(INDEX, "denied-1", "{\"n\":1}"));

    await().atMost(WAIT).until(() -> events.authFailures.get() >= 3);
    assertThat(sink.authFailing()).isTrue();
    assertThat(outcomes.byHandle).isEmpty();
    assertThat(events.authReasons)
        .allSatisfy(reason ->
            assertThat(reason)
                .startsWith("401 ")
                .doesNotContain(WRONG_PASSWORD)
                .doesNotContain(WRITER_PASSWORD));
    assertThat(sink.close(Duration.ZERO)).isFalse();
    assertThat(outcomes.byHandle).isEmpty();
  }

  @Test
  void missingWritePrivilegeIsConfigurationFailureNeverDlq() {
    BulkSink<String> sink = open("viewer");
    sink.submit(doc(INDEX, "forbidden-1", "{\"n\":1}"));

    await().atMost(WAIT).until(() -> events.authFailures.get() >= 2);
    assertThat(events.authReasons).allSatisfy(reason -> assertThat(reason).startsWith("403 "));
    assertThat(outcomes.byHandle).isEmpty();
  }
}
