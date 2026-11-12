package io.github.matabar.kafkafim.sink.es;

import static io.github.matabar.kafkafim.sink.es.SinkHarness.doc;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.Response;
import co.elastic.clients.transport.rest5_client.low_level.ResponseException;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.config.ElasticsearchCluster.ElasticsearchIdentity;
import io.github.matabar.kafkafim.config.ElasticsearchCluster.Trust;
import io.github.matabar.kafkafim.config.Pipeline.OnVersionConflict;
import io.github.matabar.kafkafim.sink.es.SinkHarness.Events;
import io.github.matabar.kafkafim.sink.es.SinkHarness.Outcomes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;

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
  static final Duration PROVISION_WAIT = Duration.ofMinutes(3);
  static final Set<Integer> TRANSIENT = Set.of(429, 502, 503, 504);

  @Container
  static final ElasticsearchContainer ES =
      new ElasticsearchContainer(
              DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.19.0"))
          .withPassword(ADMIN_PASSWORD)
          .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m -XX:TieredStopAtLevel=1")
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
      // One deadline for the whole provisioning, so a slow node fails @BeforeAll in bounded time.
      long deadline = System.nanoTime() + PROVISION_WAIT.toNanos();
      awaitSecurityReady(admin, deadline);
      provisionCall(
          admin, deadline, "PUT", "/" + INDEX, "{\"settings\":{\"number_of_replicas\":0}}");
      provisionCall(
          admin,
          deadline,
          "PUT",
          "/_security/role/kfm-writer",
          "{\"indices\":[{\"names\":[\"" + INDEX + "\"],\"privileges\":[\"write\"]}]}");
      provisionCall(
          admin,
          deadline,
          "PUT",
          "/_security/user/kfm-writer",
          "{\"password\":\"" + WRITER_PASSWORD + "\",\"roles\":[\"kfm-writer\"]}");
      provisionCall(
          admin,
          deadline,
          "PUT",
          "/_security/user/kfm-viewer",
          "{\"password\":\"" + WRITER_PASSWORD + "\",\"roles\":[\"viewer\"]}");
      apiKey =
          required(
              provisionCall(
                  admin,
                  deadline,
                  "POST",
                  "/_security/api_key",
                  "{\"name\":\"kfm-it\",\"role_descriptors\":{\"w\":{\"indices\":"
                      + "[{\"names\":[\""
                      + INDEX
                      + "\"],\"privileges\":[\"write\"]}]}}}"),
              "POST /_security/api_key",
              "encoded");
    }
  }

  /**
   * The wait strategy only waits for the node's "started" log line; security can still be
   * initialising then. Waits until the admin authenticates and the cluster is at least yellow.
   */
  static void awaitSecurityReady(Rest5Client admin, long deadline) throws Exception {
    String last = "no reply yet";
    while (true) {
      try {
        Reply auth = send(admin, "GET", "/_security/_authenticate", null);
        if (auth.status() == 200) {
          Reply health =
              send(admin, "GET", "/_cluster/health?wait_for_status=yellow&timeout=5s", null);
          if (health.status() == 200) {
            return;
          }
          last = "cluster health: HTTP " + health.status() + ", body: " + health.body();
        } else {
          last = "_security/_authenticate: HTTP " + auth.status() + ", body: " + auth.body();
        }
      } catch (IOException e) {
        last = e.toString();
      }
      if (System.nanoTime() > deadline) {
        throw new AssertionError(
            "Elasticsearch security not ready within "
                + PROVISION_WAIT
                + "; last: "
                + last
                + nodeLogTail());
      }
      Thread.sleep(1000);
    }
  }

  /**
   * One provisioning request, retried until the shared deadline only on clearly transient outcomes
   * (429, 502, 503, 504, or an I/O error); any other non-2xx fails with the status, the body and
   * the node's log tail. A retried {@code POST /_security/api_key} may leave an extra key behind in
   * this throwaway node; the key from the successful reply is the one used.
   */
  static Reply provisionCall(
      Rest5Client admin, long deadline, String method, String path, String json) throws Exception {
    String request = method + " " + path;
    while (true) {
      Reply reply;
      try {
        reply = send(admin, method, path, json);
      } catch (IOException e) {
        if (System.nanoTime() > deadline) {
          throw new AssertionError(request + ": " + e + nodeLogTail(), e);
        }
        Thread.sleep(1000);
        continue;
      }
      if (reply.status() / 100 == 2) {
        return reply;
      }
      if (!TRANSIENT.contains(reply.status()) || System.nanoTime() > deadline) {
        throw reply.failure(request, "unexpected status", null);
      }
      Thread.sleep(1000);
    }
  }

  /** The named field of a 2xx provisioning response; fails with the whole reply when missing. */
  static String required(Reply reply, String request, String field) {
    JsonNode value;
    try {
      value = SinkHarness.JSON.readTree(reply.body()).get(field);
    } catch (JacksonException e) {
      throw reply.failure(request, "unparseable body", e);
    }
    if (value == null || !value.isString()) {
      throw reply.failure(request, "no '" + field + "' field", null);
    }
    return value.asString();
  }

  /**
   * One request and its reply, whatever the status. {@link Rest5Client#performRequest} returns
   * every status below 500 as a normal response and throws {@link ResponseException} only for 5xx
   * (see {@code Rest5ClientStatusContractTest}), so callers check {@link Reply#status()}. Before
   * #61 this class treated "no exception" as success: a 4xx reply to the API-key request was parsed
   * as if it were the key, and {@code get("encoded")} on the error body was the T49 NPE.
   */
  static Reply send(Rest5Client admin, String method, String path, @Nullable String json)
      throws Exception {
    Request request = new Request(method, path);
    if (json != null) {
      request.setJsonEntity(json);
    }
    Response response;
    try {
      response = admin.performRequest(request);
    } catch (ResponseException e) {
      response = e.getResponse();
    }
    String body =
        response.getEntity() == null ? "" : EntityUtils.toString(response.getEntity(), UTF_8);
    return new Reply(response.getStatusCode(), body);
  }

  record Reply(int status, String body) {
    AssertionError failure(String request, String why, @Nullable Throwable cause) {
      return new AssertionError(
          request + ": " + why + ", HTTP " + status + ", body: " + body + nodeLogTail(), cause);
    }
  }

  /** The last lines of the node's log, so a CI failure shows what security was doing. */
  static String nodeLogTail() {
    List<String> lines = ES.getLogs().lines().toList();
    return "\n--- last Elasticsearch log lines ---\n"
        + String.join("\n", lines.subList(Math.max(0, lines.size() - 40), lines.size()));
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
