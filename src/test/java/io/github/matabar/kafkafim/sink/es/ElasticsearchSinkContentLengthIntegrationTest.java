package io.github.matabar.kafkafim.sink.es;

import static io.github.matabar.kafkafim.sink.es.SinkHarness.call;
import static io.github.matabar.kafkafim.sink.es.SinkHarness.doc;
import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.config.Pipeline.OnVersionConflict;
import io.github.matabar.kafkafim.sink.es.SinkHarness.Outcomes;
import java.net.URI;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * The sink against an Elasticsearch 8.19 that rejects bodies over a lowered {@code
 * http.max_content_length} (1mb, so the test bodies stay small). Elasticsearch answers 413 as soon
 * as it knows the body is too large and closes the connection, while the client may still be
 * uploading. The sink must see that 413 and split, with gzip on or off; if the client reported a
 * connection reset instead, the sink would retry the same body forever (issue #169).
 */
@Testcontainers
class ElasticsearchSinkContentLengthIntegrationTest {

  static final long LIMIT = 1L << 20;

  @Container
  static final ElasticsearchContainer ES =
      new ElasticsearchContainer(
              DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.19.0"))
          .withEnv("xpack.security.enabled", "false")
          .withEnv("action.auto_create_index", "false")
          .withEnv("http.max_content_length", "1mb")
          .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m -XX:TieredStopAtLevel=1")
          // Parallel Failsafe forks can boot several nodes at once; the 60 s default is tight.
          .withStartupTimeout(Duration.ofMinutes(3));

  static final Duration WAIT = Duration.ofSeconds(120);

  static Rest5Client admin;

  final Outcomes outcomes = new Outcomes();
  final Responses responses = new Responses();
  final Random random = new Random(169);
  ElasticsearchSinks sinks;
  String index;

  /** One answered bulk request. */
  record Answered(int status, int docs, long bytes) {}

  /** Records every answered request and counts request-level retries. */
  static final class Responses implements SinkListener {
    final List<Answered> answered = new CopyOnWriteArrayList<>();
    final AtomicInteger requestRetries = new AtomicInteger();

    @Override
    public void onBulkResponse(SinkKey key, int status, int docs, long bytes, Duration latency) {
      answered.add(new Answered(status, docs, bytes));
    }

    @Override
    public void onRequestRetry(
        SinkKey key, int status, @Nullable Throwable error, int attempt, Duration delay) {
      requestRetries.incrementAndGet();
    }
  }

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
    // The sources are only stored, not indexed: the test is about the request, not the mapping.
    call(
        admin,
        "PUT",
        "/" + index,
        "{\"settings\":{\"number_of_replicas\":0},\"mappings\":{\"dynamic\":false}}");
  }

  @AfterEach
  void tearDown() {
    if (sinks != null) {
      sinks.close();
    }
  }

  /** A sink whose batches seal at 8MB, eight times what the node accepts. */
  BulkSink<String> open(boolean compression) {
    ElasticsearchCluster cluster =
        new ElasticsearchCluster(
            List.of("http://" + ES.getHttpHostAddress()),
            null,
            Map.of(),
            null,
            null,
            compression,
            null);
    sinks =
        new ElasticsearchSinks(
            Map.of("local", cluster), ClientSettings.DEFAULT, BreakerSettings.DEFAULT, responses);
    SinkSettings settings =
        new SinkSettings(
            10_000,
            8 * LIMIT,
            Duration.ofSeconds(30),
            1,
            10,
            Duration.ofMinutes(10),
            OnVersionConflict.IGNORE,
            Backoff.DEFAULT);
    return sinks.open(new SinkKey("it", "local", null), settings, outcomes);
  }

  /** A source of about {@code bytes} bytes that gzip cannot shrink much. */
  String source(int bytes) {
    byte[] raw = new byte[bytes * 3 / 4];
    random.nextBytes(raw);
    return "{\"s\":\"" + Base64.getEncoder().encodeToString(raw) + "\"}";
  }

  long count() throws Exception {
    call(admin, "POST", "/" + index + "/_refresh", null);
    return call(admin, "GET", "/" + index + "/_count", null).get("count").asLong();
  }

  @ParameterizedTest(name = "compression = {0}")
  @ValueSource(booleans = {false, true})
  void bodyFarOverTheLimitIsAnsweredWith413AndSplitUntilItFits(boolean compression)
      throws Exception {
    BulkSink<String> sink = open(compression);
    // 160 documents of 64 KB: the first batch seals for bytes just under 8MB.
    // Mind the margin when changing sizes: the split ends in 16-document requests about 1.4 KB
    // over the limit (413) and 8-document ones well under it. With gzip on, those 16-document
    // bodies compress to under 1MB and still get 413: the limit counts uncompressed bytes.
    for (int i = 0; i < 160; i++) {
      sink.submit(doc(index, "id-" + i, source(64 << 10)));
    }
    sink.flush();

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.byHandle)
        .hasSize(160)
        .allSatisfy((id, o) -> assertThat(o).isEqualTo(ActionOutcome.INDEXED));
    // The whole 8MB upload was answered with 413, not with a connection reset.
    assertThat(responses.answered)
        .anySatisfy(a -> {
          assertThat(a.status()).isEqualTo(413);
          assertThat(a.bytes()).isGreaterThan(7 * LIMIT);
        });
    assertThat(responses.answered)
        .allSatisfy(a -> assertThat(a.status()).isEqualTo(a.bytes() > LIMIT ? 413 : 200));
    assertThat(responses.requestRetries).hasValue(0);
    assertThat(count()).isEqualTo(160);
  }

  @ParameterizedTest(name = "compression = {0}")
  @ValueSource(booleans = {false, true})
  void singleDocumentOverTheLimitGoesToDlqTooLargeAndTheOthersAreIndexed(boolean compression)
      throws Exception {
    BulkSink<String> sink = open(compression);
    sink.submit(doc(index, "small-1", "{\"s\":\"a\"}"));
    sink.submit(doc(index, "big", source(4 << 20)));
    sink.submit(doc(index, "small-2", "{\"s\":\"b\"}"));
    sink.flush();

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.byHandle.get("small-1")).isEqualTo(ActionOutcome.INDEXED);
    assertThat(outcomes.byHandle.get("small-2")).isEqualTo(ActionOutcome.INDEXED);
    assertThat(outcomes.byHandle.get("big"))
        .isInstanceOfSatisfying(
            ActionOutcome.Dlq.class,
            dlq -> {
              assertThat(dlq.errorType()).isEqualTo(SinkErrorType.TOO_LARGE);
              assertThat(dlq.status()).isEqualTo(413);
            });
    assertThat(responses.requestRetries).hasValue(0);
    assertThat(count()).isEqualTo(2);
  }
}
