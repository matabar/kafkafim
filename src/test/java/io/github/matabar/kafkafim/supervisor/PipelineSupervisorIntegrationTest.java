package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.KafkaTestContainers;
import io.github.matabar.kafkafim.KafkaTopics;
import io.github.matabar.kafkafim.KafkafimApplication;
import io.github.matabar.kafkafim.dlq.DlqRecordFormat;
import io.github.matabar.kafkafim.telemetry.TelemetryNames;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The whole application against a real broker (auto-creation off, as in production) and
 * Elasticsearch 8.19: records in, documents and DLQ records out, offsets committed only for what is
 * done (DESIGN §4, §7, §9). Each test starts its own application context with its own pipelines,
 * topics and indices; the containers are shared.
 */
class PipelineSupervisorIntegrationTest {

  private static final KafkaContainer KAFKA =
      KafkaTestContainers.kafka().withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

  private static final ElasticsearchContainer ES =
      new ElasticsearchContainer(
              DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.19.0"))
          .withEnv("xpack.security.enabled", "false")
          .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m -XX:TieredStopAtLevel=1")
          // Parallel Failsafe forks can boot several nodes at once; the 60 s default is tight.
          .withStartupTimeout(Duration.ofMinutes(3));

  private static final Duration TIMEOUT = Duration.ofSeconds(90);
  private static final JsonMapper JSON = JsonMapper.builder().build();

  private static Admin admin;
  private static KafkaProducer<byte[], byte[]> producer;
  private static HttpClient http;

  private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();

  @BeforeAll
  static void startContainers() {
    Startables.deepStart(KAFKA, ES).join();
    admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    producer = new KafkaProducer<>(KafkaTopics.seedingProducerConfig(KAFKA.getBootstrapServers()));
    http = HttpClient.newHttpClient();
  }

  @AfterAll
  static void stopContainers() {
    if (producer != null) {
      producer.close();
    }
    if (admin != null) {
      admin.close();
    }
    if (http != null) {
      http.close();
    }
    ES.stop();
    KAFKA.stop();
  }

  @AfterEach
  void closeContexts() {
    contexts.forEach(ConfigurableApplicationContext::close);
  }

  // ---- (a) ----

  @Test
  void objectsAndArraysLandInDailyIndicesWithTheirIdsAndOffsetsAreCommitted() throws Exception {
    createTopics(3, "a.events", "a.events.kafkafim-dlq");
    ConfigurableApplicationContext app =
        start(
            new PipelineProps("a")
                .topic("a.events")
                .set("document.id-field", "/order_id")
                .set("document.timestamp-field", "/ts")
                .defaultRoute("a-orders-%{+yyyy.MM.dd}")
                .map());
    awaitState(app, "a", PipelineState.RUNNING);

    send("a.events", "k1", "{\"order_id\":\"o-1\",\"ts\":\"2026-01-02T03:04:05Z\",\"n\":1}");
    send(
        "a.events",
        "k2",
        "[{\"order_id\":\"o-2\",\"ts\":\"2026-01-03T00:00:00Z\"},"
            + "{\"order_id\":7,\"ts\":\"2026-01-03T23:59:59Z\"}]");
    send("a.events", "k3", "[]");
    for (int i = 0; i < 30; i++) {
      send(
          "a.events", "k" + i, "{\"order_id\":\"bulk-" + i + "\",\"ts\":\"2026-01-04T12:00:00Z\"}");
    }
    producer.flush();

    await().atMost(TIMEOUT).untilAsserted(() -> assertThat(count("a-orders-*")).isEqualTo(33));
    JsonNode o1 = document("a-orders-2026.01.02", "o-1");
    assertThat(o1.path("found").asBoolean()).isTrue();
    // Indexed byte for byte from the record value.
    assertThat(o1.get("_source").get("n").asInt()).isEqualTo(1);
    assertThat(document("a-orders-2026.01.03", "o-2").path("found").asBoolean()).isTrue();
    assertThat(document("a-orders-2026.01.03", "7").path("found").asBoolean()).isTrue();
    assertThat(count("a-orders-2026.01.04")).isEqualTo(30);
    awaitCommittedToEnd("kafkafim.a", "a.events");
    assertThat(readAll("a.events.kafkafim-dlq", Duration.ofSeconds(1))).isEmpty();
  }

  // ---- (a2) drop routes ----

  @Test
  void droppedDocumentsAreCommittedWithNothingInElasticsearchOrTheDlq() throws Exception {
    createTopics(2, "f.events", "f.events.kafkafim-dlq");
    ConfigurableApplicationContext app =
        start(
            new PipelineProps("f")
                .topic("f.events")
                .set("routes[0].name", "debug")
                .set("routes[0].when[0].field", "/level")
                .set("routes[0].when[0].equals", "debug")
                .set("routes[0].drop", true)
                .set("routes[1].default", true)
                .set("routes[1].cluster", "local")
                .set("routes[1].index", "f-docs")
                .map());
    awaitState(app, "f", PipelineState.RUNNING);

    // No id: it would be missing_id if it were indexed, but it is dropped first (ADR-0037).
    send("f.events", "k1", "{\"level\":\"debug\",\"msg\":\"x\"}");
    send("f.events", "k2", "[{\"id\":\"d1\",\"level\":\"debug\"},{\"level\":\"debug\"}]");
    send("f.events", "k3", "[{\"id\":\"d2\",\"level\":\"debug\"},{\"id\":\"kept\"}]");
    for (int i = 0; i < 20; i++) {
      send("f.events", "k" + i, "{\"id\":\"drop-" + i + "\",\"level\":\"debug\"}");
    }
    producer.flush();

    awaitCommittedToEnd("kafkafim.f", "f.events");
    await().atMost(TIMEOUT).untilAsserted(() -> assertThat(count("f-docs")).isOne());
    assertThat(document("f-docs", "kept").path("found").asBoolean()).isTrue();
    assertThat(document("f-docs", "d1").path("found").asBoolean()).isFalse();
    assertThat(document("f-docs", "drop-0").path("found").asBoolean()).isFalse();
    assertThat(readAll("f.events.kafkafim-dlq", Duration.ofSeconds(1))).isEmpty();
    MeterRegistry meters = app.getBean(MeterRegistry.class);
    assertThat(
            meters
                .get(TelemetryNames.DOCS)
                .tags("pipeline", "f", "route", "debug", "outcome", "dropped")
                .counter()
                .count())
        .isEqualTo(24);
  }

  // ---- (b) ----

  @Test
  void mappingErrorIsDeadLetteredWithHeadersWhileSiblingsAreIndexed() throws Exception {
    createTopics(1, "b.events", "b.events.kafkafim-dlq");
    createIndex("b-docs", "{\"mappings\":{\"properties\":{\"count\":{\"type\":\"integer\"}}}}");
    ConfigurableApplicationContext app =
        start(new PipelineProps("b").topic("b.events").defaultRoute("b-docs").map());
    awaitState(app, "b", PipelineState.RUNNING);

    final String array =
        "[{\"id\":\"b1\",\"count\":1},"
            + "{\"id\":\"b2\",\"count\":\"many\"},"
            + "{\"id\":\"b3\",\"count\":3}]";
    final long offset = send("b.events", "key-b", array, new RecordHeader("trace", bytes("t-1")));
    send("b.events", "key-c", "{\"id\":\"b4\",\"count\":4}");
    send("b.events", "key-d", "{\"count\":5}"); // missing_id
    producer.flush();

    await().atMost(TIMEOUT).untilAsserted(() -> assertThat(count("b-docs")).isEqualTo(3));
    assertThat(document("b-docs", "b2").path("found").asBoolean()).isFalse();
    List<ConsumerRecord<byte[], byte[]>> dlq =
        awaitRecords("b.events.kafkafim-dlq", 2, Duration.ofSeconds(60));
    ConsumerRecord<byte[], byte[]> mapping =
        dlq.stream()
            .filter(r -> header(r, DlqRecordFormat.ERROR_TYPE).equals("es_error"))
            .findFirst()
            .orElseThrow();
    assertThat(mapping.key()).isEqualTo(bytes("key-b"));
    assertThat(mapping.value()).isEqualTo(bytes(array));
    assertThat(header(mapping, "trace")).isEqualTo("t-1");
    assertThat(header(mapping, DlqRecordFormat.PIPELINE)).isEqualTo("b");
    assertThat(header(mapping, DlqRecordFormat.SOURCE_TOPIC)).isEqualTo("b.events");
    assertThat(header(mapping, DlqRecordFormat.SOURCE_PARTITION)).isEqualTo("0");
    assertThat(header(mapping, DlqRecordFormat.SOURCE_OFFSET)).isEqualTo(Long.toString(offset));
    assertThat(header(mapping, DlqRecordFormat.SOURCE_TIMESTAMP)).isNotBlank();
    assertThat(header(mapping, DlqRecordFormat.TARGET_CLUSTER)).isEqualTo("local");
    assertThat(header(mapping, DlqRecordFormat.TARGET_INDEX)).isEqualTo("b-docs");
    assertThat(header(mapping, DlqRecordFormat.DOC_INDEX)).isEqualTo("1");
    assertThat(header(mapping, DlqRecordFormat.ATTEMPTS)).isEqualTo("1");
    assertThat(header(mapping, DlqRecordFormat.FIRST_FAILURE_TS)).isNotBlank();
    assertThat(header(mapping, DlqRecordFormat.ERROR_REASON)).contains("parsing_exception");
    ConsumerRecord<byte[], byte[]> missingId =
        dlq.stream()
            .filter(r -> header(r, DlqRecordFormat.ERROR_TYPE).equals("missing_id"))
            .findFirst()
            .orElseThrow();
    assertThat(missingId.headers().lastHeader(DlqRecordFormat.DOC_INDEX)).isNull();
    assertThat(missingId.headers().lastHeader(DlqRecordFormat.TARGET_CLUSTER)).isNull();
    awaitCommittedToEnd("kafkafim.b", "b.events");
  }

  // ---- (c) ----

  @Test
  void elasticsearchOutageProducesLagNotDeadLettersAndEverythingArrivesAfterwards()
      throws Exception {
    createTopics(2, "c.events", "c.events.kafkafim-dlq");
    ConfigurableApplicationContext app =
        start(new PipelineProps("c").topic("c.events").defaultRoute("c-docs").map());
    awaitState(app, "c", PipelineState.RUNNING);
    sendDocs("c.events", "c", 0, 50);
    await().atMost(TIMEOUT).untilAsserted(() -> assertThat(count("c-docs")).isEqualTo(50));
    awaitCommittedToEnd("kafkafim.c", "c.events");

    ES.getDockerClient().pauseContainerCmd(ES.getContainerId()).exec();
    long committedDuringOutage;
    try {
      sendDocs("c.events", "c", 50, 150);
      Thread.sleep(8_000);
      committedDuringOutage = committedTotal("kafkafim.c", "c.events");
      assertThat(endTotal("c.events") - committedDuringOutage).as("lag").isEqualTo(100);
    } finally {
      ES.getDockerClient().unpauseContainerCmd(ES.getContainerId()).exec();
    }
    assertThat(committedDuringOutage).isEqualTo(50);

    await().atMost(TIMEOUT).untilAsserted(() -> assertThat(count("c-docs")).isEqualTo(150));
    awaitCommittedToEnd("kafkafim.c", "c.events");
    assertThat(readAll("c.events.kafkafim-dlq", Duration.ofSeconds(1))).isEmpty();
    assertThat(supervisor(app).state("c")).isIn(PipelineState.RUNNING, PipelineState.PAUSED);
  }

  // ---- (d) ----

  @Test
  void missingDlqTopicKeepsOnePipelineInFailedPreflightWhileAnotherRuns() throws Exception {
    createTopics(1, "d.bad", "d.replay", "d.replay.kafkafim-dlq");
    Map<String, Object> props = new LinkedHashMap<>();
    props.putAll(new PipelineProps("d-bad").topic("d.bad").defaultRoute("d-bad").map());
    props.putAll(
        new PipelineProps("d-replay")
            .topic("d.replay")
            .defaultRoute("d-replay-%{+yyyy.MM.dd}")
            .map());
    ConfigurableApplicationContext app = start(props);

    awaitState(app, "d-bad", PipelineState.FAILED_PREFLIGHT);
    awaitState(app, "d-replay", PipelineState.RUNNING);
    PipelineSupervisor supervisor = supervisor(app);
    assertThat(supervisor.failure("d-bad"))
        .contains("DLQ topic 'd.bad.kafkafim-dlq' (identity '<none>' on Kafka cluster 'local')")
        .contains("does not exist");
    MeterRegistry meters = app.getBean(MeterRegistry.class);
    assertThat(gauge(meters, "d-bad", "failed_preflight")).isEqualTo(1);
    assertThat(gauge(meters, "d-bad", "running")).isZero();
    assertThat(gauge(meters, "d-replay", "running")).isEqualTo(1);
    assertThat(readiness(app)).isEqualTo(200);
    // Never created (ADR-0021).
    assertThat(admin.listTopics().names().get()).doesNotContain("d.bad.kafkafim-dlq");

    // The running pipeline replays a DLQ record into the index of its original date (ADR-0027).
    send(
        "d.replay",
        "r",
        "{\"id\":\"r1\"}",
        new RecordHeader(DlqRecordFormat.SOURCE_TIMESTAMP, bytes("2025-12-31T10:00:00Z")));
    producer.flush();
    await()
        .atMost(TIMEOUT)
        .untilAsserted(() ->
            assertThat(document("d-replay-2025.12.31", "r1").path("found").asBoolean()).isTrue());
  }

  // ---- (e) ----

  @Test
  void contextClosedMidStreamLosesNothingAndRestartRedeliversTheRest() throws Exception {
    createTopics(3, "e.events", "e.events.kafkafim-dlq");
    Map<String, Object> props =
        new PipelineProps("e")
            .topic("e.events")
            // A small in-flight budget, so only part of the topic is fetched when the context
            // closes.
            .set("source.max-in-flight-bytes", "64KB")
            .set("source.max-record-bytes", "16KB")
            .set("source.max-poll-records", 10)
            .defaultRoute("e-docs")
            .map();
    props.put("kafkafim.elasticsearch.clusters.local.bulk.max-docs", 100);
    props.put("kafkafim.elasticsearch.clusters.local.bulk.in-flight", 1);
    int records = 2000;
    int perRecord = 10;
    // Sent asynchronously and flushed once, not one round trip per record. createTopics returns
    // only when the broker serves the partitions, and the producer keeps one request in flight, so
    // the first batch can neither be refused nor overtaken (KafkaTopics, #147).
    List<Future<RecordMetadata>> sent = new ArrayList<>();
    for (int r = 0; r < records; r++) {
      StringBuilder value = new StringBuilder("[");
      for (int d = 0; d < perRecord; d++) {
        value.append(d == 0 ? "" : ",").append("{\"id\":\"e-").append(r * perRecord + d);
        value.append("\",\"payload\":\"").append("x".repeat(200)).append("\"}");
      }
      sent.add(
          producer.send(
              new ProducerRecord<>(
                  "e.events", bytes("k" + r), bytes(value.append(']').toString()))));
    }
    producer.flush();
    for (Future<RecordMetadata> future : sent) {
      future.get(); // every record acknowledged, or the test fails here with the cause
    }
    final int total = records * perRecord;

    ConfigurableApplicationContext first = start(props);
    await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(50)).until(() -> count("e-docs") > 0);
    first.close(); // what SIGTERM triggers: graceful shutdown of every lifecycle phase
    long indexedAtClose = count("e-docs");
    long committedAtClose = committedTotal("kafkafim.e", "e.events");
    assertThat(committedAtClose).as("committed at close (mid-stream)").isLessThan(records);
    // Nothing committed was lost: every committed record's documents are in Elasticsearch.
    assertThat(indexedAtClose).isGreaterThanOrEqualTo(committedAtClose * perRecord);

    start(props);
    await().atMost(TIMEOUT).untilAsserted(() -> assertThat(count("e-docs")).isEqualTo(total));
    awaitCommittedToEnd("kafkafim.e", "e.events");
    assertThat(readAll("e.events.kafkafim-dlq", Duration.ofSeconds(1))).isEmpty();
  }

  // ---- (f) ----

  @Test
  void consumerTheBrokerRejectsMovesThePipelineToFailedInStateGaugeAndReadiness() throws Exception {
    // A real source failure, end to end (ADR-0036, #101): a broker that admits one member per
    // consumer group, and a group that is already full. kafkafim's consumer is refused on join;
    // poll throws GroupMaxSizeReachedException. It is no authentication or authorization error,
    // which the container would retry, so kafkafim's stopping error handler stops the consumer and
    // the source reports it.
    // Its own broker, because the shared one runs groups of several consumers.
    try (KafkaContainer oneMemberGroups =
        KafkaTestContainers.kafka()
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .withEnv("KAFKA_GROUP_MAX_SIZE", "1")) {
      oneMemberGroups.start();
      String bootstrap = oneMemberGroups.getBootstrapServers();
      try (Admin ownAdmin = Admin.create(Map.of("bootstrap.servers", bootstrap));
          KafkaConsumer<byte[], byte[]> member = consumer(bootstrap, "kafkafim.f")) {
        KafkaTopics.create(
            ownAdmin,
            List.of(
                new NewTopic("f.events", 1, (short) 1),
                new NewTopic("f.events.kafkafim-dlq", 1, (short) 1)));
        member.subscribe(List.of("f.events"));
        await()
            .atMost(TIMEOUT)
            .until(() -> {
              member.poll(Duration.ofMillis(200));
              return !member.assignment().isEmpty();
            });

        Map<String, Object> props =
            new PipelineProps("f").topic("f.events").defaultRoute("f-docs").map();
        props.put("kafkafim.kafka.clusters.local.bootstrap-servers[0]", bootstrap);
        // Closed here, before its broker stops, even when an assertion fails (closeContexts closes
        // it again, which does nothing).
        try (ConfigurableApplicationContext app = start(props)) {
          awaitState(app, "f", PipelineState.FAILED);
          PipelineSupervisor supervisor = supervisor(app);
          // ABNORMAL: the container's stopping error handler stopped the consumer on the poll
          // exception (ERROR is for a java.lang.Error from the listener).
          assertThat(supervisor.failure("f")).isEqualTo("Kafka consumer stopped (ABNORMAL)");
          // State, gauge and readiness agree: the pod's only pipeline failed, so it is out of
          // service.
          MeterRegistry meters = app.getBean(MeterRegistry.class);
          assertThat(gauge(meters, "f", "failed")).isEqualTo(1);
          assertThat(gauge(meters, "f", "running")).isZero();
          assertThat(readiness(app)).isEqualTo(503);
          // The failed source is stopped and closed, not left behind.
          await()
              .atMost(TIMEOUT)
              .until(() -> supervisor.runtimes().stream().allMatch(r -> r.source() == null));
          assertThat(supervisor.state("f")).isEqualTo(PipelineState.FAILED);
        }
      }
    }
  }

  // ---- application ----

  private ConfigurableApplicationContext start(Map<String, Object> pipelines) {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("spring.docker.compose.enabled", false);
    props.put("management.otlp.metrics.export.enabled", false);
    props.put("management.tracing.sampling.probability", 0.0);
    props.put("kafkafim.kafka.clusters.local.bootstrap-servers[0]", KAFKA.getBootstrapServers());
    props.put("kafkafim.elasticsearch.clusters.local.uris[0]", "http://" + ES.getHttpHostAddress());
    props.put("kafkafim.elasticsearch.clusters.local.bulk.max-age", "100ms");
    props.putAll(pipelines);
    ConfigurableApplicationContext context =
        new SpringApplicationBuilder(KafkafimApplication.class)
            .profiles("supervisor-it") // not `local`: no sample pipeline, no compose stack
            .properties(props)
            // application.yaml sets 8080; a command-line argument beats it (defaults do not).
            .run("--server.port=0");
    contexts.add(context);
    return context;
  }

  private static PipelineSupervisor supervisor(ConfigurableApplicationContext app) {
    return app.getBean(PipelineSupervisor.class);
  }

  private static void awaitState(
      ConfigurableApplicationContext app, String pipeline, PipelineState state) {
    await()
        .atMost(TIMEOUT)
        .untilAsserted(() -> assertThat(supervisor(app).state(pipeline)).isEqualTo(state));
  }

  private static double gauge(MeterRegistry meters, String pipeline, String state) {
    return meters
        .get(PipelineStateMetrics.NAME)
        .tag("pipeline", pipeline)
        .tag("state", state)
        .gauge()
        .value();
  }

  private static int readiness(ConfigurableApplicationContext app) throws Exception {
    String port = app.getEnvironment().getProperty("local.server.port");
    return http.send(
            HttpRequest.newBuilder(
                    URI.create("http://localhost:" + port + "/actuator/health/readiness"))
                .build(),
            HttpResponse.BodyHandlers.discarding())
        .statusCode();
  }

  /** Properties of one pipeline on the {@code local} clusters. */
  private static final class PipelineProps {
    private final String prefix;
    private final Map<String, Object> props = new LinkedHashMap<>();

    PipelineProps(String name) {
      this.prefix = "kafkafim.pipelines." + name + ".";
      set("source.cluster", "local");
      set("source.offset-reset", "earliest");
      set("source.commit-interval", "200ms");
    }

    PipelineProps topic(String topic) {
      return set("source.topics[0]", topic);
    }

    PipelineProps defaultRoute(String index) {
      set("routes[0].default", true);
      set("routes[0].cluster", "local");
      return set("routes[0].index", index);
    }

    PipelineProps set(String key, Object value) {
      props.put(prefix + key, value);
      return this;
    }

    Map<String, Object> map() {
      return new LinkedHashMap<>(props);
    }
  }

  // ---- Kafka ----

  private static void createTopics(int partitions, String... topics)
      throws ExecutionException, InterruptedException {
    List<NewTopic> newTopics = new ArrayList<>();
    for (String topic : topics) {
      newTopics.add(new NewTopic(topic, partitions, (short) 1));
    }
    KafkaTopics.create(admin, newTopics);
  }

  private static long send(String topic, String key, String value, Header... headers)
      throws Exception {
    ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(topic, null, bytes(key), bytes(value), List.of(headers));
    return producer.send(record).get().offset();
  }

  private static void sendDocs(String topic, String prefix, int from, int to) throws Exception {
    for (int i = from; i < to; i++) {
      producer.send(
          new ProducerRecord<>(
              topic, bytes("k" + i), bytes("{\"id\":\"" + prefix + "-" + i + "\"}")));
    }
    producer.flush();
  }

  private static long endTotal(String topic) throws Exception {
    List<TopicPartition> partitions =
        admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions().stream()
            .map(p -> new TopicPartition(topic, p.partition()))
            .toList();
    return admin
        .listOffsets(
            partitions.stream().collect(Collectors.toMap(p -> p, p -> OffsetSpec.latest())))
        .all()
        .get()
        .values()
        .stream()
        .mapToLong(info -> info.offset())
        .sum();
  }

  private static long committedTotal(String group, String topic) throws Exception {
    Map<TopicPartition, OffsetAndMetadata> committed =
        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
    return committed.entrySet().stream()
        .filter(e -> e.getKey().topic().equals(topic) && e.getValue() != null)
        .mapToLong(e -> e.getValue().offset())
        .sum();
  }

  private static void awaitCommittedToEnd(String group, String topic) {
    await()
        .atMost(TIMEOUT)
        .untilAsserted(() -> assertThat(committedTotal(group, topic)).isEqualTo(endTotal(topic)));
  }

  private static KafkaConsumer<byte[], byte[]> consumer(String bootstrap, String group) {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    return new KafkaConsumer<>(config);
  }

  private static List<ConsumerRecord<byte[], byte[]>> readAll(String topic, Duration wait) {
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer =
        consumer(KAFKA.getBootstrapServers(), "it-reader-" + UUID.randomUUID())) {
      consumer.subscribe(List.of(topic));
      long deadline = System.nanoTime() + wait.toNanos();
      while (System.nanoTime() < deadline) {
        consumer.poll(Duration.ofMillis(200)).forEach(records::add);
      }
    }
    return records;
  }

  private static List<ConsumerRecord<byte[], byte[]>> awaitRecords(
      String topic, int expected, Duration within) {
    List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
    await()
        .atMost(within)
        .untilAsserted(() -> {
          records.clear();
          records.addAll(readAll(topic, Duration.ofSeconds(2)));
          assertThat(records).hasSize(expected);
        });
    return records;
  }

  private static String header(ConsumerRecord<byte[], byte[]> record, String name) {
    Header header = record.headers().lastHeader(name);
    return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
  }

  private static byte[] bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  // ---- Elasticsearch ----

  private static HttpResponse<String> es(String method, String path, String body)
      throws IOException, InterruptedException {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://" + ES.getHttpHostAddress() + path))
            .header("Content-Type", "application/json");
    request.method(
        method,
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body));
    return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  private static void createIndex(String index, String body) throws Exception {
    assertThat(es("PUT", "/" + index, body).statusCode()).isEqualTo(200);
  }

  private static long count(String indices) throws Exception {
    es("POST", "/" + indices + "/_refresh?ignore_unavailable=true&allow_no_indices=true", null);
    HttpResponse<String> response =
        es("GET", "/" + indices + "/_count?ignore_unavailable=true&allow_no_indices=true", null);
    if (response.statusCode() != 200) {
      return 0;
    }
    return JSON.readTree(response.body()).get("count").asLong();
  }

  private static JsonNode document(String index, String id) throws Exception {
    return JSON.readTree(es("GET", "/" + index + "/_doc/" + id, null).body());
  }
}
