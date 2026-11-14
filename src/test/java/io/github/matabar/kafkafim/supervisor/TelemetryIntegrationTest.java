package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.KafkaTestContainers;
import io.github.matabar.kafkafim.KafkaTopics;
import io.github.matabar.kafkafim.KafkafimApplication;
import io.github.matabar.kafkafim.telemetry.BatchTracer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * DESIGN §10 end to end: real Kafka → Elasticsearch traffic (good documents, a mapping failure, an
 * undecodable record), then {@code /actuator/prometheus} carries the §10 series with their labels,
 * the liveness and readiness groups contain the {@code supervisor} and {@code kafka} contributors,
 * and the failed batch is traced although the sampling probability is 0: a {@code pipeline.batch}
 * span with an {@code es.bulk} child and a link to the trace of the record's {@code traceparent}
 * header.
 */
class TelemetryIntegrationTest {

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
  private static final String TOPIC = "tel.events";
  private static final String BAD_TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
  private static final String BAD_SPAN = "00f067aa0ba902b7";

  private static Admin admin;
  private static KafkaProducer<byte[], byte[]> producer;
  private static HttpClient http;

  private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();

  /** Captures the spans the application exports, next to (disabled) OTLP. */
  @Configuration(proxyBeanMethods = false)
  static class SpanCapture {

    static final List<SpanData> SPANS = new CopyOnWriteArrayList<>();

    @Bean
    SpanExporter capturingSpanExporter() {
      return new SpanExporter() {
        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
          SPANS.addAll(spans);
          return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
          return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
          return CompletableResultCode.ofSuccess();
        }
      };
    }
  }

  @BeforeAll
  static void startContainers() {
    Startables.deepStart(KAFKA, ES).join();
    admin = KafkaTopics.awaitedAdmin(KAFKA.getBootstrapServers());
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

  @Test
  void prometheusSpansAndProbesAfterRealTraffic() throws Exception {
    KafkaTopics.create(
        admin,
        List.of(
            new NewTopic(TOPIC, 2, (short) 1),
            new NewTopic(TOPIC + ".kafkafim-dlq", 1, (short) 1)));
    es(
        "PUT",
        "/tel-docs",
        "{\"mappings\":{\"dynamic\":\"strict\",\"properties\":"
            + "{\"id\":{\"type\":\"keyword\"},\"n\":{\"type\":\"integer\"}}}}");
    ConfigurableApplicationContext app = start();
    PipelineSupervisor supervisor = app.getBean(PipelineSupervisor.class);
    await().atMost(TIMEOUT).until(() -> supervisor.state("tel") == PipelineState.RUNNING);

    for (int i = 0; i < 10; i++) {
      send("{\"id\":\"g-" + i + "\",\"n\":" + i + "}", String.format("%032x", i + 1));
    }
    send("{\"id\":\"bad\",\"n\":\"not a number\"}", BAD_TRACE);
    send("not json", null);
    producer.flush();
    await().atMost(TIMEOUT).until(() -> committed() == endOffsets());

    // ---- metrics ----
    String port = app.getEnvironment().getProperty("local.server.port");
    await()
        .atMost(TIMEOUT)
        .untilAsserted(() -> {
          String scrape = get(port, "/actuator/prometheus");
          String base = "kafka_cluster=\"local\",pipeline=\"tel\"";
          assertThat(value(scrape, "kafkafim_records_consumed_total", base, "topic=\"tel.events\""))
              .isEqualTo(12);
          assertThat(value(scrape, "kafkafim_bytes_consumed_total", base)).isPositive();
          assertThat(
                  value(scrape, "kafkafim_docs_total", base, "outcome=\"indexed\"", "route=\"0\""))
              .isEqualTo(10);
          assertThat(value(scrape, "kafkafim_docs_total", base, "outcome=\"dlq\"", "route=\"0\""))
              .isEqualTo(1);
          assertThat(
                  value(scrape, "kafkafim_docs_total", base, "outcome=\"dlq\"", "route=\"none\""))
              .isEqualTo(1);
          assertThat(value(scrape, "kafkafim_dlq_records_total", base, "error_type=\"es_error\""))
              .isEqualTo(1);
          assertThat(value(scrape, "kafkafim_dlq_records_total", base, "error_type=\"decode\""))
              .isEqualTo(1);
          assertThat(
                  value(
                      scrape,
                      "kafkafim_bulk_requests_total",
                      base,
                      "es_cluster=\"local\"",
                      "status=\"200\""))
              .isPositive();
          assertThat(value(scrape, "kafkafim_item_errors_total", base, "retryable=\"false\""))
              .isEqualTo(1);
          assertThat(value(scrape, "kafkafim_bulk_latency_seconds_count", base)).isPositive();
          assertThat(value(scrape, "kafkafim_bulk_docs_sum", base)).isEqualTo(11);
          assertThat(value(scrape, "kafkafim_bulk_bytes_sum", base)).isPositive();
          assertThat(value(scrape, "kafkafim_batch_flush_total", base, "es_cluster=\"local\""))
              .isPositive();
          assertThat(value(scrape, "kafkafim_e2e_latency_seconds_count", base)).isEqualTo(10);
          assertThat(value(scrape, "kafkafim_assigned_partitions", base)).isEqualTo(2);
          assertThat(value(scrape, "kafkafim_kafka_connected", base)).isEqualTo(1);
          assertThat(value(scrape, "kafkafim_in_flight_bytes", base)).isZero();
          assertThat(value(scrape, "kafkafim_breaker_state", "es_cluster=\"local\"")).isZero();
          // Registered from the configuration: the unauthenticated cluster's identity="none".
          assertThat(
                  value(
                      scrape,
                      "kafkafim_auth_failures_total",
                      base,
                      "es_cluster=\"local\"",
                      "identity=\"none\""))
              .isZero();
          assertThat(
                  value(scrape, "kafkafim_pipeline_state", "pipeline=\"tel\"", "state=\"running\""))
              .isEqualTo(1);
          assertThat(value(scrape, "kafkafim_config_generation")).isEqualTo(1);
          assertThat(
                  scrape
                      .lines()
                      .filter(l -> l.startsWith("kafkafim_"))
                      .collect(Collectors.joining("\n")))
              .as("no resolved index or partition labels, and no consumer lag (ADR-0034)")
              .doesNotContain("tel-docs", "partition=\"", "kafkafim_consumer_lag");
        });
    System.out.println(
        "---- /actuator/prometheus (kafkafim_*) ----\n"
            + get(port, "/actuator/prometheus")
                .lines()
                .filter(l -> l.startsWith("kafkafim_") && !l.contains("_bucket{"))
                .collect(Collectors.joining("\n")));

    // ---- probes ----
    String liveness = get(port, "/actuator/health/liveness");
    assertThat(liveness).contains("\"supervisor\"").contains("\"status\":\"UP\"");
    String readiness = get(port, "/actuator/health/readiness");
    assertThat(readiness)
        .contains("\"kafka\"", "\"pipelines\"")
        .doesNotContain("\"elasticsearch\"")
        .contains("\"kafka\":{\"status\":\"UP\"}")
        .endsWith("\"status\":\"UP\"}");

    // ---- traces: the failed batch is kept at probability 0 ----
    await()
        .atMost(TIMEOUT)
        .untilAsserted(() -> {
          SpanData batch =
              SpanCapture.SPANS.stream()
                  .filter(s -> s.getName().equals(BatchTracer.BATCH_SPAN))
                  .filter(s ->
                      s.getLinks().stream()
                          .anyMatch(l -> l.getSpanContext().getTraceId().equals(BAD_TRACE)))
                  .findFirst()
                  .orElseThrow();
          assertThat(batch.getAttributes().get(AttributeKey.stringKey("error.type")))
              .isEqualTo("item_errors");
          assertThat(batch.getTraceId()).isNotEqualTo(BAD_TRACE); // a link, not a parent
          assertThat(batch.getLinks())
              .anySatisfy(l -> assertThat(l.getSpanContext().getSpanId()).isEqualTo(BAD_SPAN));
          SpanData bulk =
              SpanCapture.SPANS.stream()
                  .filter(s -> s.getName().equals(BatchTracer.BULK_SPAN))
                  .filter(s -> s.getParentSpanId().equals(batch.getSpanId()))
                  .findFirst()
                  .orElseThrow();
          assertThat(bulk.getAttributes().get(AttributeKey.stringKey("kafkafim.es_cluster")))
              .isEqualTo("local");
          assertThat(bulk.getAttributes().get(AttributeKey.longKey("http.response.status_code")))
              .isEqualTo(200);
          assertThat(bulk.getAttributes().get(AttributeKey.longKey("kafkafim.item_errors")))
              .isEqualTo(1);
          assertThat(bulk.getAttributes().get(AttributeKey.longKey("kafkafim.retries"))).isZero();
          assertThat(bulk.getAttributes().get(AttributeKey.longKey("kafkafim.docs"))).isPositive();
          assertThat(bulk.getAttributes().get(AttributeKey.longKey("kafkafim.bytes"))).isPositive();
        });
    // Healthy batches are not sampled at probability 0.
    assertThat(SpanCapture.SPANS)
        .filteredOn(s -> s.getName().equals(BatchTracer.BATCH_SPAN))
        .allSatisfy(s ->
            assertThat(s.getAttributes().get(AttributeKey.stringKey("error.type"))).isNotNull());
  }

  // ---- application ----

  private ConfigurableApplicationContext start() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("spring.docker.compose.enabled", false);
    props.put("management.otlp.metrics.export.enabled", false);
    props.put("management.logging.export.otlp.enabled", false);
    props.put("management.tracing.export.otlp.enabled", false);
    props.put("management.tracing.sampling.probability", 0.0);
    props.put("management.opentelemetry.tracing.export.schedule-delay", "200ms");
    props.put("kafkafim.kafka.clusters.local.bootstrap-servers[0]", KAFKA.getBootstrapServers());
    props.put("kafkafim.elasticsearch.clusters.local.uris[0]", "http://" + ES.getHttpHostAddress());
    props.put("kafkafim.elasticsearch.clusters.local.bulk.max-age", "200ms");
    String p = "kafkafim.pipelines.tel.";
    props.put(p + "source.cluster", "local");
    props.put(p + "source.topics[0]", TOPIC);
    props.put(p + "source.offset-reset", "earliest");
    props.put(p + "source.commit-interval", "200ms");
    props.put(p + "routes[0].default", true);
    props.put(p + "routes[0].cluster", "local");
    props.put(p + "routes[0].index", "tel-docs");
    ConfigurableApplicationContext context =
        new SpringApplicationBuilder(KafkafimApplication.class, SpanCapture.class)
            .profiles("telemetry-it") // not `local`: no sample pipeline, no compose stack
            .properties(props)
            .run("--server.port=0");
    contexts.add(context);
    return context;
  }

  // ---- helpers ----

  private static void send(String value, String traceId) throws Exception {
    ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(TOPIC, null, value.getBytes(StandardCharsets.UTF_8));
    if (traceId != null) {
      String span = traceId.equals(BAD_TRACE) ? BAD_SPAN : "00000000000000a1";
      record
          .headers()
          .add(
              new RecordHeader(
                  "traceparent",
                  ("00-" + traceId + "-" + span + "-01").getBytes(StandardCharsets.UTF_8)));
    }
    producer.send(record).get();
  }

  private static long endOffsets() throws Exception {
    Map<TopicPartition, OffsetSpec> request = new HashMap<>();
    for (int p = 0; p < 2; p++) {
      request.put(new TopicPartition(TOPIC, p), OffsetSpec.latest());
    }
    return admin.listOffsets(request).all().get().values().stream()
        .mapToLong(info -> info.offset())
        .sum();
  }

  private static long committed() throws Exception {
    Map<TopicPartition, OffsetAndMetadata> offsets =
        admin.listConsumerGroupOffsets("kafkafim.tel").partitionsToOffsetAndMetadata().get();
    return offsets.values().stream().filter(o -> o != null).mapToLong(o -> o.offset()).sum();
  }

  /**
   * Asks 127.0.0.1, not {@code localhost}: see {@code PipelineSupervisorIntegrationTest} (#211).
   */
  private static String get(String port, String path) throws Exception {
    return http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).build(),
            HttpResponse.BodyHandlers.ofString())
        .body();
  }

  private static void es(String method, String path, String body) throws Exception {
    HttpResponse<String> response =
        http.send(
            HttpRequest.newBuilder(URI.create("http://" + ES.getHttpHostAddress() + path))
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
  }

  /**
   * The sum of every sample of {@code name} whose labels contain all of {@code labels}, or -1 if
   * there is none.
   */
  static double value(String scrape, String name, String... labels) {
    Pattern line = Pattern.compile("^" + Pattern.quote(name) + "(\\{[^}]*\\})? (\\S+)$");
    double sum = 0;
    boolean found = false;
    for (String text : scrape.lines().toList()) {
      Matcher matcher = line.matcher(text);
      if (!matcher.matches()) {
        continue;
      }
      String set = matcher.group(1) == null ? "" : matcher.group(1);
      boolean all = true;
      for (String label : labels) {
        for (String part : label.split(",")) {
          if (!set.contains(part)) {
            all = false;
          }
        }
      }
      if (all) {
        found = true;
        sum += Double.parseDouble(matcher.group(2));
      }
    }
    return found ? sum : -1;
  }
}
