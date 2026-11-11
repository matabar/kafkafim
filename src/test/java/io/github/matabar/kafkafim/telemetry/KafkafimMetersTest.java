package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Meter binding of DESIGN §10 against a {@link SimpleMeterRegistry}, and the Prometheus names. */
class KafkafimMetersTest {

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final KafkafimMeters meters = new KafkafimMeters(registry);
  private final Map<String, CircuitBreaker.State> breakers = new HashMap<>();

  @AfterEach
  void close() {
    meters.close();
  }

  /** A probe with settable values. */
  static final class Probe implements SourceProbe {
    final AtomicLong inFlight = new AtomicLong();
    final AtomicInteger assigned = new AtomicInteger();
    volatile boolean connected;

    @Override
    public long inFlightBytes() {
      return inFlight.get();
    }

    @Override
    public int assignedPartitions() {
      return assigned.get();
    }

    @Override
    public boolean connected() {
      return connected;
    }
  }

  private static PipelineSpec spec(String kafka, List<String> topics, SourceProbe probe) {
    return new PipelineSpec(kafka, topics, List.of("eu", "1"), Set.of("search-eu"), probe);
  }

  private void sync(Map<String, PipelineSpec> specs) {
    Set<String> clusters =
        specs.values().stream()
            .flatMap(s -> s.esClusters().stream())
            .collect(Collectors.toCollection(LinkedHashSet::new));
    meters.sync(specs, clusters, breakers::get);
  }

  private double counter(String name, String... tags) {
    return registry.get(name).tags(tags).counter().count();
  }

  private double gauge(String name, String... tags) {
    return registry.get(name).tags(tags).gauge().value();
  }

  private Set<String> pipelinesOf(String name) {
    return registry.find(name).meters().stream()
        .map(m -> m.getId().getTag("pipeline"))
        .collect(Collectors.toSet());
  }

  @Test
  void configuredSeriesExistFromTheStartWithBothBaseLabels() {
    Probe probe = new Probe();
    sync(Map.of("orders", spec("prod-a", List.of("orders.v1", "orders.v2"), probe)));

    for (String topic : List.of("orders.v1", "orders.v2")) {
      String[] tags = {"pipeline", "orders", "kafka_cluster", "prod-a", "topic", topic};
      assertThat(counter(TelemetryNames.RECORDS_CONSUMED, tags)).isZero();
      assertThat(counter(TelemetryNames.BYTES_CONSUMED, tags)).isZero();
    }
    for (String route : List.of("eu", "1")) {
      for (String outcome : List.of("indexed", "stale", "dlq")) {
        assertThat(
                counter(
                    TelemetryNames.DOCS,
                    "pipeline",
                    "orders",
                    "kafka_cluster",
                    "prod-a",
                    "route",
                    route,
                    "outcome",
                    outcome))
            .isZero();
      }
    }
    // Pre-routing failures (route="none") can only end in the DLQ.
    assertThat(
            counter(
                TelemetryNames.DOCS,
                "pipeline",
                "orders",
                "kafka_cluster",
                "prod-a",
                "route",
                TelemetryNames.NO_ROUTE,
                "outcome",
                "dlq"))
        .isZero();
    assertThat(
            registry
                .find(TelemetryNames.DOCS)
                .tag("route", TelemetryNames.NO_ROUTE)
                .meters()
                .stream()
                .map(m -> m.getId().getTag("outcome"))
                .toList())
        .containsExactly("dlq");
    assertThat(registry.get(TelemetryNames.BULK_LATENCY).tag("es_cluster", "search-eu").timer())
        .isNotNull();
    assertThat(registry.get(TelemetryNames.E2E_LATENCY).tag("pipeline", "orders").timer())
        .isNotNull();
    assertThat(gauge(TelemetryNames.BREAKER_STATE, "es_cluster", "search-eu")).isZero();
    // Every pipeline series has pipeline and kafka_cluster.
    for (Meter meter : registry.getMeters()) {
      if (!meter.getId().getName().equals(TelemetryNames.BREAKER_STATE)) {
        assertThat(meter.getId().getTags())
            .extracting(Tag::getKey)
            .as(meter.getId().toString())
            .contains("pipeline", "kafka_cluster")
            .doesNotContain("partition", "index");
      }
    }
  }

  @Test
  void eventsLandOnTheirSeries() {
    sync(Map.of("orders", spec("prod-a", List.of("orders.v1"), new Probe())));
    PipelineMeters orders = meters.pipeline("orders");

    orders.consumed("orders.v1", 120);
    orders.consumed("orders.v1", 30);
    orders.document("eu", DocOutcome.INDEXED);
    orders.document(TelemetryNames.NO_ROUTE, DocOutcome.DLQ);
    orders.endToEnd(Duration.ofMillis(250));
    orders.endToEnd(Duration.ofMillis(-5));
    orders.flush("search-eu", "DOCS");
    orders.bulkResponse("search-eu", 200, 10, 4096, Duration.ofMillis(40));
    orders.bulkResponse("search-eu", 429, 10, 4096, Duration.ofMillis(5));
    orders.bulkTransportError("search-eu");
    orders.itemError("search-eu", "mapper_parsing_exception", false);
    orders.itemError("search-eu", "es_rejected_execution_exception", true);
    orders.retry("search-eu", "request");
    orders.retry("search-eu", "item");
    orders.authFailure("search-eu", "writer");
    orders.authFailure("search-eu", null);
    orders.dlqRecord("es_error");

    String[] base = {"pipeline", "orders", "kafka_cluster", "prod-a"};
    assertThat(counter(TelemetryNames.RECORDS_CONSUMED, with(base, "topic", "orders.v1")))
        .isEqualTo(2);
    assertThat(counter(TelemetryNames.BYTES_CONSUMED, with(base, "topic", "orders.v1")))
        .isEqualTo(150);
    assertThat(counter(TelemetryNames.DOCS, with(base, "route", "eu", "outcome", "indexed")))
        .isOne();
    assertThat(counter(TelemetryNames.DOCS, with(base, "route", "none", "outcome", "dlq"))).isOne();
    assertThat(registry.get(TelemetryNames.E2E_LATENCY).tags(base).timer().count()).isEqualTo(2);
    assertThat(
            registry.get(TelemetryNames.E2E_LATENCY).tags(base).timer().max(TimeUnit.MILLISECONDS))
        .isEqualTo(250);
    assertThat(
            counter(
                TelemetryNames.BATCH_FLUSH,
                with(base, "es_cluster", "search-eu", "reason", "docs")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.BULK_REQUESTS,
                with(base, "es_cluster", "search-eu", "status", "200")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.BULK_REQUESTS,
                with(base, "es_cluster", "search-eu", "status", "429")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.BULK_REQUESTS,
                with(base, "es_cluster", "search-eu", "status", "io_error")))
        .isOne();
    assertThat(registry.get(TelemetryNames.BULK_DOCS).tags(base).summary().totalAmount())
        .isEqualTo(20);
    assertThat(registry.get(TelemetryNames.BULK_BYTES).tags(base).summary().totalAmount())
        .isEqualTo(8192);
    assertThat(registry.get(TelemetryNames.BULK_LATENCY).tags(base).timer().count()).isEqualTo(2);
    assertThat(
            counter(
                TelemetryNames.ITEM_ERRORS,
                with(
                    base,
                    "es_cluster",
                    "search-eu",
                    "error_type",
                    "mapper_parsing_exception",
                    "retryable",
                    "false")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.ITEM_ERRORS,
                with(
                    base,
                    "es_cluster",
                    "search-eu",
                    "error_type",
                    "es_rejected_execution_exception",
                    "retryable",
                    "true")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.RETRIES, with(base, "es_cluster", "search-eu", "scope", "request")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.AUTH_FAILURES,
                with(base, "es_cluster", "search-eu", "identity", "writer")))
        .isOne();
    assertThat(
            counter(
                TelemetryNames.AUTH_FAILURES,
                with(base, "es_cluster", "search-eu", "identity", "none")))
        .isOne();
    assertThat(counter(TelemetryNames.DLQ_RECORDS, with(base, "error_type", "es_error"))).isOne();
  }

  @Test
  void gaugesReadTheProbe() {
    Probe probe = new Probe();
    sync(Map.of("orders", spec("prod-a", List.of("orders.v1", "orders.v2"), probe)));
    probe.inFlight.set(2048);
    probe.assigned.set(3);
    probe.connected = true;
    final String[] base = {"pipeline", "orders", "kafka_cluster", "prod-a"};
    assertThat(gauge(TelemetryNames.IN_FLIGHT_BYTES, base)).isEqualTo(2048);
    assertThat(gauge(TelemetryNames.ASSIGNED_PARTITIONS, base)).isEqualTo(3);
    assertThat(gauge(TelemetryNames.KAFKA_CONNECTED, base)).isOne();
  }

  @Test
  void breakerGaugeEncodesTheState() {
    sync(Map.of("orders", spec("prod-a", List.of("t"), new Probe())));

    breakers.put("search-eu", CircuitBreaker.State.OPEN);
    assertThat(gauge(TelemetryNames.BREAKER_STATE, "es_cluster", "search-eu")).isEqualTo(2);
    breakers.put("search-eu", CircuitBreaker.State.HALF_OPEN);
    assertThat(gauge(TelemetryNames.BREAKER_STATE, "es_cluster", "search-eu")).isOne();
    breakers.put("search-eu", CircuitBreaker.State.CLOSED);
    assertThat(gauge(TelemetryNames.BREAKER_STATE, "es_cluster", "search-eu")).isZero();
  }

  @Test
  void reloadRemovesSeriesOfRemovedPipelinesAndAddsThoseOfNewOnes() {
    Map<String, PipelineSpec> first = new LinkedHashMap<>();
    first.put("orders", spec("prod-a", List.of("orders.v1"), new Probe()));
    first.put("audit", spec("prod-a", List.of("audit"), new Probe()));
    sync(first);
    meters.pipeline("orders").consumed("orders.v1", 10);
    final PipelineMeters audit = meters.pipeline("audit");

    Map<String, PipelineSpec> second = new LinkedHashMap<>();
    second.put("orders", spec("prod-a", List.of("orders.v1"), new Probe()));
    second.put("billing", spec("prod-b", List.of("billing"), new Probe()));
    sync(second);

    assertThat(pipelinesOf(TelemetryNames.RECORDS_CONSUMED)).containsOnly("orders", "billing");
    assertThat(pipelinesOf(TelemetryNames.IN_FLIGHT_BYTES)).containsOnly("orders", "billing");
    // An unchanged pipeline keeps its counters across the reload.
    assertThat(
            counter(
                TelemetryNames.RECORDS_CONSUMED,
                "pipeline",
                "orders",
                "kafka_cluster",
                "prod-a",
                "topic",
                "orders.v1"))
        .isEqualTo(1);
    // Late events of the removed pipeline create nothing.
    audit.consumed("audit", 5);
    audit.dlqRecord("es_error");
    assertThat(meters.pipeline("audit")).isSameAs(PipelineMeters.NOOP);
    assertThat(pipelinesOf(TelemetryNames.DLQ_RECORDS)).doesNotContain("audit");
    assertThat(pipelinesOf(TelemetryNames.RECORDS_CONSUMED)).doesNotContain("audit");
  }

  @Test
  void relabelledPipelineGetsFreshSeriesAndLosesTheOldOnes() {
    sync(Map.of("orders", spec("prod-a", List.of("orders.v1"), new Probe())));
    sync(Map.of("orders", spec("prod-b", List.of("orders.v1"), new Probe())));

    assertThat(
            registry.find(TelemetryNames.RECORDS_CONSUMED).meters().stream()
                .map(m -> m.getId().getTag("kafka_cluster"))
                .toList())
        .containsExactly("prod-b");
  }

  @Test
  void breakerSeriesFollowTheUsedClusters() {
    sync(Map.of("orders", spec("prod-a", List.of("t"), new Probe())));
    meters.sync(Map.of(), Set.of(), breakers::get);

    assertThat(registry.find(TelemetryNames.BREAKER_STATE).meters()).isEmpty();
    assertThat(registry.getMeters()).isEmpty();
  }

  @Test
  void noopMetersRecordNothing() {
    PipelineMeters.NOOP.consumed("t", 1);
    PipelineMeters.NOOP.document("r", DocOutcome.INDEXED);
    PipelineMeters.NOOP.bulkResponse("c", 200, 1, 1, Duration.ZERO);
    assertThat(registry.getMeters()).isEmpty();
    assertThat(meters.pipeline("unknown")).isSameAs(PipelineMeters.NOOP);
  }

  /** The §10 names, as /actuator/prometheus renders them. */
  @Test
  void prometheusRendersTheDesignNames() {
    PrometheusMeterRegistry prometheus = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    try (KafkafimMeters onPrometheus = new KafkafimMeters(prometheus)) {
      onPrometheus.sync(
          Map.of("orders", spec("prod-a", List.of("orders.v1"), new Probe())),
          Set.of("search-eu"),
          c -> CircuitBreaker.State.CLOSED);
      PipelineMeters orders = onPrometheus.pipeline("orders");
      orders.flush("search-eu", "AGE");
      orders.bulkResponse("search-eu", 200, 3, 300, Duration.ofMillis(3));
      orders.itemError("search-eu", "mapper_parsing_exception", false);
      orders.retry("search-eu", "request");
      orders.authFailure("search-eu", "writer");
      orders.dlqRecord("decode");

      String scrape = prometheus.scrape();
      assertThat(scrape)
          .contains(
              "kafkafim_records_consumed_total{kafka_cluster=\"prod-a\",pipeline=\"orders\","
                  + "topic=\"orders.v1\"} 0.0",
              "kafkafim_bytes_consumed_total{",
              "kafkafim_docs_total{kafka_cluster=\"prod-a\",outcome=\"indexed\","
                  + "pipeline=\"orders\",route=\"eu\"} 0.0",
              "kafkafim_bulk_requests_total{es_cluster=\"search-eu\",kafka_cluster=\"prod-a\","
                  + "pipeline=\"orders\",status=\"200\"} 1.0",
              "kafkafim_bulk_latency_seconds_bucket{",
              "kafkafim_bulk_latency_seconds_count{",
              "kafkafim_bulk_docs_bucket{",
              "kafkafim_bulk_bytes_bucket{",
              "kafkafim_item_errors_total{error_type=\"mapper_parsing_exception\","
                  + "es_cluster=\"search-eu\",kafka_cluster=\"prod-a\",pipeline=\"orders\","
                  + "retryable=\"false\"} 1.0",
              "kafkafim_retries_total{",
              "kafkafim_breaker_state{es_cluster=\"search-eu\"} 0.0",
              "kafkafim_auth_failures_total{es_cluster=\"search-eu\",identity=\"writer\",",
              "kafkafim_e2e_latency_seconds_bucket{",
              "kafkafim_batch_flush_total{es_cluster=\"search-eu\",kafka_cluster=\"prod-a\","
                  + "pipeline=\"orders\",reason=\"age\"} 1.0",
              "kafkafim_in_flight_bytes{kafka_cluster=\"prod-a\",pipeline=\"orders\"} 0.0",
              "kafkafim_assigned_partitions{kafka_cluster=\"prod-a\",pipeline=\"orders\"} 0.0",
              "kafkafim_kafka_connected{kafka_cluster=\"prod-a\",pipeline=\"orders\"} 0.0",
              "kafkafim_dlq_records_total{error_type=\"decode\",kafka_cluster=\"prod-a\","
                  + "pipeline=\"orders\"} 1.0")
          .doesNotContain(
              "kafkafim_bulk_bytes_bytes",
              "kafkafim_in_flight_bytes_bytes",
              "kafkafim_consumer_lag");
    } finally {
      prometheus.close();
    }
  }

  private static String[] with(String[] base, String... more) {
    String[] all = new String[base.length + more.length];
    System.arraycopy(base, 0, all, 0, base.length);
    System.arraycopy(more, 0, all, base.length, more.length);
    return all;
  }
}
