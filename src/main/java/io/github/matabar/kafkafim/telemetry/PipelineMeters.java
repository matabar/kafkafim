package io.github.matabar.kafkafim.telemetry;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.ToDoubleFunction;
import org.jspecify.annotations.Nullable;

/**
 * The meters of one pipeline (DESIGN §10). Every series carries {@code pipeline} and {@code
 * kafka_cluster}; {@code topic}, {@code es_cluster} and {@code route} where they apply. A {@code
 * route} is the route's {@code name} or position, never a resolved index, and there are no
 * partition labels.
 *
 * <p>Series that are known from the configuration (consumption per topic, documents per route and
 * outcome plus {@code route="none", outcome="dlq"}, bulk histograms per Elasticsearch cluster, auth
 * failures per Elasticsearch cluster and identity, the gauges) exist from the start with 0, so
 * {@code rate()} works from the first event; the others (statuses, error types) appear with their
 * first event. {@link #close()} removes every series of the pipeline, which is what a reload does
 * for a removed pipeline (ADR-0028); events after that are ignored.
 *
 * <p>Thread-safe; the recording methods are called from consumer, sender and DLQ threads.
 */
public final class PipelineMeters {

  /** Meters that record nothing; for pipelines that are not (or no longer) configured. */
  public static final PipelineMeters NOOP = new PipelineMeters();

  /** Hands out no-op meters for events after {@link #close()}. */
  private static final MeterRegistry DETACHED = detached();

  private static final double[] LATENCY_SLOS_SECONDS = {
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60
  };
  private static final double[] E2E_SLOS_SECONDS = {
    0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 900, 3600
  };
  private static final double[] DOCS_SLOS = {1, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000};
  private static final double[] BYTES_SLOS = {
    1024, 16384, 65536, 262144, 1048576, 4194304, 10485760, 26214400, 52428800, 104857600
  };

  private final @Nullable MeterRegistry registry;
  private final String pipeline;
  private final PipelineSpec spec;
  private final Tags base;
  private final Map<String, Meter> meters = new ConcurrentHashMap<>();
  // Hot-path caches (per record, per document): no key building per event.
  private final Map<String, Counter[]> consumedByTopic = new ConcurrentHashMap<>();
  private final Map<String, Counter[]> docsByRoute = new ConcurrentHashMap<>();
  private final @Nullable Timer e2e;
  private volatile SourceProbe probe;
  private volatile boolean closed;

  private PipelineMeters() {
    this.registry = null;
    this.pipeline = "";
    this.spec = new PipelineSpec("", List.of(), List.of(), Set.of(), SourceProbe.NONE);
    this.base = Tags.empty();
    this.probe = SourceProbe.NONE;
    this.e2e = null;
    this.closed = true;
  }

  PipelineMeters(MeterRegistry registry, String pipeline, PipelineSpec spec) {
    this.registry = registry;
    this.pipeline = pipeline;
    this.spec = spec;
    this.base = Tags.of(TelemetryNames.PIPELINE, pipeline, "kafka_cluster", spec.kafkaCluster());
    this.probe = spec.probe();
    gauge(TelemetryNames.IN_FLIGHT_BYTES, "bytes", m -> m.probe.inFlightBytes(), Tags.empty());
    gauge(
        TelemetryNames.ASSIGNED_PARTITIONS, null, m -> m.probe.assignedPartitions(), Tags.empty());
    gauge(TelemetryNames.KAFKA_CONNECTED, null, m -> m.probe.connected() ? 1 : 0, Tags.empty());
    for (String topic : spec.topics()) {
      counter(TelemetryNames.RECORDS_CONSUMED, topic(topic));
      counter(TelemetryNames.BYTES_CONSUMED, topic(topic));
    }
    for (String route : spec.routes()) {
      for (DocOutcome outcome : DocOutcome.values()) {
        counter(TelemetryNames.DOCS, Tags.of("route", route, "outcome", outcome.label()));
      }
    }
    // Documents that fail before a route is selected only ever end in the DLQ.
    counter(
        TelemetryNames.DOCS,
        Tags.of("route", TelemetryNames.NO_ROUTE, "outcome", DocOutcome.DLQ.label()));
    for (String cluster : spec.esClusters()) {
      bulkLatency(cluster);
      bulkDocs(cluster);
      bulkBytes(cluster);
    }
    for (PipelineSpec.EsStream stream : spec.esStreams()) {
      authFailures(stream.esCluster(), stream.identityLabel());
    }
    this.e2e = timer(TelemetryNames.E2E_LATENCY, E2E_SLOS_SECONDS, Tags.empty());
  }

  /** The pipeline name. */
  public String pipeline() {
    return pipeline;
  }

  /** The source Kafka cluster label. */
  public String kafkaCluster() {
    return spec.kafkaCluster();
  }

  PipelineSpec spec() {
    return spec;
  }

  /** The source view the gauges read. */
  SourceProbe probe() {
    return probe;
  }

  void probe(SourceProbe next) {
    this.probe = next;
  }

  // ---- source ----

  /**
   * One record was delivered by the source.
   *
   * @param topic its topic
   * @param bytes key plus value length
   */
  public void consumed(String topic, long bytes) {
    Counter[] counters = consumedByTopic.get(topic);
    if (counters == null) {
      counters =
          new Counter[] {
            counter(TelemetryNames.RECORDS_CONSUMED, topic(topic)),
            counter(TelemetryNames.BYTES_CONSUMED, topic(topic))
          };
      if (!closed) {
        consumedByTopic.putIfAbsent(topic, counters);
      }
    }
    counters[0].increment();
    counters[1].increment(bytes);
  }

  // ---- documents ----

  /**
   * One routed copy or failed document is done.
   *
   * @param route the route label, or {@link TelemetryNames#NO_ROUTE} for a document that failed
   *     before a route was selected
   * @param outcome how it ended
   */
  public void document(String route, DocOutcome outcome) {
    Counter[] counters = docsByRoute.get(route);
    if (counters == null) {
      counters = new Counter[DocOutcome.values().length];
      if (!closed) {
        Counter[] raced = docsByRoute.putIfAbsent(route, counters);
        if (raced != null) {
          counters = raced;
        }
      }
    }
    Counter counter = counters[outcome.ordinal()];
    if (counter == null) {
      // Registered per outcome on first use, so route="none" only ever gets outcome="dlq".
      counter = counter(TelemetryNames.DOCS, Tags.of("route", route, "outcome", outcome.label()));
      counters[outcome.ordinal()] = counter;
    }
    counter.increment();
  }

  /**
   * End-to-end latency of one acknowledged document: ack time minus the Kafka record timestamp.
   * Negative values (clock skew) count as zero.
   */
  public void endToEnd(Duration latency) {
    if (!closed && e2e != null) {
      e2e.record(latency.isNegative() ? Duration.ZERO : latency);
    }
  }

  // ---- Elasticsearch ----

  /** A batch was sealed ({@code reason}: {@code docs}, {@code bytes}, {@code age}, ...). */
  public void flush(String esCluster, String reason) {
    counter(
            TelemetryNames.BATCH_FLUSH,
            Tags.of(TelemetryNames.ES_CLUSTER, esCluster, "reason", lower(reason)))
        .increment();
  }

  /** A bulk request got an HTTP response. */
  public void bulkResponse(String esCluster, int status, int docs, long bytes, Duration latency) {
    bulkRequest(esCluster, Integer.toString(status));
    bulkLatency(esCluster).record(latency);
    bulkDocs(esCluster).record(docs);
    bulkBytes(esCluster).record(bytes);
  }

  /** A bulk request failed without an HTTP response ({@code status="io_error"}). */
  public void bulkTransportError(String esCluster) {
    bulkRequest(esCluster, TelemetryNames.IO_ERROR_STATUS);
  }

  /**
   * An item failed.
   *
   * @param errorType the Elasticsearch error type ({@code status_<n>} when the item had none)
   * @param retryable whether it will be sent again (subject to the poison bound)
   */
  public void itemError(String esCluster, String errorType, boolean retryable) {
    counter(
            TelemetryNames.ITEM_ERRORS,
            Tags.of(
                TelemetryNames.ES_CLUSTER,
                esCluster,
                "error_type",
                errorType,
                "retryable",
                Boolean.toString(retryable)))
        .increment();
  }

  /** A request ({@code scope="request"}) or an item ({@code scope="item"}) failed retryably. */
  public void retry(String esCluster, String scope) {
    counter(TelemetryNames.RETRIES, Tags.of(TelemetryNames.ES_CLUSTER, esCluster, "scope", scope))
        .increment();
  }

  /** Elasticsearch rejected an identity with 401/403. */
  public void authFailure(String esCluster, @Nullable String identity) {
    authFailures(esCluster, identity == null ? TelemetryNames.NONE : identity).increment();
  }

  // ---- DLQ ----

  /** A DLQ record was acknowledged by the broker. */
  public void dlqRecord(String errorType) {
    counter(TelemetryNames.DLQ_RECORDS, Tags.of("error_type", errorType)).increment();
  }

  // ---- registration ----

  /** Removes every series of this pipeline; later events are ignored. */
  synchronized void close() {
    closed = true;
    if (registry != null) {
      meters.values().forEach(registry::remove);
    }
    meters.clear();
    consumedByTopic.clear();
    docsByRoute.clear();
  }

  private void bulkRequest(String esCluster, String status) {
    counter(
            TelemetryNames.BULK_REQUESTS,
            Tags.of(TelemetryNames.ES_CLUSTER, esCluster, "status", status))
        .increment();
  }

  private Counter authFailures(String esCluster, String identity) {
    return counter(
        TelemetryNames.AUTH_FAILURES,
        Tags.of(TelemetryNames.ES_CLUSTER, esCluster, "identity", identity));
  }

  private Timer bulkLatency(String esCluster) {
    return timer(
        TelemetryNames.BULK_LATENCY,
        LATENCY_SLOS_SECONDS,
        Tags.of(TelemetryNames.ES_CLUSTER, esCluster));
  }

  private DistributionSummary bulkDocs(String esCluster) {
    return summary(
        TelemetryNames.BULK_DOCS, null, DOCS_SLOS, Tags.of(TelemetryNames.ES_CLUSTER, esCluster));
  }

  private DistributionSummary bulkBytes(String esCluster) {
    return summary(
        TelemetryNames.BULK_BYTES,
        "bytes",
        BYTES_SLOS,
        Tags.of(TelemetryNames.ES_CLUSTER, esCluster));
  }

  private static Tags topic(String topic) {
    return Tags.of("topic", topic);
  }

  private static String lower(String value) {
    return value.toLowerCase(Locale.ROOT);
  }

  private Counter counter(String name, Tags tags) {
    return (Counter)
        meter(
            name,
            tags,
            (target, all) ->
                Counter.builder(name)
                    .description(TelemetryNames.description(name))
                    .tags(all)
                    .register(target));
  }

  private Timer timer(String name, double[] slosSeconds, Tags tags) {
    return (Timer)
        meter(
            name,
            tags,
            (target, all) ->
                Timer.builder(name)
                    .description(TelemetryNames.description(name))
                    .tags(all)
                    .serviceLevelObjectives(seconds(slosSeconds))
                    .register(target));
  }

  private DistributionSummary summary(
      String name, @Nullable String unit, double[] slos, Tags tags) {
    return (DistributionSummary)
        meter(
            name,
            tags,
            (target, all) ->
                DistributionSummary.builder(name)
                    .description(TelemetryNames.description(name))
                    .baseUnit(unit)
                    .tags(all)
                    .serviceLevelObjectives(slos)
                    .register(target));
  }

  private void gauge(
      String name, @Nullable String unit, ToDoubleFunction<PipelineMeters> value, Tags tags) {
    meter(
        name,
        tags,
        (target, all) ->
            Gauge.builder(name, this, value)
                .description(TelemetryNames.description(name))
                .baseUnit(unit)
                .tags(all)
                .register(target));
  }

  /**
   * The cached meter for {@code name} and {@code tags}, registered on first use. After {@link
   * #close()}, and for {@link #NOOP}, a no-op meter that nothing exports.
   */
  private Meter meter(String name, Tags tags, BiFunction<MeterRegistry, Tags, Meter> register) {
    String key = name + tags;
    Meter cached = meters.get(key);
    if (cached != null) {
      return cached;
    }
    synchronized (this) {
      cached = meters.get(key);
      if (cached != null) {
        return cached;
      }
      if (closed || registry == null) {
        return register.apply(DETACHED, tags);
      }
      Meter created = register.apply(registry, base.and(tags));
      meters.put(key, created);
      return created;
    }
  }

  private static Duration[] seconds(double[] values) {
    List<Duration> durations = new ArrayList<>(values.length);
    for (double value : values) {
      durations.add(Duration.ofNanos((long) (value * 1_000_000_000L)));
    }
    return durations.toArray(Duration[]::new);
  }

  private static MeterRegistry detached() {
    MeterRegistry registry = new SimpleMeterRegistry();
    registry.config().meterFilter(MeterFilter.deny());
    return registry;
  }
}
