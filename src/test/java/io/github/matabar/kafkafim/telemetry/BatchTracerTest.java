package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@code pipeline.batch} and {@code es.bulk} spans through Micrometer Tracing's OpenTelemetry
 * bridge, with the production sampler (DESIGN §10, ADR-0029).
 */
class BatchTracerTest {

  static final String TRACE_A = "4bf92f3577b34da6a3ce929d0e0e4736";
  static final String SPAN_A = "00f067aa0ba902b7";
  static final String TRACE_B = "0af7651916cd43dd8448eb211c80319c";
  static final String SPAN_B = "b7ad6b7169203331";

  /** Collects exported spans. */
  static final class Capture implements SpanExporter {
    final List<SpanData> spans = new CopyOnWriteArrayList<>();

    @Override
    public CompletableResultCode export(Collection<SpanData> batch) {
      spans.addAll(batch);
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

    SpanData named(String name) {
      return spans.stream().filter(s -> s.getName().equals(name)).findFirst().orElseThrow();
    }

    List<SpanData> all(String name) {
      return spans.stream().filter(s -> s.getName().equals(name)).toList();
    }
  }

  private final Capture capture = new Capture();
  private SdkTracerProvider provider;

  private BatchTracer tracer(double probability) {
    provider =
        SdkTracerProvider.builder()
            .setSampler(BatchSampler.create(probability))
            .addSpanProcessor(SimpleSpanProcessor.create(capture))
            .build();
    OtelTracer otel =
        new OtelTracer(provider.get("kafkafim-test"), new OtelCurrentTraceContext(), event -> {});
    return new BatchTracer(otel);
  }

  @AfterEach
  void shutdown() {
    if (provider != null) {
      provider.shutdown().join(5, TimeUnit.SECONDS);
    }
  }

  private static final Instant T0 = Instant.parse("2026-11-08T10:00:00.000001Z");

  private static BatchTrace batch(List<BatchTrace.BulkRequest> requests, List<String> parents) {
    return new BatchTrace(
        "orders",
        "prod-a",
        "search-eu",
        "writer",
        "docs",
        3,
        900,
        T0,
        T0.plusMillis(80),
        requests,
        parents);
  }

  private static BatchTrace.BulkRequest request(int status, int retries, int itemErrors) {
    return new BatchTrace.BulkRequest(
        T0.plusMillis(10), T0.plusMillis(30), 3, 900, status, null, retries, itemErrors);
  }

  @Test
  void batchSpanHasOneEsBulkChildPerRequestAndLinksTheRecordsTraces() {
    BatchTracer tracer = tracer(1.0);

    tracer.record(
        batch(
            List.of(request(503, 0, 0), request(200, 1, 0)),
            List.of(
                "00-" + TRACE_A + "-" + SPAN_A + "-01",
                "00-" + TRACE_A + "-" + SPAN_A + "-01", // duplicate: one link
                "00-" + TRACE_B + "-" + SPAN_B + "-00",
                "not-a-traceparent")));

    SpanData batch = capture.named(BatchTracer.BATCH_SPAN);
    assertThat(batch.getParentSpanContext().isValid()).isFalse(); // a root, never a child
    assertThat(batch.getKind()).isEqualTo(SpanKind.CONSUMER);
    assertThat(batch.getStartEpochNanos())
        .isEqualTo(TimeUnit.MILLISECONDS.toNanos(T0.toEpochMilli()) + 1_000);
    assertThat(batch.getEndEpochNanos() - batch.getStartEpochNanos())
        .isEqualTo(TimeUnit.MILLISECONDS.toNanos(80));
    assertThat(batch.getAttributes().get(AttributeKey.stringKey("kafkafim.pipeline")))
        .isEqualTo("orders");
    assertThat(batch.getAttributes().get(AttributeKey.stringKey("kafkafim.kafka_cluster")))
        .isEqualTo("prod-a");
    assertThat(batch.getAttributes().get(AttributeKey.longKey("kafkafim.docs"))).isEqualTo(3);
    assertThat(batch.getAttributes().get(AttributeKey.stringKey("error.type"))).isEqualTo("503");
    assertThat(batch.getLinks())
        .extracting(l -> l.getSpanContext().getTraceId(), l -> l.getSpanContext().getSpanId())
        .containsExactly(
            org.assertj.core.groups.Tuple.tuple(TRACE_A, SPAN_A),
            org.assertj.core.groups.Tuple.tuple(TRACE_B, SPAN_B));
    // The link is a link, not the parent: the batch has its own trace.
    assertThat(batch.getTraceId()).isNotIn(TRACE_A, TRACE_B);

    List<SpanData> bulks = capture.all(BatchTracer.BULK_SPAN);
    assertThat(bulks).hasSize(2);
    for (SpanData bulk : bulks) {
      assertThat(bulk.getParentSpanId()).isEqualTo(batch.getSpanId());
      assertThat(bulk.getTraceId()).isEqualTo(batch.getTraceId());
      assertThat(bulk.getKind()).isEqualTo(SpanKind.CLIENT);
      assertThat(bulk.getAttributes().get(AttributeKey.stringKey("kafkafim.es_cluster")))
          .isEqualTo("search-eu");
      assertThat(bulk.getAttributes().get(AttributeKey.longKey("kafkafim.docs"))).isEqualTo(3);
      assertThat(bulk.getAttributes().get(AttributeKey.longKey("kafkafim.bytes"))).isEqualTo(900);
    }
    assertThat(bulks)
        .extracting(
            b -> b.getAttributes().get(AttributeKey.longKey("http.response.status_code")),
            b -> b.getAttributes().get(AttributeKey.longKey("kafkafim.retries")),
            b -> b.getStatus().getStatusCode())
        .containsExactlyInAnyOrder(
            org.assertj.core.groups.Tuple.tuple(503L, 0L, StatusCode.ERROR),
            org.assertj.core.groups.Tuple.tuple(200L, 1L, StatusCode.UNSET));
  }

  @Test
  void healthyBatchesFollowTheProbabilityButFailedOnesAreAlwaysKept() {
    BatchTracer tracer = tracer(0.0);

    tracer.record(batch(List.of(request(200, 0, 0)), List.of()));
    assertThat(capture.spans).isEmpty();

    tracer.record(batch(List.of(request(200, 0, 2)), List.of()));
    assertThat(capture.spans)
        .extracting(SpanData::getName)
        .containsExactlyInAnyOrder(BatchTracer.BATCH_SPAN, BatchTracer.BULK_SPAN);
    assertThat(
            capture
                .named(BatchTracer.BATCH_SPAN)
                .getAttributes()
                .get(AttributeKey.stringKey("error.type")))
        .isEqualTo("item_errors");

    capture.spans.clear();
    tracer.record(
        batch(
            List.of(
                new BatchTrace.BulkRequest(
                    T0, T0.plusMillis(1), 3, 900, 0, "java.net.ConnectException: refused", 0, 0),
                request(200, 1, 0)),
            List.of()));
    assertThat(capture.named(BatchTracer.BATCH_SPAN).getStatus().getStatusCode())
        .isEqualTo(StatusCode.ERROR);
    assertThat(capture.all(BatchTracer.BULK_SPAN)).hasSize(2);
  }

  @Test
  void linksAreCappedAtTheLimit() {
    BatchTracer tracer = tracer(1.0);
    List<String> parents =
        java.util.stream.IntStream.range(1, 300)
            .mapToObj(i -> String.format("00-%032x-%016x-01", i, i))
            .toList();

    tracer.record(batch(List.of(request(200, 0, 0)), parents));

    assertThat(capture.named(BatchTracer.BATCH_SPAN).getLinks()).hasSize(BatchTracer.MAX_LINKS);
  }
}
