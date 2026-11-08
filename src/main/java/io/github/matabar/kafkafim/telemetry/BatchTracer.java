package io.github.matabar.kafkafim.telemetry;

import io.micrometer.tracing.Link;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Turns finished batches into spans (DESIGN §10, ADR-0029): one {@code pipeline.batch} root span
 * per batch, from sealing to the end of its last request, with one {@code es.bulk} child per bulk
 * request (retries and 413 halves included). Each distinct valid {@code traceparent} of the batch's
 * source records becomes a span <em>link</em>, never a parent, because a batch mixes documents of
 * many traces. There are no per-document spans.
 *
 * <p>The spans are recorded after the fact with their real timestamps, so the sampler sees at the
 * start of the root span whether the batch failed: {@link BatchSampler} keeps every failed batch
 * and the configured fraction (1% in production) of the others.
 */
public final class BatchTracer {

  /** Name of the batch span. */
  public static final String BATCH_SPAN = "pipeline.batch";

  /** Name of the per-request span. */
  public static final String BULK_SPAN = "es.bulk";

  /** OpenTelemetry's {@code error.type} attribute; set on failed batches and requests. */
  public static final String ERROR_TYPE = "error.type";

  /** At most this many links per batch span (the OpenTelemetry SDK's default limit). */
  public static final int MAX_LINKS = 128;

  private final Tracer tracer;

  /**
   * Creates the tracer.
   *
   * @param tracer Micrometer Tracing over the OpenTelemetry bridge, or {@link Tracer#NOOP}
   */
  public BatchTracer(Tracer tracer) {
    this.tracer = Objects.requireNonNull(tracer, "tracer");
  }

  /** Records the spans of one finished batch. */
  public void record(BatchTrace batch) {
    String errorType = batch.errorType();
    Span.Builder builder =
        tracer
            .spanBuilder()
            .name(BATCH_SPAN)
            .setNoParent()
            .kind(Span.Kind.CONSUMER)
            .startTimestamp(micros(batch.started()), TimeUnit.MICROSECONDS)
            .tag("kafkafim.pipeline", batch.pipeline())
            .tag("kafkafim.kafka_cluster", batch.kafkaCluster())
            .tag("kafkafim.es_cluster", batch.esCluster())
            .tag("kafkafim.flush_reason", batch.flushReason())
            .tag("kafkafim.docs", batch.docs())
            .tag("kafkafim.bytes", batch.bytes())
            .tag("kafkafim.requests", batch.requests().size());
    if (batch.identity() != null) {
      builder.tag("kafkafim.es_identity", batch.identity());
    }
    if (errorType != null) {
      builder.tag(ERROR_TYPE, errorType);
    }
    for (TraceContext link : links(batch)) {
      builder.addLink(new Link(link));
    }
    Span span = builder.start();
    if (errorType != null) {
      span.error(new BatchFailure("batch failed: " + errorType));
    }
    for (BatchTrace.BulkRequest request : batch.requests()) {
      record(span, batch, request);
    }
    span.end(micros(batch.ended()), TimeUnit.MICROSECONDS);
  }

  private void record(Span parent, BatchTrace batch, BatchTrace.BulkRequest request) {
    String errorType = request.errorType();
    Span.Builder builder =
        tracer
            .spanBuilder()
            .name(BULK_SPAN)
            .setParent(parent.context())
            .kind(Span.Kind.CLIENT)
            .startTimestamp(micros(request.started()), TimeUnit.MICROSECONDS)
            .tag("kafkafim.es_cluster", batch.esCluster())
            .tag("kafkafim.docs", request.docs())
            .tag("kafkafim.bytes", request.bytes())
            .tag("kafkafim.retries", request.retries())
            .tag("kafkafim.item_errors", request.itemErrors());
    if (request.status() > 0) {
      builder.tag("http.response.status_code", request.status());
    }
    if (errorType != null) {
      builder.tag(ERROR_TYPE, errorType);
    }
    Span span = builder.start();
    if (errorType != null) {
      span.error(
          new BatchFailure(
              request.error() != null ? request.error() : "bulk request: " + errorType));
    }
    span.end(micros(request.ended()), TimeUnit.MICROSECONDS);
  }

  private Set<TraceContext> links(BatchTrace batch) {
    Set<String> seen = new LinkedHashSet<>();
    Set<TraceContext> links = new LinkedHashSet<>();
    for (String value : batch.traceParents()) {
      if (links.size() >= MAX_LINKS) {
        break;
      }
      TraceParent parent = TraceParent.parse(value);
      if (parent != null && seen.add(parent.traceId() + parent.spanId())) {
        links.add(
            tracer
                .traceContextBuilder()
                .traceId(parent.traceId())
                .spanId(parent.spanId())
                .sampled(parent.sampled())
                .build());
      }
    }
    return links;
  }

  private static long micros(Instant instant) {
    return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + instant.getNano() / 1_000;
  }

  /** The failure recorded on a span; no stack trace, names and statuses only. */
  static final class BatchFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;

    BatchFailure(String message) {
      super(message, null, false, false);
    }
  }
}
