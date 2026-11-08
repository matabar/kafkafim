package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.dlq.DlqListener;
import io.github.matabar.kafkafim.dlq.DlqRequest;
import io.github.matabar.kafkafim.sink.es.BatchReport;
import io.github.matabar.kafkafim.sink.es.FlushReason;
import io.github.matabar.kafkafim.sink.es.SinkKey;
import io.github.matabar.kafkafim.sink.es.SinkListener;
import io.github.matabar.kafkafim.telemetry.BatchTrace;
import io.github.matabar.kafkafim.telemetry.BatchTracer;
import io.github.matabar.kafkafim.telemetry.KafkafimMeters;
import io.github.matabar.kafkafim.telemetry.PipelineMeters;
import io.github.matabar.kafkafim.telemetry.TraceParent;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.jspecify.annotations.Nullable;

/**
 * Feeds the sink and DLQ events to the §10 meters and the batch spans (DESIGN §10). Lives in {@code
 * supervisor} because it is the one place that knows both the building blocks' listener interfaces
 * and the telemetry package (ADR-0003, ADR-0029: {@code telemetry} depends on no kafkafim package
 * but {@code config}), and because only the supervisor knows that a batch's handles are {@link
 * RecordProcessor.DocumentRef}s, whose records carry the {@code traceparent} headers that become
 * span links.
 *
 * <p>Events of pipelines that are not configured (any more) are dropped: their meters are {@link
 * PipelineMeters#NOOP}.
 */
final class TelemetryListener implements SinkListener, DlqListener {

  private final KafkafimMeters meters;
  private final BatchTracer tracer;

  TelemetryListener(KafkafimMeters meters, BatchTracer tracer) {
    this.meters = meters;
    this.tracer = tracer;
  }

  private PipelineMeters of(String pipeline) {
    return meters.pipeline(pipeline);
  }

  // ---- sink ----

  @Override
  public void onFlush(SinkKey key, FlushReason reason, int docs, long bytes) {
    of(key.pipeline()).flush(key.cluster(), reason.name());
  }

  @Override
  public void onBulkResponse(SinkKey key, int status, int docs, long bytes, Duration latency) {
    of(key.pipeline()).bulkResponse(key.cluster(), status, docs, bytes, latency);
  }

  @Override
  public void onRequestRetry(
      SinkKey key, int status, @Nullable Throwable error, int attempt, Duration delay) {
    PipelineMeters pipeline = of(key.pipeline());
    if (status == 0) {
      pipeline.bulkTransportError(key.cluster()); // no response, so no onBulkResponse
    }
    pipeline.retry(key.cluster(), "request");
  }

  @Override
  public void onAuthFailure(SinkKey key, int status, String reason) {
    of(key.pipeline()).authFailure(key.cluster(), key.identity());
  }

  @Override
  public void onItemRetry(SinkKey key, int status, String errorType, int attempts) {
    PipelineMeters pipeline = of(key.pipeline());
    pipeline.itemError(key.cluster(), errorType, true);
    pipeline.retry(key.cluster(), "item");
  }

  @Override
  public void onItemError(SinkKey key, int status, String errorType) {
    of(key.pipeline()).itemError(key.cluster(), errorType, false);
  }

  @Override
  public void onBatchSent(SinkKey key, BatchReport report) {
    PipelineMeters pipeline = of(key.pipeline());
    if (pipeline == PipelineMeters.NOOP) {
      return;
    }
    List<BatchTrace.BulkRequest> requests = new ArrayList<>(report.requests().size());
    for (BatchReport.Request request : report.requests()) {
      requests.add(
          new BatchTrace.BulkRequest(
              request.started(),
              request.ended(),
              request.docs(),
              request.bytes(),
              request.status(),
              request.error(),
              request.retries(),
              request.itemErrors()));
    }
    tracer.record(
        new BatchTrace(
            key.pipeline(),
            pipeline.kafkaCluster(),
            key.cluster(),
            key.identity(),
            report.reason().name().toLowerCase(Locale.ROOT),
            report.docs(),
            report.bytes(),
            report.sealed(),
            report.ended(),
            requests,
            traceParents(report.handles())));
  }

  /**
   * The distinct {@code traceparent} header values of the batch's source records, at most {@link
   * BatchTracer#MAX_LINKS}; the tracer validates them.
   */
  static List<String> traceParents(List<?> handles) {
    Set<String> values = new LinkedHashSet<>();
    for (Object handle : handles) {
      if (values.size() >= BatchTracer.MAX_LINKS) {
        break;
      }
      if (handle instanceof RecordProcessor.DocumentRef ref) {
        Header header = ref.record().record().headers().lastHeader(TraceParent.HEADER);
        if (header != null && header.value() != null) {
          values.add(new String(header.value(), StandardCharsets.UTF_8));
        }
      }
    }
    return List.copyOf(values);
  }

  // ---- DLQ ----

  @Override
  public void onWritten(String pipeline, DlqRequest request, RecordMetadata metadata) {
    of(pipeline).dlqRecord(request.errorType());
  }
}
