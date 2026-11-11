package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.decode.DecodeError;
import io.github.matabar.kafkafim.decode.DecodeResult;
import io.github.matabar.kafkafim.decode.DocumentOutcome;
import io.github.matabar.kafkafim.dlq.DlqClosedException;
import io.github.matabar.kafkafim.dlq.DlqRequest;
import io.github.matabar.kafkafim.route.HeaderLookup;
import io.github.matabar.kafkafim.route.IndexResolution;
import io.github.matabar.kafkafim.route.Route;
import io.github.matabar.kafkafim.route.Router;
import io.github.matabar.kafkafim.sink.es.ActionOutcome;
import io.github.matabar.kafkafim.sink.es.BulkAction;
import io.github.matabar.kafkafim.telemetry.DocOutcome;
import io.github.matabar.kafkafim.telemetry.PipelineMeters;
import io.github.matabar.kafkafim.telemetry.TelemetryNames;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The data path of one pipeline (DESIGN §4.2, §4.3): decodes each record, routes each document,
 * submits one {@link BulkAction} per routed copy and dead-letters every failure, and completes the
 * source record only when every one of its parts is done.
 *
 * <p><b>Parts.</b> A record is split into parts before anything is dispatched, and declares their
 * number with {@link InboundRecord#expectParts}: one part per routed copy of a decoded document
 * (fan-out in {@code route-mode: all} makes several), one per document that fails decoding or
 * routing ({@code missing_id}, {@code bad_timestamp}, {@code decode}, {@code no_route}, {@code
 * unresolved_index}), one per document a drop route selected, and exactly one for a record that
 * fails as a whole (oversize, undecodable). Each document is routed before its id, version and
 * timestamp problems apply ({@link DocumentOutcome.Unindexable}); such a problem is still one part
 * per document, whatever the fan-out, and a dropped document is never checked for one. An empty
 * array has zero parts and completes at once. A part completes when Elasticsearch acked the action
 * ({@code indexed} or {@code stale}), when the DLQ broker acknowledged its DLQ record (the sink's
 * {@code dlq} outcomes and every decode or route failure), or, for a dropped document, as soon as
 * it is dispatched: a record whose documents all drop completes at once. A DLQ send that fails at
 * shutdown leaves the part, and so the record, incomplete: it is re-delivered.
 *
 * <p>Thread-safety: {@link #process} runs on consumer threads, {@link #onOutcome} on sink sender
 * threads and DLQ completions on DLQ threads; the part counter lives in the source's record.
 */
final class RecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(RecordProcessor.class);

  /** {@code error.type} of a record or element that is not a decodable document. */
  static final String DECODE = "decode";

  private final CompiledPipeline pipeline;
  private final Function<RouteTarget.StreamKey, ActionSink> sinks;
  private final DeadLetters deadLetters;
  private final Clock clock;
  private final PipelineMeters meters;

  /**
   * Creates the processor.
   *
   * @param pipeline the compiled pipeline
   * @param sinks the bulk sink of each {@code (cluster, identity)} of the pipeline's routes
   * @param deadLetters the pipeline's DLQ writer
   * @param clock for first-failure times and records without a timestamp
   */
  RecordProcessor(
      CompiledPipeline pipeline,
      Function<RouteTarget.StreamKey, ActionSink> sinks,
      DeadLetters deadLetters,
      Clock clock) {
    this(pipeline, sinks, deadLetters, clock, PipelineMeters.NOOP);
  }

  /**
   * Creates the processor with meters: {@code kafkafim_docs_total} per route and outcome, and
   * {@code kafkafim_e2e_latency_seconds} per acknowledged document.
   */
  RecordProcessor(
      CompiledPipeline pipeline,
      Function<RouteTarget.StreamKey, ActionSink> sinks,
      DeadLetters deadLetters,
      Clock clock,
      PipelineMeters meters) {
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
    this.sinks = Objects.requireNonNull(sinks, "sinks");
    this.deadLetters = Objects.requireNonNull(deadLetters, "deadLetters");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.meters = Objects.requireNonNull(meters, "meters");
  }

  /** Where routed actions go: one bulk sink per {@code (cluster, identity)}. */
  interface ActionSink {

    /** Queues an action; never blocks. */
    void submit(BulkAction<DocumentRef> action);

    /** Seals the open batch. */
    void flush();
  }

  /** Where failures go: the pipeline's DLQ writer. */
  @FunctionalInterface
  interface DeadLetters {

    /**
     * Sends a DLQ record.
     *
     * @return completes after the broker acknowledged it; fails only at shutdown
     */
    CompletableFuture<?> send(DlqRequest request);
  }

  /**
   * The completion handle of one bulk action.
   *
   * @param record the source record
   * @param docIndex position in a JSON array record, or {@code null} for an object record
   * @param target the selected route
   */
  record DocumentRef(InboundRecord record, @Nullable Integer docIndex, RouteTarget target) {}

  /** Processes one poll's records, in order. */
  void process(List<? extends InboundRecord> records) {
    for (InboundRecord record : records) {
      process(record);
    }
  }

  /** Processes one record: plans its parts, declares them, then dispatches them. */
  void process(InboundRecord inbound) {
    if (inbound.isAbandoned()) {
      return; // its partition's owner re-reads it
    }
    List<Part> parts = plan(inbound);
    inbound.expectParts(parts.size());
    for (Part part : parts) {
      switch (part) {
        case IndexPart index ->
            sinks.apply(index.action().handle().target().stream()).submit(index.action());
        case DlqPart dlq -> deadLetter(inbound, dlq.request(), dlq.route());
        case DroppedPart dropped -> {
          meters.document(dropped.route(), DocOutcome.DROPPED);
          inbound.complete();
        }
      }
    }
  }

  private List<Part> plan(InboundRecord inbound) {
    ConsumerRecord<byte[], byte[]> record = inbound.record();
    if (inbound.oversize()) {
      long size = record.value() == null ? 0 : record.value().length;
      return List.of(
          recordFailure(
              inbound,
              DECODE,
              "record value is "
                  + size
                  + " bytes, larger than max-record-bytes "
                  + pipeline.decoder().spec().maxRecordBytes()));
    }
    Instant ingestTime = KafkaRecords.ingestTime(record, pipeline.timestampFieldSet(), clock);
    DecodeResult result = pipeline.decoder().decode(record.value(), ingestTime);
    return switch (result) {
      case DecodeResult.RecordFailure failure ->
          List.of(recordFailure(inbound, failure.error().type().wireName(), reason(failure)));
      case DecodeResult.Documents documents -> plan(inbound, documents);
    };
  }

  private List<Part> plan(InboundRecord inbound, DecodeResult.Documents documents) {
    ConsumerRecord<byte[], byte[]> record = inbound.record();
    HeaderLookup headers = KafkaRecords.headers(record.headers());
    Router<RouteTarget> router = pipeline.router();
    List<Part> parts = new ArrayList<>(documents.outcomes().size());
    for (DocumentOutcome outcome : documents.outcomes()) {
      Integer docIndex = documents.array() ? outcome.index() : null;
      switch (outcome) {
        case DocumentOutcome.Failed failed ->
            parts.add(documentFailure(inbound, failed.error(), docIndex));
        case DocumentOutcome.Routable routable -> {
          // Route first: whether an id, version or timestamp problem applies depends on where the
          // document goes (DESIGN §4.2).
          List<Route<RouteTarget>> routes = router.select(routable.node(), record.topic(), headers);
          if (routes.size() == 1 && routes.getFirst().isDrop()) {
            // Discarded (ADR-0037): no copy, no DLQ record, no id, version or timestamp check, no
            // source bytes, so nothing of it counts toward max-in-flight-bytes beyond the record.
            parts.add(new DroppedPart(routes.getFirst().target().label()));
            continue;
          }
          switch (routable) {
            case DocumentOutcome.Unindexable unindexable ->
                // The error applies because the document is to be indexed: an indexing route
                // selected it, or none did (an empty selection reports the error, not no_route).
                // It is one part per document (route "none"), not one per selected copy.
                parts.add(documentFailure(inbound, unindexable.error(), docIndex));
            case DocumentOutcome.Decoded doc -> addCopies(inbound, doc, routes, docIndex, parts);
          }
        }
      }
    }
    return parts;
  }

  /** One DLQ part for a document that failed on its own, before any route label applies. */
  private DlqPart documentFailure(
      InboundRecord inbound, DecodeError error, @Nullable Integer docIndex) {
    return dlq(inbound, error.type().wireName(), error.reason(), null, null, 1, now(), docIndex);
  }

  /** One part per selected route of a decoded document, or one {@code no_route} part. */
  private void addCopies(
      InboundRecord inbound,
      DocumentOutcome.Decoded doc,
      List<Route<RouteTarget>> routes,
      @Nullable Integer docIndex,
      List<Part> parts) {
    if (routes.isEmpty()) {
      parts.add(
          dlq(
              inbound,
              Router.NO_ROUTE,
              "no route matched and the pipeline has no default route",
              null,
              null,
              1,
              now(),
              docIndex));
      return;
    }
    byte[] source = null;
    for (Route<RouteTarget> route : routes) {
      RouteTarget target = route.target();
      IndexResolution resolution = route.index().resolve(doc.node(), doc.timestamp());
      switch (resolution) {
        case IndexResolution.Resolved resolved -> {
          if (source == null) {
            source = doc.source().toByteArray();
          }
          parts.add(
              new IndexPart(
                  new BulkAction<>(
                      resolved.index(),
                      doc.id().orElse(null),
                      pipeline.op(),
                      doc.version().isPresent() ? doc.version().getAsLong() : null,
                      target.ingestPipeline(),
                      source,
                      new DocumentRef(inbound, docIndex, target))));
        }
        case IndexResolution.Failure failure ->
            parts.add(
                dlq(
                    inbound,
                    failure.errorType(),
                    failure.reason(),
                    target.cluster(),
                    null,
                    1,
                    now(),
                    docIndex,
                    target.label()));
      }
    }
  }

  /**
   * The final outcome of one bulk action: an ack completes its part; a DLQ outcome completes it
   * once the DLQ record is acknowledged.
   */
  void onOutcome(BulkAction<DocumentRef> action, ActionOutcome outcome) {
    DocumentRef ref = action.handle();
    switch (outcome) {
      case ActionOutcome.Indexed indexed -> {
        acknowledged(ref, DocOutcome.INDEXED);
        ref.record().complete();
      }
      case ActionOutcome.Stale stale -> {
        acknowledged(ref, DocOutcome.STALE);
        ref.record().complete();
      }
      case ActionOutcome.Dlq dlq -> {
        if (ref.record().isAbandoned()) {
          return; // re-delivered to the partition's next owner, which dead-letters it again
        }
        deadLetter(
            ref.record(),
            request(
                ref.record(),
                dlq.errorType().wireName(),
                dlq.reason(),
                ref.target().cluster(),
                action.index(),
                Math.max(1, dlq.attempts()),
                dlq.firstFailure(),
                ref.docIndex()),
            ref.target().label());
      }
    }
  }

  /** Counts an Elasticsearch acknowledgement and its end-to-end latency (DESIGN §10). */
  private void acknowledged(DocumentRef ref, DocOutcome outcome) {
    meters.document(ref.target().label(), outcome);
    long timestamp = ref.record().record().timestamp();
    if (timestamp >= 0) {
      meters.endToEnd(Duration.ofMillis(clock.millis() - timestamp));
    }
  }

  private void deadLetter(InboundRecord inbound, DlqRequest request, String route) {
    CompletableFuture<?> sent;
    try {
      sent = deadLetters.send(request);
    } catch (RuntimeException e) {
      LOG.warn(
          "Pipeline '{}': DLQ send for {} refused; the record will be re-delivered: {}",
          pipeline.name(),
          request,
          e.toString());
      return;
    }
    sent.whenComplete((ignored, error) -> {
      if (error == null) {
        meters.document(route, DocOutcome.DLQ);
        inbound.complete();
        return;
      }
      Throwable cause =
          error instanceof CompletionException && error.getCause() != null
              ? error.getCause()
              : error;
      if (cause instanceof DlqClosedException) {
        LOG.debug("Pipeline '{}': {} not dead-lettered before shutdown", pipeline.name(), request);
      } else {
        LOG.warn(
            "Pipeline '{}': DLQ send for {} failed; the record will be re-delivered: {}",
            pipeline.name(),
            request,
            cause.toString());
      }
    });
  }

  private DlqPart recordFailure(InboundRecord inbound, String errorType, String reason) {
    return dlq(inbound, errorType, reason, null, null, 1, now(), null);
  }

  private DlqPart dlq(
      InboundRecord inbound,
      String errorType,
      @Nullable String reason,
      @Nullable String targetCluster,
      @Nullable String targetIndex,
      int attempts,
      Instant firstFailure,
      @Nullable Integer docIndex) {
    return dlq(
        inbound,
        errorType,
        reason,
        targetCluster,
        targetIndex,
        attempts,
        firstFailure,
        docIndex,
        TelemetryNames.NO_ROUTE);
  }

  private DlqPart dlq(
      InboundRecord inbound,
      String errorType,
      @Nullable String reason,
      @Nullable String targetCluster,
      @Nullable String targetIndex,
      int attempts,
      Instant firstFailure,
      @Nullable Integer docIndex,
      String route) {
    return new DlqPart(
        request(
            inbound,
            errorType,
            reason,
            targetCluster,
            targetIndex,
            attempts,
            firstFailure,
            docIndex),
        route);
  }

  private static DlqRequest request(
      InboundRecord inbound,
      String errorType,
      @Nullable String reason,
      @Nullable String targetCluster,
      @Nullable String targetIndex,
      int attempts,
      Instant firstFailure,
      @Nullable Integer docIndex) {
    ConsumerRecord<byte[], byte[]> record = inbound.record();
    return new DlqRequest(
        record.topic(),
        record.partition(),
        record.offset(),
        KafkaRecords.timestamp(record),
        record.key(),
        record.value(),
        Arrays.asList(record.headers().toArray()),
        errorType,
        reason,
        targetCluster,
        targetIndex,
        attempts,
        firstFailure,
        docIndex);
  }

  private static String reason(DecodeResult.RecordFailure failure) {
    return failure.error().reason();
  }

  private Instant now() {
    return clock.instant();
  }

  private sealed interface Part {}

  private record IndexPart(BulkAction<DocumentRef> action) implements Part {}

  /** A failure to dead-letter; {@code route} is the route label, or none before routing. */
  private record DlqPart(DlqRequest request, String route) implements Part {}

  /** A document a drop route selected; {@code route} is that route's label. */
  private record DroppedPart(String route) implements Part {}
}
