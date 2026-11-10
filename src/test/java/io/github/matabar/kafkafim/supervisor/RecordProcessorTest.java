package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.dlq.DlqClosedException;
import io.github.matabar.kafkafim.dlq.DlqListener;
import io.github.matabar.kafkafim.dlq.DlqProducers;
import io.github.matabar.kafkafim.dlq.DlqRecordFormat;
import io.github.matabar.kafkafim.dlq.DlqRequest;
import io.github.matabar.kafkafim.dlq.DlqWriter;
import io.github.matabar.kafkafim.sink.es.ActionOutcome;
import io.github.matabar.kafkafim.sink.es.BulkAction;
import io.github.matabar.kafkafim.sink.es.SinkErrorType;
import io.github.matabar.kafkafim.telemetry.KafkafimMeters;
import io.github.matabar.kafkafim.telemetry.PipelineMeters;
import io.github.matabar.kafkafim.telemetry.PipelineSpec;
import io.github.matabar.kafkafim.telemetry.SourceProbe;
import io.github.matabar.kafkafim.telemetry.TelemetryNames;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Record-completion accounting (DESIGN §4.3): a record completes only when every part (routed copy
 * or dead-lettered document) is done, whatever order the outcomes arrive in, and never through a
 * revoked partition or an unacknowledged DLQ send.
 */
class RecordProcessorTest {

  private static final Instant NOW = Instant.parse("2026-11-08T12:00:00Z");

  /** 2026-03-04T05:06:07Z, the Kafka timestamp of every test record. */
  private static final long RECORD_TS = Instant.parse("2026-03-04T05:06:07Z").toEpochMilli();

  private static final String FAN_OUT =
      """
      p:
        source: { cluster: k, topics: [t], max-record-bytes: 1KB }
        route-mode: all
        routes:
          - when: [ { field: /region, equals: eu } ]
            cluster: es
            index: "eu-%{+yyyy.MM.dd}"
          - when: [ { field: /copy, exists: true } ]
            cluster: es2
            index: "copy-%{tenant}"
            pipeline: enrich
      """;

  private static final RouteTarget.StreamKey ES = new RouteTarget.StreamKey("es", "writer");
  private static final RouteTarget.StreamKey ES2 = new RouteTarget.StreamKey("es2", null);

  private final Map<RouteTarget.StreamKey, FakeSink> sinks = new LinkedHashMap<>();
  private final List<Sent> dlq = new ArrayList<>();
  private RecordProcessor processor = processor(FAN_OUT);

  private record Sent(DlqRequest request, CompletableFuture<Object> future) {
    void ack() {
      future.complete(new Object());
    }
  }

  private static final class FakeSink implements RecordProcessor.ActionSink {
    final List<BulkAction<RecordProcessor.DocumentRef>> actions = new ArrayList<>();
    int flushes;

    @Override
    public void submit(BulkAction<RecordProcessor.DocumentRef> action) {
      actions.add(action);
    }

    @Override
    public void flush() {
      flushes++;
    }
  }

  /** Mirrors {@code SourceRecord}'s part counting and abandonment. */
  private static final class FakeRecord implements InboundRecord {
    private final ConsumerRecord<byte[], byte[]> record;
    private final boolean oversize;
    private Integer expected;
    private int completed;
    private boolean abandoned;

    FakeRecord(ConsumerRecord<byte[], byte[]> record, boolean oversize) {
      this.record = record;
      this.oversize = oversize;
    }

    @Override
    public ConsumerRecord<byte[], byte[]> record() {
      return record;
    }

    @Override
    public boolean oversize() {
      return oversize;
    }

    @Override
    public void expectParts(int parts) {
      if (expected != null || completed > 0) {
        throw new IllegalStateException("parts already declared");
      }
      expected = parts;
    }

    @Override
    public void complete() {
      if (!abandoned && !isComplete()) {
        completed++;
      }
    }

    @Override
    public boolean isAbandoned() {
      return abandoned;
    }

    boolean isComplete() {
      return expected == null ? completed >= 1 : completed >= expected;
    }

    void revoke() {
      if (!isComplete()) {
        abandoned = true;
      }
    }
  }

  private RecordProcessor processor(String pipelines) {
    return processor(pipelines, PipelineMeters.NOOP);
  }

  private RecordProcessor processor(String pipelines, PipelineMeters meters) {
    CompiledPipeline compiled = PipelineMapping.compile("p", TestConfigs.withPipelines(pipelines));
    sinks.clear();
    for (RouteTarget.StreamKey key : compiled.streams().keySet()) {
      sinks.put(key, new FakeSink());
    }
    return new RecordProcessor(
        compiled,
        sinks::get,
        request -> {
          Sent sent = new Sent(request, new CompletableFuture<>());
          dlq.add(sent);
          return sent.future();
        },
        Clock.fixed(NOW, ZoneOffset.UTC),
        meters);
  }

  private static FakeRecord record(String value) {
    return record(value, new RecordHeaders(), RECORD_TS, false);
  }

  private static FakeRecord record(
      String value, RecordHeaders headers, long timestamp, boolean oversize) {
    return new FakeRecord(
        new ConsumerRecord<>(
            "t",
            2,
            41,
            timestamp,
            TimestampType.CREATE_TIME,
            1,
            value == null ? -1 : value.length(),
            "k".getBytes(StandardCharsets.UTF_8),
            value == null ? null : value.getBytes(StandardCharsets.UTF_8),
            headers,
            Optional.empty()),
        oversize);
  }

  private List<BulkAction<RecordProcessor.DocumentRef>> actions(RouteTarget.StreamKey key) {
    return sinks.get(key).actions;
  }

  private void ack(BulkAction<RecordProcessor.DocumentRef> action) {
    processor.onOutcome(action, ActionOutcome.INDEXED);
  }

  @Test
  void documentsAreCountedPerRouteAndOutcomeWithTheirEndToEndLatency() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (KafkafimMeters meters = new KafkafimMeters(registry)) {
      meters.sync(
          Map.of(
              "p",
              new PipelineSpec(
                  "k", List.of("t"), List.of("0", "1"), Set.of("es", "es2"), SourceProbe.NONE)),
          Set.of("es", "es2"),
          cluster -> null);
      processor = processor(FAN_OUT, meters.pipeline("p"));

      FakeRecord both = record("{\"id\":\"a\",\"region\":\"eu\",\"copy\":1,\"tenant\":\"x\"}");
      FakeRecord broken = record("not json");
      processor.process(List.of(both, broken));
      processor.onOutcome(actions(ES).getFirst(), ActionOutcome.INDEXED);
      processor.onOutcome(
          actions(ES2).getFirst(),
          new ActionOutcome.Dlq(SinkErrorType.ES_ERROR, "{}", 400, 1, NOW));
      dlq.forEach(Sent::ack);

      assertThat(docs(registry, "0", "indexed")).isOne();
      assertThat(docs(registry, "1", "dlq")).isOne();
      assertThat(docs(registry, TelemetryNames.NO_ROUTE, "dlq")).isOne();
      assertThat(docs(registry, "1", "indexed")).isZero();
      Timer e2e = registry.get(TelemetryNames.E2E_LATENCY).tag("pipeline", "p").timer();
      assertThat(e2e.count()).isOne(); // only Elasticsearch acknowledgements
      assertThat(e2e.max(TimeUnit.MILLISECONDS))
          .isEqualTo((double) (NOW.toEpochMilli() - RECORD_TS));
      assertThat(both.isComplete()).isTrue();
      assertThat(broken.isComplete()).isTrue();
    }
  }

  private static double docs(SimpleMeterRegistry registry, String route, String outcome) {
    return registry
        .get(TelemetryNames.DOCS)
        .tags("pipeline", "p", "kafka_cluster", "k", "route", route, "outcome", outcome)
        .counter()
        .count();
  }

  @Test
  void objectRecordCompletesWhenItsActionIsAcked() {
    FakeRecord r = record("{\"id\":\"a1\",\"region\":\"eu\"}");
    processor.process(List.of(r));

    assertThat(actions(ES))
        .singleElement()
        .satisfies(a -> {
          assertThat(a.index()).isEqualTo("eu-2026.03.04"); // ingest time: the record timestamp
          assertThat(a.id()).isEqualTo("a1");
          assertThat(new String(a.source(), StandardCharsets.UTF_8))
              .isEqualTo("{\"id\":\"a1\",\"region\":\"eu\"}");
          assertThat(a.ingestPipeline()).isNull();
          assertThat(a.handle().docIndex()).isNull();
        });
    assertThat(r.isComplete()).isFalse();
    ack(actions(ES).get(0));
    assertThat(r.isComplete()).isTrue();
    assertThat(dlq).isEmpty();
  }

  @Test
  void arrayRecordCompletesOnlyWhenEveryDocumentIsAckedInAnyOrder() {
    FakeRecord r =
        record(
            "[{\"id\":1,\"region\":\"eu\"},"
                + "{\"id\":2,\"region\":\"eu\"},"
                + "{\"id\":3,\"region\":\"eu\"}]");
    processor.process(List.of(r));

    List<BulkAction<RecordProcessor.DocumentRef>> actions = actions(ES);
    assertThat(actions).extracting(BulkAction::id).containsExactly("1", "2", "3");
    assertThat(actions).extracting(a -> a.handle().docIndex()).containsExactly(0, 1, 2);
    ack(actions.get(2));
    ack(actions.get(0));
    assertThat(r.isComplete()).isFalse();
    ack(actions.get(1));
    assertThat(r.isComplete()).isTrue();
  }

  @Test
  void emptyArrayCompletesAtOnce() {
    FakeRecord r = record("[]");
    processor.process(List.of(r));
    assertThat(r.isComplete()).isTrue();
    assertThat(actions(ES)).isEmpty();
    assertThat(dlq).isEmpty();
  }

  @Test
  void fanOutCopiesAreSeparatePartsOnTheirStreams() {
    FakeRecord r = record("{\"id\":\"x\",\"region\":\"eu\",\"copy\":true,\"tenant\":\"ACME\"}");
    processor.process(List.of(r));

    assertThat(actions(ES))
        .singleElement()
        .extracting(BulkAction::index)
        .isEqualTo("eu-2026.03.04");
    assertThat(actions(ES2))
        .singleElement()
        .satisfies(a -> {
          assertThat(a.index()).isEqualTo("copy-acme");
          assertThat(a.ingestPipeline()).isEqualTo("enrich");
          assertThat(a.handle().target()).isEqualTo(new RouteTarget("es2", null, "enrich", "1"));
        });
    // Both copies share one byte array: one copy of the source per document.
    assertThat(actions(ES).get(0).source()).isSameAs(actions(ES2).get(0).source());
    ack(actions(ES2).get(0));
    assertThat(r.isComplete()).isFalse();
    ack(actions(ES).get(0));
    assertThat(r.isComplete()).isTrue();
  }

  @Test
  void mixedDecodeRouteAndIndexFailuresAreDeadLetteredWithDocIndexWhileSiblingsIndex() {
    FakeRecord r =
        record(
            "["
                + "{\"id\":\"ok\",\"region\":\"eu\"}," // 0: indexed
                + "42," // 1: decode
                + "{\"region\":\"eu\"}," // 2: missing_id
                + "{\"id\":\"lost\"}," // 3: no_route (no route matches, no default)
                + "{\"id\":\"half\",\"region\":\"eu\",\"copy\":1}" // 4: eu indexed, copy unresolved
                + "]");
    processor.process(List.of(r));

    assertThat(actions(ES)).extracting(BulkAction::id).containsExactly("ok", "half");
    assertThat(actions(ES2)).isEmpty();
    assertThat(dlq)
        .extracting(s -> s.request().errorType(), s -> s.request().docIndex())
        .containsExactly(
            org.assertj.core.groups.Tuple.tuple("decode", 1),
            org.assertj.core.groups.Tuple.tuple("missing_id", 2),
            org.assertj.core.groups.Tuple.tuple("no_route", 3),
            org.assertj.core.groups.Tuple.tuple("unresolved_index", 4));
    DlqRequest unresolved = dlq.get(3).request();
    assertThat(unresolved.targetCluster()).isEqualTo("es2");
    assertThat(unresolved.targetIndex()).isNull();
    assertThat(unresolved.reason()).contains("tenant").contains("missing");
    assertThat(dlq.get(2).request().targetCluster()).isNull();

    // 6 parts: 2 actions + 4 DLQ records.
    actions(ES).forEach(this::ack);
    dlq.get(0).ack();
    dlq.get(1).ack();
    dlq.get(2).ack();
    assertThat(r.isComplete()).isFalse();
    dlq.get(3).ack();
    assertThat(r.isComplete()).isTrue();
  }

  /**
   * Routing comes before the id, version and timestamp checks, but while every route indexes an
   * extraction problem is still one DLQ part per document, with the same {@code error.type}, no
   * target and the {@code none} route label, however many routes the document matched.
   */
  @ParameterizedTest(name = "route-mode: {0}")
  @ValueSource(strings = {"first", "all"})
  void extractionErrorsAreOneDeadLetterPerDocumentWhateverTheRoutes(String mode) {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (KafkafimMeters meters = new KafkafimMeters(registry, Duration.ofHours(1))) {
      meters.sync(
          Map.of(
              "p",
              new PipelineSpec(
                  "k",
                  List.of("t"),
                  List.of("0", "1", "2"),
                  Set.of("es", "es2"),
                  SourceProbe.NONE)),
          Set.of("es", "es2"),
          cluster -> null);
      processor =
          processor(
              """
              p:
                source: { cluster: k, topics: [t], max-record-bytes: 1KB }
                route-mode: %s
                document: { timestamp-field: /ts, version-field: /v }
                routes:
                  - when: [ { field: /region, equals: eu } ]
                    cluster: es
                    index: "eu-%%{+yyyy.MM.dd}"
                  - when: [ { field: /copy, exists: true } ]
                    cluster: es2
                    index: "copy-%%{tenant}"
                  - { default: true, cluster: es, index: other }
              """
                  .formatted(mode),
              meters.pipeline("p"));

      // Every element matches both conditional routes (two copies in `all`) and has a problem,
      // except 0, which is indexed so the record has a sibling to wait for.
      String both = "\"region\":\"eu\",\"copy\":1,\"tenant\":\"x\"";
      FakeRecord r =
          record(
              "["
                  + "{\"id\":\"ok\",\"v\":1,\"ts\":\"2026-01-02T03:04:05Z\","
                  + both
                  + "}," // 0: indexed
                  + "{\"v\":1,\"ts\":\"2026-01-02T03:04:05Z\","
                  + both
                  + "}," // 1: missing_id
                  + "{\"id\":\"b\",\"ts\":\"2026-01-02T03:04:05Z\","
                  + both
                  + "}," // 2: decode (no version)
                  + "{\"id\":\"c\",\"v\":1,\"ts\":\"yesterday\","
                  + both
                  + "}," // 3: bad_timestamp
                  + "{\"v\":1,\"ts\":\"2026-01-02T03:04:05Z\"}" // 4: missing_id, default route
                  + "]");
      processor.process(List.of(r));

      int copies = mode.equals("all") ? 2 : 1;
      assertThat(r.expected).isEqualTo(copies + 4);
      assertThat(actions(ES)).extracting(BulkAction::id).containsExactly("ok");
      assertThat(actions(ES2)).hasSize(copies - 1);
      assertThat(dlq)
          .extracting(
              s -> s.request().errorType(),
              s -> s.request().docIndex(),
              s -> s.request().targetCluster())
          .containsExactly(
              org.assertj.core.groups.Tuple.tuple("missing_id", 1, null),
              org.assertj.core.groups.Tuple.tuple("decode", 2, null),
              org.assertj.core.groups.Tuple.tuple("bad_timestamp", 3, null),
              org.assertj.core.groups.Tuple.tuple("missing_id", 4, null));
      assertThat(dlq.get(1).request().reason()).isEqualTo("version at /v is missing");

      dlq.forEach(Sent::ack);
      assertThat(r.isComplete()).isFalse();
      actions(ES).forEach(this::ack);
      actions(ES2).forEach(this::ack);
      assertThat(r.isComplete()).isTrue();
      assertThat(docs(registry, TelemetryNames.NO_ROUTE, "dlq")).isEqualTo(4);
      assertThat(docs(registry, "0", "dlq")).isZero();
      assertThat(docs(registry, "1", "dlq")).isZero();
      assertThat(docs(registry, "2", "dlq")).isZero();
    }
  }

  @Test
  void extractionErrorWinsOverNoRoute() {
    FakeRecord r = record("[{\"region\":\"us\"},{\"id\":\"lost\"}]");
    processor.process(List.of(r));

    assertThat(r.expected).isEqualTo(2);
    assertThat(dlq)
        .extracting(s -> s.request().errorType(), s -> s.request().docIndex())
        .containsExactly(
            org.assertj.core.groups.Tuple.tuple("missing_id", 0),
            org.assertj.core.groups.Tuple.tuple("no_route", 1));
  }

  @Test
  void deadLetterRequestCarriesTheOriginalRecord() {
    RecordHeaders headers = new RecordHeaders();
    headers.add(new RecordHeader("trace", "t1".getBytes(StandardCharsets.UTF_8)));
    FakeRecord r = record("{\"id\":\"a\"}", headers, RECORD_TS, false);
    processor.process(List.of(r));

    DlqRequest request = dlq.get(0).request();
    assertThat(request.errorType()).isEqualTo("no_route");
    assertThat(request.sourceTopic()).isEqualTo("t");
    assertThat(request.sourcePartition()).isEqualTo(2);
    assertThat(request.sourceOffset()).isEqualTo(41);
    assertThat(request.sourceTimestamp()).isEqualTo(Instant.ofEpochMilli(RECORD_TS));
    assertThat(request.key()).isEqualTo("k".getBytes(StandardCharsets.UTF_8));
    assertThat(request.value()).isEqualTo("{\"id\":\"a\"}".getBytes(StandardCharsets.UTF_8));
    assertThat(request.headers()).extracting(h -> h.key()).containsExactly("trace");
    assertThat(request.attempts()).isEqualTo(1);
    assertThat(request.firstFailure()).isEqualTo(NOW);
    assertThat(request.docIndex()).isNull(); // an object record has no doc-index
  }

  @Test
  void undecodableRecordIsOneDeadLetterWithoutDocIndex() {
    FakeRecord r = record("{not json");
    processor.process(List.of(r));
    assertThat(dlq)
        .singleElement()
        .satisfies(s -> {
          assertThat(s.request().errorType()).isEqualTo("decode");
          assertThat(s.request().docIndex()).isNull();
        });
    assertThat(r.isComplete()).isFalse();
    dlq.get(0).ack();
    assertThat(r.isComplete()).isTrue();
  }

  @Test
  void tombstoneIsDeadLettered() {
    FakeRecord r = record(null);
    processor.process(List.of(r));
    assertThat(dlq)
        .singleElement()
        .satisfies(s -> {
          assertThat(s.request().errorType()).isEqualTo("decode");
          assertThat(s.request().value()).isNull();
        });
  }

  @Test
  void oversizeRecordIsDeadLetteredWithoutDecoding() {
    // Valid JSON, but flagged oversize by the source: it must not be decoded or indexed.
    FakeRecord r =
        record("{\"id\":\"big\",\"region\":\"eu\"}", new RecordHeaders(), RECORD_TS, true);
    processor.process(List.of(r));
    assertThat(actions(ES)).isEmpty();
    assertThat(dlq)
        .singleElement()
        .satisfies(s -> {
          assertThat(s.request().errorType()).isEqualTo("decode");
          assertThat(s.request().reason()).contains("max-record-bytes 1024");
        });
    dlq.get(0).ack();
    assertThat(r.isComplete()).isTrue();
  }

  @Test
  void sinkDeadLetterCompletesOnlyAfterTheDlqAck() {
    FakeRecord r = record("[{\"id\":1,\"region\":\"eu\"},{\"id\":2,\"region\":\"eu\"}]");
    processor.process(List.of(r));
    BulkAction<RecordProcessor.DocumentRef> second = actions(ES).get(1);
    Instant first = Instant.parse("2026-11-08T11:00:00Z");
    processor.onOutcome(
        second,
        new ActionOutcome.Dlq(SinkErrorType.ES_ERROR, "{\"type\":\"mapper\"}", 400, 2, first));
    ack(actions(ES).get(0));

    assertThat(dlq)
        .singleElement()
        .satisfies(s -> {
          DlqRequest req = s.request();
          assertThat(req.errorType()).isEqualTo("es_error");
          assertThat(req.reason()).isEqualTo("{\"type\":\"mapper\"}");
          assertThat(req.targetCluster()).isEqualTo("es");
          assertThat(req.targetIndex()).isEqualTo("eu-2026.03.04");
          assertThat(req.attempts()).isEqualTo(2);
          assertThat(req.firstFailure()).isEqualTo(first);
          assertThat(req.docIndex()).isEqualTo(1);
        });
    assertThat(r.isComplete()).isFalse();
    dlq.get(0).ack();
    assertThat(r.isComplete()).isTrue();
  }

  @Test
  void staleCountsAsDone() {
    FakeRecord r = record("{\"id\":1,\"region\":\"eu\"}");
    processor.process(List.of(r));
    processor.onOutcome(actions(ES).get(0), new ActionOutcome.Stale("{}"));
    assertThat(r.isComplete()).isTrue();
  }

  @Test
  void tooLargeWithZeroAttemptsStillBuildsValidRequest() {
    FakeRecord r = record("{\"id\":1,\"region\":\"eu\"}");
    processor.process(List.of(r));
    processor.onOutcome(
        actions(ES).get(0), new ActionOutcome.Dlq(SinkErrorType.TOO_LARGE, "413", 413, 0, NOW));
    assertThat(dlq)
        .singleElement()
        .satisfies(s -> {
          assertThat(s.request().errorType()).isEqualTo("too_large");
          assertThat(s.request().attempts()).isEqualTo(1);
        });
  }

  @Test
  void dlqClosedAtShutdownLeavesTheRecordIncomplete() {
    FakeRecord r = record("{\"id\":\"a\"}");
    processor.process(List.of(r));
    dlq.get(0).future().completeExceptionally(dlqClosed(dlq.get(0).request()));
    assertThat(r.isComplete()).isFalse();
  }

  /** The exception a real DLQ writer fails a send with once the producers are closed. */
  private static Throwable dlqClosed(DlqRequest request) {
    KafkafimProperties local =
        TestConfigs.bind(
            """
            kafkafim:
              kafka:
                clusters:
                  k: { bootstrap-servers: ["127.0.0.1:9"] }
              pipelines:
                p:
                  source: { cluster: k, topics: [t] }
                  routes: [ { default: true, cluster: es, index: i } ]
            """);
    DlqProducers producers = new DlqProducers(local, DlqListener.NONE);
    DlqWriter writer = producers.writer("p");
    producers.close();
    Throwable closed = writer.send(request).exceptionNow();
    assertThat(closed).isInstanceOf(DlqClosedException.class);
    return closed;
  }

  @Test
  void revokedPartitionIgnoresLateOutcomesAndSkipsDeadLetters() {
    FakeRecord r = record("[{\"id\":1,\"region\":\"eu\"},{\"id\":2,\"region\":\"eu\"}]");
    processor.process(List.of(r));
    ack(actions(ES).get(0));
    r.revoke();
    processor.onOutcome(
        actions(ES).get(1), new ActionOutcome.Dlq(SinkErrorType.ES_ERROR, "x", 400, 1, NOW));
    assertThat(dlq).isEmpty(); // the next owner re-reads and dead-letters it
    ack(actions(ES).get(1));
    assertThat(r.isComplete()).isFalse();
    assertThat(r.isAbandoned()).isTrue();
  }

  @Test
  void recordAbandonedOnArrivalIsSkipped() {
    FakeRecord r = record("{\"id\":1,\"region\":\"eu\"}");
    r.revoke();
    processor.process(List.of(r));
    assertThat(actions(ES)).isEmpty();
    assertThat(dlq).isEmpty();
  }

  @Test
  void refusedDlqSendLeavesTheRecordIncomplete() {
    CompiledPipeline compiled = PipelineMapping.compile("p", TestConfigs.withPipelines(FAN_OUT));
    RecordProcessor refusing =
        new RecordProcessor(
            compiled,
            k -> new FakeSink(),
            request -> {
              throw new IllegalStateException("DLQ producers are closed");
            },
            Clock.systemUTC());
    FakeRecord r = record("{\"id\":\"a\"}");
    refusing.process(List.of(r));
    assertThat(r.isComplete()).isFalse();
  }

  @Test
  void replayedDlqRecordUsesTheOriginalTimestampWithoutTimestampField() {
    RecordHeaders headers = new RecordHeaders();
    headers.add(
        new RecordHeader(
            DlqRecordFormat.SOURCE_TIMESTAMP,
            "2025-12-31T23:59:59.999Z".getBytes(StandardCharsets.UTF_8)));
    processor.process(List.of(record("{\"id\":1,\"region\":\"eu\"}", headers, RECORD_TS, false)));
    assertThat(actions(ES))
        .singleElement()
        .extracting(BulkAction::index)
        .isEqualTo("eu-2025.12.31");
  }

  @Test
  void timestampFieldWinsOverTheDlqHeader() {
    processor =
        processor(
            """
            p:
              source: { cluster: k, topics: [t] }
              document: { timestamp-field: /ts }
              routes: [ { default: true, cluster: es, index: "d-%{+yyyy.MM.dd}" } ]
            """);
    RecordHeaders headers = new RecordHeaders();
    headers.add(
        new RecordHeader(
            DlqRecordFormat.SOURCE_TIMESTAMP,
            "2025-12-31T23:59:59Z".getBytes(StandardCharsets.UTF_8)));
    processor.process(
        List.of(record("{\"id\":1,\"ts\":\"2024-02-29T10:00:00Z\"}", headers, RECORD_TS, false)));
    assertThat(actions(ES)).singleElement().extracting(BulkAction::index).isEqualTo("d-2024.02.29");
  }
}
