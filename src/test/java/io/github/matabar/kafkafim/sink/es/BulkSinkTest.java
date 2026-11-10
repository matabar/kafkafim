package io.github.matabar.kafkafim.sink.es;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.matabar.kafkafim.config.Pipeline.OnVersionConflict;
import io.github.matabar.kafkafim.config.Pipeline.Op;
import io.github.matabar.kafkafim.sink.es.BulkTransport.Result;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.io.IOException;
import java.net.ConnectException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class BulkSinkTest {

  static final Backoff FAST = new Backoff(Duration.ofMillis(1), Duration.ofMillis(5), 2.0, 0.0);
  static final Duration HOUR = Duration.ofHours(1);
  static final Duration WAIT = Duration.ofSeconds(10);

  final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
  final Outcomes outcomes = new Outcomes();
  final Events events = new Events();
  final List<BulkSink<String>> sinks = new ArrayList<>();
  CircuitBreaker breaker =
      CircuitBreaker.of(
          "c", BreakerSettings.DEFAULT.toConfig(ClientSettings.DEFAULT.responseTimeout()));

  @AfterEach
  void tearDown() {
    sinks.forEach(sink -> sink.close(Duration.ZERO));
    timer.shutdownNow();
    outcomes.assertAtMostOnce();
  }

  static SinkSettings settings(int maxDocs, long maxBytes, Duration maxAge, int inFlight) {
    return new SinkSettings(
        maxDocs,
        maxBytes,
        maxAge,
        inFlight,
        10,
        Duration.ofMinutes(10),
        OnVersionConflict.IGNORE,
        FAST);
  }

  static SinkSettings poison(int maxAttempts, Duration maxElapsed) {
    return retrying(100, Duration.ofMillis(5), maxAttempts, maxElapsed);
  }

  static SinkSettings retrying(int maxDocs, Duration maxAge, int maxAttempts, Duration maxElapsed) {
    return new SinkSettings(
        maxDocs, 1 << 20, maxAge, 4, maxAttempts, maxElapsed, OnVersionConflict.IGNORE, FAST);
  }

  BulkSink<String> sink(SinkSettings settings, BulkTransport transport) {
    BulkSink<String> sink =
        new BulkSink<>(
            new SinkKey("orders", "search-eu", "writer"),
            settings,
            transport,
            breaker,
            Duration.ofMillis(5),
            timer,
            outcomes,
            events,
            Clock.systemUTC(),
            closed -> {});
    sinks.add(sink);
    return sink;
  }

  static BulkAction<String> doc(String id) {
    return doc(id, Op.INDEX, null);
  }

  static BulkAction<String> doc(String id, Op op, @Nullable Long version) {
    return new BulkAction<>("orders", id, op, version, null, "{\"n\":1}".getBytes(UTF_8), id);
  }

  static long sizeOf(BulkAction<?> action) {
    return BulkBody.size(BulkBody.metadataLine(action), action.source());
  }

  static void submitAll(BulkSink<String> sink, String... ids) {
    for (String id : ids) {
      sink.submit(doc(id));
    }
  }

  /**
   * Submits {@code ids} while the timer's one thread is held, so a short {@code max-age} cannot
   * seal part of them however long this thread stalls between submits. The age seal and retries run
   * on that timer as usual once the last action is in.
   */
  void submitWithTimerHeld(BulkSink<String> sink, String... ids) throws InterruptedException {
    CountDownLatch held = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    timer.execute(() -> {
      held.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    held.await();
    try {
      submitAll(sink, ids);
    } finally {
      release.countDown();
    }
  }

  static String[] ids(int count) {
    return IntStream.range(0, count).mapToObj(i -> "d" + i).toArray(String[]::new);
  }

  // ---- batching ----

  @Test
  void flushesOnMaxDocs() throws Exception {
    FakeTransport transport = new FakeTransport((request, call) -> FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(3, 1 << 20, HOUR, 1), transport);

    submitAll(sink, ids(7));
    await().until(() -> outcomes.count() == 6);
    assertThat(sink.pendingActions()).isOne();
    sink.flush();

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(transport.requests()).extracting(r -> r.actions().size()).containsExactly(3, 3, 1);
    assertThat(events.flushes)
        .containsExactly(FlushReason.DOCS, FlushReason.DOCS, FlushReason.EXPLICIT);
    assertThat(outcomes.all()).allMatch(o -> o instanceof ActionOutcome.Indexed);
    assertThat(sink.pendingBytes()).isZero();
  }

  @Test
  void flushesOnMaxBytesAndNeverExceedsThem() throws Exception {
    long size = sizeOf(doc("d0"));
    FakeTransport transport = new FakeTransport((request, call) -> FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(100, 2 * size + size / 2, HOUR, 1), transport);

    submitAll(sink, ids(5));
    sink.flush();

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(transport.requests()).extracting(r -> r.actions().size()).containsExactly(2, 2, 1);
    assertThat(transport.requests()).allMatch(r -> r.bodyBytes() <= 2 * size + size / 2);
    assertThat(events.flushes).startsWith(FlushReason.BYTES, FlushReason.BYTES);
  }

  @Test
  void bodySizeAccountingMatchesTheSentBody() throws Exception {
    long size = sizeOf(doc("d0"));
    FakeTransport transport = new FakeTransport((request, call) -> FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(100, 3 * size, HOUR, 4), transport);

    submitAll(sink, ids(3));

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(transport.requests())
        .singleElement()
        .extracting(r -> r.bodyBytes())
        .isEqualTo((int) (3 * size));
    assertThat(events.flushBytes).containsExactly(3 * size);
  }

  @Test
  void actionLargerThanMaxBytesIsSentAlone() throws Exception {
    FakeTransport transport = new FakeTransport((request, call) -> FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(100, 10, HOUR, 1), transport);

    submitAll(sink, "a", "b");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(transport.requests())
        .extracting(FakeTransport.Request::ids)
        .containsExactly(List.of("a"), List.of("b"));
  }

  @Test
  void flushesOnMaxAge() throws Exception {
    FakeTransport transport = new FakeTransport((request, call) -> FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(100, 1 << 20, Duration.ofMillis(50), 4), transport);

    long start = System.nanoTime();
    submitWithTimerHeld(sink, "a", "b");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(Duration.ofNanos(System.nanoTime() - start))
        .isGreaterThanOrEqualTo(Duration.ofMillis(50));
    assertThat(transport.requests())
        .extracting(FakeTransport.Request::ids)
        .containsExactly(List.of("a", "b"));
    assertThat(events.flushes).containsExactly(FlushReason.AGE);
  }

  @Test
  void inFlightRequestsAreBounded() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger started = new AtomicInteger();
    FakeTransport transport =
        new FakeTransport((request, call) -> {
          started.incrementAndGet();
          block(release);
          return FakeTransport.allOk();
        });
    BulkSink<String> sink = sink(settings(1, 1 << 20, HOUR, 2), transport);

    submitAll(sink, ids(6));
    await().until(() -> started.get() == 2);
    Thread.sleep(100);
    assertThat(started).hasValue(2);
    assertThat(sink.pendingActions()).isEqualTo(6);
    release.countDown();

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(transport.maxConcurrent()).isEqualTo(2);
    assertThat(outcomes.count()).isEqualTo(6);
  }

  // ---- item outcomes ----

  @Test
  void mappingErrorGoesToDlqAtOnceWhileOthersAck() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            FakeTransport.items(
                request,
                sent ->
                    sent.id().equals("bad")
                        ? new String[] {"400", "mapper_parsing_exception"}
                        : new String[] {"201"}));
    BulkSink<String> sink = sink(settings(3, 1 << 20, HOUR, 4), transport);

    submitAll(sink, "a", "bad", "c");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(transport.requests()).hasSize(1);
    assertThat(outcomes.get("a")).isEqualTo(ActionOutcome.INDEXED);
    assertThat(outcomes.get("c")).isEqualTo(ActionOutcome.INDEXED);
    assertThat(outcomes.get("bad"))
        .isInstanceOfSatisfying(
            ActionOutcome.Dlq.class,
            dlq -> {
              assertThat(dlq.errorType()).isEqualTo(SinkErrorType.ES_ERROR);
              assertThat(dlq.status()).isEqualTo(400);
              assertThat(dlq.attempts()).isOne();
              assertThat(dlq.reason()).contains("mapper_parsing_exception");
            });
  }

  @Test
  void failedItemsAreLoggedOnceWithoutTheirReason() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(BulkSink.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      FakeTransport transport =
          new FakeTransport((request, call) ->
              FakeTransport.items(
                  request,
                  sent ->
                      sent.id().equals("ok")
                          ? new String[] {"201"}
                          : new String[] {"400", "document_parsing_exception"}));
      BulkSink<String> sink = sink(settings(3, 1 << 20, HOUR, 1), transport);
      // The line is written before the response's outcomes, so a caller that sees them done
      // (here: awaitIdle) finds it in the log.
      List<Long> linesAtOutcome = new CopyOnWriteArrayList<>();
      outcomes.beforeEach = handle -> linesAtOutcome.add(failureLines(appender).count());

      submitAll(sink, "ok", "bad1", "bad2");

      assertThat(sink.awaitIdle(WAIT)).isTrue();
      assertThat(linesAtOutcome).containsExactly(1L, 1L, 1L);
      List<String> lines = failureLines(appender).toList();
      // Elasticsearch's reason quotes field values of the document: never logged (DESIGN §10).
      assertThat(lines)
          .singleElement()
          .asString()
          .contains("2 of 3 items failed")
          .contains("#1 400 document_parsing_exception; #2 400 document_parsing_exception")
          .doesNotContain("scripted");
    } finally {
      logger.detachAppender(appender);
    }
  }

  private static Stream<String> failureLines(ListAppender<ILoggingEvent> appender) {
    return logged(appender).stream()
        .map(ILoggingEvent::getFormattedMessage)
        .filter(m -> m.contains("items failed"));
  }

  /** A copy of the captured events, safe while sender threads still log. */
  static List<ILoggingEvent> logged(ListAppender<ILoggingEvent> appender) {
    synchronized (appender) { // AppenderBase.doAppend holds this lock while it adds
      return List.copyOf(appender.list);
    }
  }

  @Test
  void versionConflictIsStaleUnderIgnoreAndDlqUnderDlq() throws Exception {
    FakeTransport.Handler conflict =
        (request, call) ->
            FakeTransport.items(
                request, sent -> new String[] {"409", "version_conflict_engine_exception"});
    BulkSink<String> ignoring = sink(settings(1, 1 << 20, HOUR, 1), new FakeTransport(conflict));
    ignoring.submit(doc("v-ignore", Op.INDEX, 3L));
    ignoring.submit(doc("create-dup", Op.CREATE, null));
    assertThat(ignoring.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.get("v-ignore")).isInstanceOf(ActionOutcome.Stale.class);
    assertThat(outcomes.get("create-dup")).isEqualTo(ActionOutcome.INDEXED);

    SinkSettings dlqPolicy =
        new SinkSettings(1, 1 << 20, HOUR, 1, 10, HOUR, OnVersionConflict.DLQ, FAST);
    BulkSink<String> strict = sink(dlqPolicy, new FakeTransport(conflict));
    strict.submit(doc("v-dlq", Op.INDEX, 3L));
    assertThat(strict.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.get("v-dlq"))
        .isInstanceOfSatisfying(
            ActionOutcome.Dlq.class,
            dlq -> assertThat(dlq.errorType()).isEqualTo(SinkErrorType.VERSION_CONFLICT));
    // Only the dead-lettered conflict is a non-retryable item error; stale and create-dup are not.
    assertThat(events.itemErrors).containsExactly("409 version_conflict_engine_exception");
  }

  @Test
  void retryableItemErrorIsRetriedThenSucceeds() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            FakeTransport.items(
                request,
                sent ->
                    call <= 2 && sent.id().equals("busy")
                        ? new String[] {"429", "es_rejected_execution_exception"}
                        : new String[] {"201"}));
    // max-docs seals the first batch; the short max-age seals each lone retry of "busy".
    BulkSink<String> sink = sink(retrying(2, Duration.ofMillis(50), 10, HOUR), transport);

    submitWithTimerHeld(sink, "ok", "busy");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.all()).containsOnly(ActionOutcome.INDEXED);
    assertThat(transport.requests())
        .extracting(FakeTransport.Request::ids)
        .containsExactly(List.of("ok", "busy"), List.of("busy"), List.of("busy"));
    assertThat(events.itemRetries).hasValue(2);
  }

  @Test
  void poisonBoundByMaxAttempts() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            FakeTransport.items(
                request,
                sent ->
                    sent.id().equals("poison")
                        ? new String[] {"429", "es_rejected_execution_exception"}
                        : new String[] {"201"}));
    BulkSink<String> sink = sink(poison(3, HOUR), transport);

    submitAll(sink, "ok", "poison");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.get("ok")).isEqualTo(ActionOutcome.INDEXED);
    assertThat(outcomes.get("poison"))
        .isInstanceOfSatisfying(
            ActionOutcome.Dlq.class,
            dlq -> {
              assertThat(dlq.errorType()).isEqualTo(SinkErrorType.RETRIES_EXHAUSTED);
              assertThat(dlq.attempts()).isEqualTo(3);
              assertThat(dlq.status()).isEqualTo(429);
              assertThat(dlq.reason()).contains("es_rejected_execution_exception");
            });
    assertThat(transport.requests().stream().filter(r -> r.ids().contains("poison"))).hasSize(3);
  }

  @Test
  void poisonBoundByMaxElapsed() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            FakeTransport.items(
                request, sent -> new String[] {"503", "unavailable_shards_exception"}));
    BulkSink<String> sink = sink(poison(1_000_000, Duration.ofMillis(100)), transport);

    long start = System.nanoTime();
    submitAll(sink, "poison");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(Duration.ofNanos(System.nanoTime() - start))
        .isGreaterThanOrEqualTo(Duration.ofMillis(100));
    assertThat(outcomes.get("poison"))
        .isInstanceOfSatisfying(
            ActionOutcome.Dlq.class,
            dlq -> {
              assertThat(dlq.errorType()).isEqualTo(SinkErrorType.RETRIES_EXHAUSTED);
              assertThat(dlq.attempts()).isGreaterThan(1);
            });
  }

  @Test
  void itemAuthFailureIsRetriedWithoutCountingAndNeverDlq() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            FakeTransport.items(
                request,
                sent ->
                    call <= 4 ? new String[] {"403", "security_exception"} : new String[] {"201"}));
    BulkSink<String> sink = sink(poison(1, HOUR), transport);

    submitAll(sink, "a");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.get("a")).isEqualTo(ActionOutcome.INDEXED);
    assertThat(events.authFailures).hasValue(4);
    assertThat(events.lastAuthReason).startsWith("type=security_exception");
  }

  @Test
  void itemAuthFailureIsReportedBeforeTheResponsesOutcomes() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            FakeTransport.items(
                request,
                sent ->
                    sent.id().equals("denied") && call == 1
                        ? new String[] {"403", "security_exception"}
                        : new String[] {"201"}));
    // max-docs seals the first batch; the short max-age seals the lone retry of "denied".
    BulkSink<String> sink = sink(retrying(2, Duration.ofMillis(5), 10, HOUR), transport);
    Map<String, Integer> authFailuresAtOutcome = new ConcurrentHashMap<>();
    outcomes.beforeEach = handle -> authFailuresAtOutcome.put(handle, events.authFailures.get());

    submitWithTimerHeld(sink, "ok", "denied");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(transport.requests())
        .extracting(FakeTransport.Request::ids)
        .as("one response carries both")
        .containsExactly(List.of("ok", "denied"), List.of("denied"));
    assertThat(authFailuresAtOutcome).containsOnly(Map.entry("ok", 1), Map.entry("denied", 1));
    assertThat(outcomes.get("denied")).isEqualTo(ActionOutcome.INDEXED);
  }

  @Test
  void persistentItemAuthFailureNeverReportsRecoveryUntilItClears() throws Exception {
    AtomicInteger forbiddenCalls = new AtomicInteger(Integer.MAX_VALUE);
    FakeTransport transport =
        new FakeTransport((request, call) ->
            FakeTransport.items(
                request,
                sent ->
                    sent.id().equals("denied") && call <= forbiddenCalls.get()
                        ? new String[] {"403", "security_exception"}
                        : new String[] {"201"}));
    BulkSink<String> sink = sink(retrying(2, Duration.ofMillis(5), 1, HOUR), transport);

    submitAll(sink, "ok", "denied");
    await().until(() -> transport.requests().size() >= 10);
    assertThat(events.authRecovered).hasValue(0);
    assertThat(sink.authFailing()).isTrue();
    assertThat(events.authFailures.get())
        .as("one event per response, not per item")
        .isLessThanOrEqualTo(transport.requests().size());
    forbiddenCalls.set(0);

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.get("denied")).isEqualTo(ActionOutcome.INDEXED);
    await().until(() -> events.authRecovered.get() == 1);
    Thread.sleep(50);
    assertThat(events.authRecovered).hasValue(1);
    assertThat(sink.authFailing()).isFalse();
  }

  @Test
  void throwingListenerLosesNoAction() throws Exception {
    FakeTransport transport = new FakeTransport((request, call) -> FakeTransport.allOk());
    SinkListener throwing =
        new SinkListener() {
          @Override
          public void onFlush(SinkKey key, FlushReason reason, int docs, long bytes) {
            throw new IllegalStateException("listener bug");
          }

          @Override
          public void onBulkResponse(
              SinkKey key, int status, int docs, long bytes, Duration latency) {
            throw new IllegalStateException("listener bug");
          }
        };
    BulkSink<String> sink =
        new BulkSink<>(
            new SinkKey("orders", "search-eu", null),
            settings(1, 1 << 20, HOUR, 1),
            transport,
            breaker,
            Duration.ofMillis(5),
            timer,
            outcomes,
            throwing,
            Clock.systemUTC(),
            closed -> {});
    sinks.add(sink);

    submitAll(sink, "a", "b");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.all()).hasSize(2).containsOnly(ActionOutcome.INDEXED);
  }

  // ---- request-level failures ----

  @Test
  void requestLevelFailuresAreRetriedForeverAndNeverCountTowardThePoisonBound() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) -> {
          if (call <= 5) {
            throw new ConnectException("Connection refused");
          }
          if (call <= 8) {
            return FakeTransport.status(503);
          }
          if (call == 9) {
            return FakeTransport.status(429);
          }
          if (call == 10) {
            return new Result(200, "<html>proxy</html>".getBytes(UTF_8));
          }
          if (call == 11) {
            return FakeTransport.status(404);
          }
          return FakeTransport.allOk();
        });
    breaker = CircuitBreaker.ofDefaults("never-opens"); // isolate the retry loop from the breaker
    BulkSink<String> sink = sink(retrying(2, HOUR, 1, Duration.ZERO), transport);

    submitAll(sink, "a", "b");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.all()).containsOnly(ActionOutcome.INDEXED);
    assertThat(transport.requests()).hasSize(12).allMatch(r -> r.ids().equals(List.of("a", "b")));
    assertThat(events.requestRetries).hasValue(11);
    assertThat(events.answerWithThrowable).isFalse(); // incl. the unparsable 2xx (ADR-0030)
  }

  @Test
  void requestAuthFailureRetriesReportsAndRecovers() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    FakeTransport transport =
        new FakeTransport((request, call) -> {
          if (call <= 3) {
            return new Result(401, Fixtures.load("request-401.json"));
          }
          block(release);
          return FakeTransport.allOk();
        });
    BulkSink<String> sink = sink(poison(1, Duration.ZERO), transport);

    submitAll(sink, "a");
    await().until(() -> events.authFailures.get() == 3);
    assertThat(sink.authFailing()).isTrue();
    assertThat(outcomes.count()).isZero();
    release.countDown();

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.get("a")).isEqualTo(ActionOutcome.INDEXED);
    assertThat(sink.authFailing()).isFalse();
    assertThat(events.authRecovered).hasValue(1);
    assertThat(events.lastAuthReason)
        .startsWith("type=security_exception, root_cause=[security_exception], reason=")
        .contains("unable to authenticate user [elastic]")
        .doesNotContain("WWW-Authenticate");
    assertThat(breaker.getMetrics().getNumberOfFailedCalls()).isZero();
  }

  @Test
  void itemAuthFailuresOfOneResponseAreOneEventOnTheSenderThread() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            FakeTransport.items(
                request,
                sent ->
                    call == 1 ? new String[] {"403", "security_exception"} : new String[] {"201"}));
    // max-docs seals both the batch and the three retries, which share one backoff; no age timer.
    // Ordering against outcomes is pinned by itemAuthFailureIsReportedBeforeTheResponsesOutcomes.
    BulkSink<String> sink = sink(retrying(3, HOUR, 10, HOUR), transport);
    events.authFailingProbe = sink::authFailing;

    submitAll(sink, "a", "b", "c");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(transport.requests().getFirst().ids()).containsExactly("a", "b", "c");
    assertThat(events.authFailures).as("one event per response, not per item").hasValue(1);
    assertThat(events.authFailingAtFailure).as("state set before the event").containsOnly(true);
    assertThat(events.authThreads)
        .singleElement()
        .satisfies(t -> assertThat(t.isVirtual()).isTrue())
        .satisfies(t -> assertThat(t.getName()).startsWith("kafkafim-bulk-orders-"));
    await().until(() -> events.authRecovered.get() == 1);
  }

  @Test
  void requestAuthFailureSurvivesServerErrorsAndRecoversBeforeTheOutcome() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    FakeTransport transport =
        new FakeTransport((request, call) -> {
          return switch (call) {
            case 1 -> new Result(401, Fixtures.load("request-401.json"));
            case 2 -> FakeTransport.status(503);
            default -> {
              block(release);
              yield FakeTransport.allOk();
            }
          };
        });
    BulkSink<String> sink = sink(poison(1, Duration.ZERO), transport);
    List<Integer> recoveredAtOutcome = new CopyOnWriteArrayList<>();
    outcomes.beforeEach = handle -> recoveredAtOutcome.add(events.authRecovered.get());

    submitAll(sink, "a");
    await().until(() -> transport.requests().size() == 3);
    assertThat(sink.authFailing()).as("a 503 says nothing about the identity").isTrue();
    assertThat(events.authFailures).hasValue(1);
    assertThat(events.authRecovered).hasValue(0);
    release.countDown();

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(recoveredAtOutcome).containsExactly(1);
    assertThat(sink.authFailing()).isFalse();
  }

  // ---- batch reports (spans, DESIGN §10) ----

  @Test
  void batchReportListsEveryRequestWithItsRetriesStatusAndItemErrors() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) -> {
          if (call == 1) {
            throw new ConnectException("Connection refused");
          }
          if (call == 2) {
            return FakeTransport.status(503);
          }
          return FakeTransport.items(
              request,
              sent ->
                  sent.id().equals("bad")
                      ? new String[] {"400", "mapper_parsing_exception"}
                      : new String[] {"201"});
        });
    BulkSink<String> sink = sink(settings(3, 1 << 20, HOUR, 1), transport);

    submitAll(sink, "a", "bad", "c");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    await().atMost(WAIT).until(() -> events.reports.size() == 1);
    BatchReport report = events.reports.getFirst();
    assertThat(report.reason()).isEqualTo(FlushReason.DOCS);
    assertThat(report.docs()).isEqualTo(3);
    assertThat(report.handles()).map(Object::toString).containsExactly("a", "bad", "c");
    assertThat(report.failed()).isTrue();
    assertThat(report.sealed()).isBeforeOrEqualTo(report.started());
    assertThat(report.started()).isBeforeOrEqualTo(report.ended());
    assertThat(report.requests())
        .extracting(
            BatchReport.Request::status,
            BatchReport.Request::retries,
            BatchReport.Request::itemErrors,
            BatchReport.Request::docs)
        .containsExactly(tuple(0, 0, 0, 3), tuple(503, 1, 0, 3), tuple(200, 2, 1, 3));
    assertThat(report.requests().getFirst().error()).contains("Connection refused");
    assertThat(report.requests().get(2).failed()).isTrue();
    assertThat(events.itemErrors).containsExactly("400 mapper_parsing_exception");
  }

  @Test
  void successfulBatchReportsOneRequestWithoutFailure() throws Exception {
    BulkSink<String> sink =
        sink(settings(2, 1 << 20, HOUR, 1), new FakeTransport((r, c) -> FakeTransport.allOk()));

    submitAll(sink, "a", "b");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    await().atMost(WAIT).until(() -> events.reports.size() == 1);
    BatchReport report = events.reports.getFirst();
    assertThat(report.failed()).isFalse();
    assertThat(report.requests())
        .singleElement()
        .satisfies(r -> {
          assertThat(r.status()).isEqualTo(200);
          assertThat(r.retries()).isZero();
          assertThat(r.error()).isNull();
        });
    assertThat(events.itemErrors).isEmpty();
  }

  @Test
  void splitBatchReportsEveryHalfAsItsOwnRequest() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            request.actions().size() > 2 ? FakeTransport.status(413) : FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(4, 1 << 20, HOUR, 1), transport);

    submitAll(sink, ids(4));

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    await().atMost(WAIT).until(() -> events.reports.size() == 1);
    assertThat(events.reports.getFirst().requests())
        .extracting(BatchReport.Request::status, BatchReport.Request::docs)
        .containsExactly(tuple(413, 4), tuple(200, 2), tuple(200, 2));
  }

  @Test
  void retryLogNamesErrorTypesAndKeepsTheBodyForDebug() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(BulkSink.class);
    final Level level = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.DEBUG);
    try {
      FakeTransport transport =
          new FakeTransport((request, call) ->
              switch (call) {
                case 1 -> new Result(500, ErrorSummaryTest.QUOTING_500.getBytes(UTF_8));
                case 2 -> new Result(400, ErrorSummaryTest.QUOTING_500.getBytes(UTF_8));
                case 3 -> new Result(404, "<html>4111-SECRET</html>".getBytes(UTF_8));
                default -> FakeTransport.allOk();
              });
      breaker = CircuitBreaker.ofDefaults("never-opens");
      BulkSink<String> sink = sink(settings(1, 1 << 20, HOUR, 1), transport);

      submitAll(sink, "a");

      assertThat(sink.awaitIdle(WAIT)).isTrue();
      List<String> warnings = messages(appender, Level.WARN);
      assertThat(warnings)
          .hasSize(3)
          .noneMatch(m -> m.contains("SECRET"))
          .anyMatch(m ->
              m.contains("status 500")
                  && m.endsWith(
                      ": type=document_parsing_exception,"
                          + " root_cause=[document_parsing_exception, number_format_exception]"))
          .anyMatch(m -> m.contains("status 400") && m.contains("unexpected: type="))
          .anyMatch(m -> m.contains("status 404") && m.endsWith("unexpected: non-JSON, 24 bytes"));
      assertThat(messages(appender, Level.DEBUG))
          .filteredOn(m -> m.contains("response body of the failed request"))
          .hasSize(3)
          .allMatch(m -> m.contains("4111-SECRET"));
    } finally {
      logger.setLevel(level);
      logger.detachAppender(appender);
    }
  }

  @Test
  void retryLogHasNoBodyAboveDebug() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(BulkSink.class);
    final Level level = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      FakeTransport transport =
          new FakeTransport((request, call) ->
              call == 1
                  ? new Result(503, ErrorSummaryTest.QUOTING_500.getBytes(UTF_8))
                  : FakeTransport.allOk());
      breaker = CircuitBreaker.ofDefaults("never-opens");
      BulkSink<String> sink = sink(settings(1, 1 << 20, HOUR, 1), transport);

      submitAll(sink, "a");

      assertThat(sink.awaitIdle(WAIT)).isTrue();
      assertThat(logged(appender))
          .isNotEmpty()
          .noneMatch(e -> e.getFormattedMessage().contains("SECRET"));
    } finally {
      logger.setLevel(level);
      logger.detachAppender(appender);
    }
  }

  static List<String> messages(ListAppender<ILoggingEvent> appender, Level level) {
    return logged(appender).stream()
        .filter(e -> e.getLevel() == level)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  @Test
  void requestRejectedWith413IsSplitInHalves() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            request.actions().size() > 2 ? FakeTransport.status(413) : FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(8, 1 << 20, HOUR, 1), transport);

    submitAll(sink, ids(8));

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.all()).hasSize(8).containsOnly(ActionOutcome.INDEXED);
    assertThat(transport.requests())
        .extracting(r -> r.actions().size())
        .containsExactly(8, 4, 2, 2, 4, 2, 2);
  }

  @Test
  void singleDocument413GoesToDlqTooLarge() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            request.ids().contains("big") ? FakeTransport.status(413) : FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(4, 1 << 20, HOUR, 1), transport);

    submitAll(sink, "a", "big", "c", "d");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(transport.requests())
        .extracting(FakeTransport.Request::ids)
        .containsExactly(
            List.of("a", "big", "c", "d"),
            List.of("a", "big"),
            List.of("a"),
            List.of("big"),
            List.of("c", "d"));
    assertThat(outcomes.get("big"))
        .isInstanceOfSatisfying(
            ActionOutcome.Dlq.class,
            dlq -> {
              assertThat(dlq.errorType()).isEqualTo(SinkErrorType.TOO_LARGE);
              assertThat(dlq.status()).isEqualTo(413);
            });
    assertThat(List.of("a", "c", "d")).allMatch(id -> outcomes.get(id) == ActionOutcome.INDEXED);
  }

  @Test
  void itemCountMismatchIsRetriedAsRequestFailure() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            call == 1
                ? new Result(
                    200,
                    "{\"errors\":true,\"items\":[{\"index\":{\"status\":201}}]}".getBytes(UTF_8))
                : FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(2, 1 << 20, HOUR, 1), transport);

    submitAll(sink, "a", "b");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(transport.requests()).hasSize(2);
    assertThat(outcomes.all()).containsOnly(ActionOutcome.INDEXED);
    assertThat(events.requestRetries).hasValue(1);
    assertThat(events.answerWithThrowable).isFalse();
  }

  // ---- breaker ----

  @Test
  void openBreakerHoldsSendersAndDropsNothing() throws Exception {
    FakeTransport transport = new FakeTransport((request, call) -> FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(1, 1 << 20, HOUR, 4), transport);
    breaker.transitionToOpenState();

    submitAll(sink, "a", "b");
    Thread.sleep(150);
    assertThat(transport.requests()).isEmpty();
    assertThat(sink.pendingActions()).isEqualTo(2);
    breaker.transitionToClosedState();

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.all()).containsOnly(ActionOutcome.INDEXED);
  }

  @Test
  void clusterFailuresOpenTheBreakerAndProbesCloseIt() throws Exception {
    breaker =
        CircuitBreaker.of(
            "c",
            new BreakerSettings(4, 4, 50f, Duration.ofMillis(200), 1, Duration.ofMillis(5))
                .toConfig(ClientSettings.DEFAULT.responseTimeout()));
    AtomicInteger failures = new AtomicInteger(6);
    List<CircuitBreaker.State> seen = new CopyOnWriteArrayList<>();
    breaker
        .getEventPublisher()
        .onStateTransition(e -> seen.add(e.getStateTransition().getToState()));
    FakeTransport transport =
        new FakeTransport((request, call) ->
            failures.getAndDecrement() > 0 ? FakeTransport.status(503) : FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(1, 1 << 20, HOUR, 2), transport);

    submitAll(sink, "a", "b");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.all()).containsOnly(ActionOutcome.INDEXED);
    assertThat(seen).contains(CircuitBreaker.State.OPEN, CircuitBreaker.State.HALF_OPEN);
    assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
  }

  @Test
  void exceptionDuringHalfOpenProbesDoesNotLeakBreakerPermits() throws Exception {
    // Default breaker: the half-open safety net is minutes away, so only settled permits let
    // the probes through within the wait below.
    breaker =
        CircuitBreaker.of(
            "c", BreakerSettings.DEFAULT.toConfig(ClientSettings.DEFAULT.responseTimeout()));
    breaker.transitionToOpenState();
    breaker.transitionToHalfOpenState();
    FakeTransport transport =
        new FakeTransport((request, call) -> {
          if (call <= 2) {
            throw new IllegalStateException("unexpected client failure");
          }
          return FakeTransport.allOk();
        });
    BulkSink<String> sink = sink(settings(1, 1 << 20, HOUR, 2), transport);

    submitAll(sink, "a", "b");

    assertThat(sink.awaitIdle(Duration.ofSeconds(5))).isTrue();
    assertThat(outcomes.all()).hasSize(2).containsOnly(ActionOutcome.INDEXED);
  }

  @Test
  void itemAuthFailureEndedByTooLargeReportsRecovery() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            call == 1
                ? FakeTransport.items(request, sent -> new String[] {"403", "security_exception"})
                : FakeTransport.status(413));
    BulkSink<String> sink = sink(settings(1, 1 << 20, HOUR, 1), transport);

    submitAll(sink, "a");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    assertThat(outcomes.get("a")).isInstanceOf(ActionOutcome.Dlq.class);
    await().until(() -> events.authRecovered.get() == 1);
    assertThat(events.authFailures).hasValue(1);
    assertThat(sink.authFailing()).isFalse();
  }

  @Test
  void requestAuthFailureEndedByTooLargeReportsRecovery() throws Exception {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            call == 1
                ? new Result(401, Fixtures.load("request-401.json"))
                : FakeTransport.status(413));
    BulkSink<String> sink = sink(settings(1, 1 << 20, HOUR, 1), transport);

    submitAll(sink, "a");

    assertThat(sink.awaitIdle(WAIT)).isTrue();
    await().until(() -> events.authRecovered.get() == 1);
    assertThat(sink.authFailing()).isFalse();
  }

  // ---- lifecycle ----

  @Test
  void closeSendsWhatIsBatchedAndRejectsNewActions() {
    FakeTransport transport = new FakeTransport((request, call) -> FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(100, 1 << 20, HOUR, 4), transport);
    submitAll(sink, "a", "b", "c");

    assertThat(sink.close(WAIT)).isTrue();

    assertThat(outcomes.all()).hasSize(3).containsOnly(ActionOutcome.INDEXED);
    assertThatIllegalStateException().isThrownBy(() -> sink.submit(doc("late")));
  }

  @Test
  void closeWaitsForRetriesWithinTheTimeout() {
    FakeTransport transport =
        new FakeTransport((request, call) ->
            call <= 2
                ? FakeTransport.items(
                    request, sent -> new String[] {"429", "es_rejected_execution_exception"})
                : FakeTransport.allOk());
    BulkSink<String> sink = sink(settings(100, 1 << 20, HOUR, 4), transport);
    submitAll(sink, "a");

    assertThat(sink.close(WAIT)).isTrue();
    assertThat(outcomes.get("a")).isEqualTo(ActionOutcome.INDEXED);
  }

  @Test
  void closeTimeoutAbandonsInFlightActionsWithoutOutcome() {
    CountDownLatch never = new CountDownLatch(1);
    FakeTransport transport =
        new FakeTransport((request, call) -> {
          block(never);
          return FakeTransport.allOk();
        });
    BulkSink<String> sink = sink(settings(1, 1 << 20, HOUR, 4), transport);
    submitAll(sink, "a");
    await().until(() -> transport.requests().size() == 1);

    assertThat(sink.close(Duration.ofMillis(100))).isFalse();
    never.countDown();

    assertThat(outcomes.count()).isZero();
  }

  @Test
  void failingCallbackDoesNotStallTheSink() throws Exception {
    FakeTransport transport = new FakeTransport((request, call) -> FakeTransport.allOk());
    BulkSink<String> sink =
        new BulkSink<>(
            new SinkKey("orders", "search-eu", null),
            settings(1, 1 << 20, HOUR, 1),
            transport,
            breaker,
            Duration.ofMillis(5),
            timer,
            (action, outcome) -> {
              throw new IllegalStateException("boom");
            },
            SinkListener.NONE,
            Clock.systemUTC(),
            closed -> {});
    sinks.add(sink);
    submitAll(sink, "a", "b");
    assertThat(sink.awaitIdle(WAIT)).isTrue();
  }

  static void block(CountDownLatch latch) throws IOException {
    try {
      if (!latch.await(30, TimeUnit.SECONDS)) {
        throw new IOException("test latch timed out");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted", e);
    }
  }

  /** Records outcomes by handle and fails on a second outcome for one action. */
  static final class Outcomes implements OutcomeCallback<String> {
    private final Map<String, List<ActionOutcome>> byHandle = new ConcurrentHashMap<>();

    /** Runs on the sink's thread before each outcome is recorded, to observe what came first. */
    volatile Consumer<String> beforeEach = handle -> {};

    @Override
    public void onOutcome(BulkAction<String> action, ActionOutcome outcome) {
      beforeEach.accept(action.handle());
      byHandle.computeIfAbsent(action.handle(), h -> new CopyOnWriteArrayList<>()).add(outcome);
    }

    ActionOutcome get(String handle) {
      List<ActionOutcome> list = byHandle.get(handle);
      assertThat(list).as("outcome of %s", handle).isNotNull().hasSize(1);
      return list.getFirst();
    }

    int count() {
      return byHandle.values().stream().mapToInt(List::size).sum();
    }

    List<ActionOutcome> all() {
      return byHandle.values().stream().flatMap(List::stream).toList();
    }

    void assertAtMostOnce() {
      byHandle.forEach((handle, list) -> assertThat(list).as("outcomes of %s", handle).hasSize(1));
    }
  }

  /** Records listener events. */
  static final class Events implements SinkListener {
    final List<FlushReason> flushes = Collections.synchronizedList(new ArrayList<>());
    final List<Long> flushBytes = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger requestRetries = new AtomicInteger();
    final AtomicBoolean answerWithThrowable = new AtomicBoolean();
    final AtomicInteger itemRetries = new AtomicInteger();
    final AtomicInteger authFailures = new AtomicInteger();
    final AtomicInteger authRecovered = new AtomicInteger();
    final List<String> itemErrors = new CopyOnWriteArrayList<>();
    final List<BatchReport> reports = new CopyOnWriteArrayList<>();
    volatile String lastAuthReason = "";
    final List<Thread> authThreads = new CopyOnWriteArrayList<>();
    final List<Boolean> authFailingAtFailure = new CopyOnWriteArrayList<>();
    volatile BooleanSupplier authFailingProbe = () -> true;

    @Override
    public void onItemError(SinkKey key, int status, String errorType) {
      itemErrors.add(status + " " + errorType);
    }

    @Override
    public void onBatchSent(SinkKey key, BatchReport report) {
      reports.add(report);
    }

    @Override
    public void onFlush(SinkKey key, FlushReason reason, int docs, long bytes) {
      flushes.add(reason);
      flushBytes.add(bytes);
    }

    @Override
    public void onRequestRetry(
        SinkKey key, int status, @Nullable Throwable error, int attempt, Duration delay) {
      requestRetries.incrementAndGet();
      if (status != 0 && error != null) {
        answerWithThrowable.set(true); // only a transport error carries one (ADR-0030)
      }
    }

    @Override
    public void onItemRetry(SinkKey key, int status, String errorType, int attempts) {
      itemRetries.incrementAndGet();
    }

    @Override
    public void onAuthFailure(SinkKey key, int status, String reason) {
      lastAuthReason = reason;
      authThreads.add(Thread.currentThread());
      authFailingAtFailure.add(authFailingProbe.getAsBoolean());
      authFailures.incrementAndGet();
    }

    @Override
    public void onAuthRecovered(SinkKey key) {
      authRecovered.incrementAndGet();
    }
  }
}
