package io.github.matabar.kafkafim.sink.es;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BulkSinkTest {

  static final Backoff FAST = new Backoff(Duration.ofMillis(1), Duration.ofMillis(5), 2.0, 0.0);
  static final Duration HOUR = Duration.ofHours(1);
  static final Duration WAIT = Duration.ofSeconds(10);

  final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1);
  final Outcomes outcomes = new Outcomes();
  final Events events = new Events();
  final List<BulkSink<String>> sinks = new ArrayList<>();
  CircuitBreaker breaker = CircuitBreaker.of("c", BreakerSettings.DEFAULT.toConfig());

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
    submitAll(sink, "a", "b");

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
    BulkSink<String> sink = sink(retrying(2, Duration.ofMillis(50), 10, HOUR), transport);

    submitAll(sink, "ok", "busy");

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
    assertThat(events.lastAuthReason).contains("security_exception");
    assertThat(breaker.getMetrics().getNumberOfFailedCalls()).isZero();
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
                .toConfig());
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
    breaker = CircuitBreaker.of("c", BreakerSettings.DEFAULT.toConfig());
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

    @Override
    public void onOutcome(BulkAction<String> action, ActionOutcome outcome) {
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
    final AtomicInteger itemRetries = new AtomicInteger();
    final AtomicInteger authFailures = new AtomicInteger();
    final AtomicInteger authRecovered = new AtomicInteger();
    volatile String lastAuthReason = "";

    @Override
    public void onFlush(SinkKey key, FlushReason reason, int docs, long bytes) {
      flushes.add(reason);
      flushBytes.add(bytes);
    }

    @Override
    public void onRequestRetry(
        SinkKey key, int status, @Nullable Throwable error, int attempt, Duration delay) {
      requestRetries.incrementAndGet();
    }

    @Override
    public void onItemRetry(SinkKey key, int status, String errorType, int attempts) {
      itemRetries.incrementAndGet();
    }

    @Override
    public void onAuthFailure(SinkKey key, int status, String reason) {
      lastAuthReason = reason;
      authFailures.incrementAndGet();
    }

    @Override
    public void onAuthRecovered(SinkKey key) {
      authRecovered.incrementAndGet();
    }
  }
}
