package io.github.matabar.kafkafim.sink.es;

import io.github.matabar.kafkafim.sink.es.ErrorClassifier.ItemVerdict;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The batcher and bulk sender of one bulk stream: a pipeline writing to one Elasticsearch cluster
 * under one identity (DESIGN §4.2, ADR-0008).
 *
 * <p><b>Batching.</b> {@link #submit} appends to the open batch, which is sealed when it reaches
 * {@code max-docs} or {@code max-bytes} (an action that would push it over {@code max-bytes} seals
 * it first, so only an action larger than {@code max-bytes} by itself makes a bigger request), when
 * its oldest action is {@code max-age} old, or on {@link #flush()}.
 *
 * <p><b>Sending.</b> Each sealed batch gets a virtual thread; at most {@code in-flight} requests
 * are outstanding, and batches start in the order they were sealed. A request-level failure
 * (transport error, 429, 5xx, 401/403, any other unexpected status, an unparsable response) is
 * retried with the same actions forever, with exponential backoff and jitter, and never reaches the
 * DLQ; 413 splits the request in halves. Before every attempt the sender asks the cluster's circuit
 * breaker and waits while it is open. Item results are classified by {@link ErrorClassifier};
 * retryable items go back into the batcher after a backoff and are bounded by {@code
 * max-attempts}/{@code max-elapsed}.
 *
 * <p><b>Backpressure.</b> {@code submit} never blocks: the consumer thread must keep polling, and
 * the Kafka side already bounds what is outstanding by pausing the container at {@code
 * max-in-flight-bytes} until outcomes arrive (ADR-0010). Everything the sink holds is an action
 * without an outcome, so it is inside that budget; {@link #pendingBytes()} exposes it.
 *
 * <p><b>Outcomes.</b> Every submitted action gets exactly one {@link ActionOutcome} through the
 * {@link OutcomeCallback}, unless the sink is closed first: then it gets none and its record is
 * re-delivered after the next assignment (idempotent by {@code _id}, ADR-0006).
 *
 * @param <H> type of the actions' completion handle
 */
public final class BulkSink<H> implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(BulkSink.class);

  /** Close timeout used by {@link #close()}. */
  public static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30);

  private static final int LOGGED_BODY_CHARS = 1024;

  private final SinkKey key;
  private final SinkSettings settings;
  private final BulkTransport transport;
  private final CircuitBreaker breaker;
  private final Duration breakerPoll;
  private final ScheduledExecutorService timer;
  private final OutcomeCallback<H> callback;
  private final SinkListener listener;
  private final Clock clock;
  private final Consumer<BulkSink<?>> onClosed;

  private final ExecutorService senders;
  private final Semaphore inFlight;
  private final Queue<Batch> ready = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean authFailing = new AtomicBoolean();

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition idle = lock.newCondition();
  private Batch open;
  private long generation;
  private long pendingActions;
  private long pendingBytes;
  private boolean closing;
  private volatile boolean stopped;

  BulkSink(
      SinkKey key,
      SinkSettings settings,
      BulkTransport transport,
      CircuitBreaker breaker,
      Duration breakerPoll,
      ScheduledExecutorService timer,
      OutcomeCallback<H> callback,
      SinkListener listener,
      Clock clock,
      Consumer<BulkSink<?>> onClosed) {
    this.key = Objects.requireNonNull(key, "key");
    this.settings = Objects.requireNonNull(settings, "settings");
    this.transport = Objects.requireNonNull(transport, "transport");
    this.breaker = Objects.requireNonNull(breaker, "breaker");
    this.breakerPoll = Objects.requireNonNull(breakerPoll, "breakerPoll");
    this.timer = Objects.requireNonNull(timer, "timer");
    this.callback = Objects.requireNonNull(callback, "callback");
    this.listener = Objects.requireNonNull(listener, "listener");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.onClosed = Objects.requireNonNull(onClosed, "onClosed");
    this.senders =
        Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("kafkafim-bulk-" + key.pipeline() + "-", 0).factory());
    this.inFlight = new Semaphore(settings.inFlight(), true);
    this.open = new Batch(0);
  }

  /** The stream this sink writes. */
  public SinkKey key() {
    return key;
  }

  /**
   * Queues an action. Never blocks; the outcome arrives later through the callback.
   *
   * @throws IllegalStateException if the sink is closing or closed
   */
  public void submit(BulkAction<H> action) {
    Entry entry = new Entry(action, BulkBody.metadataLine(action));
    List<Batch> sealed;
    lock.lock();
    try {
      if (closing) {
        throw new IllegalStateException("bulk sink " + key + " is closed");
      }
      pendingActions++;
      pendingBytes += entry.size;
      sealed = addLocked(entry);
    } finally {
      lock.unlock();
    }
    dispatch(sealed);
  }

  /** Seals the open batch now, if it has actions. Does not wait for the request. */
  public void flush() {
    Batch sealed = null;
    lock.lock();
    try {
      if (!open.entries.isEmpty()) {
        sealed = sealLocked(FlushReason.EXPLICIT);
      }
    } finally {
      lock.unlock();
    }
    if (sealed != null) {
      dispatch(List.of(sealed));
    }
  }

  /**
   * Waits until every submitted action has its outcome.
   *
   * @return {@code true} if nothing is pending, {@code false} on timeout
   * @throws InterruptedException if interrupted while waiting
   */
  public boolean awaitIdle(Duration timeout) throws InterruptedException {
    long nanos = timeout.toNanos();
    lock.lock();
    try {
      while (pendingActions > 0) {
        if (nanos <= 0) {
          return false;
        }
        nanos = idle.awaitNanos(nanos);
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** Actions submitted and not yet completed, including those waiting for a retry. */
  public long pendingActions() {
    lock.lock();
    try {
      return pendingActions;
    } finally {
      lock.unlock();
    }
  }

  /** Body bytes of the actions counted by {@link #pendingActions()}. */
  public long pendingBytes() {
    lock.lock();
    try {
      return pendingBytes;
    } finally {
      lock.unlock();
    }
  }

  /** Whether the last bulk answer on this stream was a 401/403 (a configuration failure). */
  public boolean authFailing() {
    return authFailing.get();
  }

  /**
   * Graceful close: rejects new actions, sends what is batched (retries are sealed at once from now
   * on), and waits up to {@code timeout} for every outcome. Then stops: requests still running are
   * abandoned and their actions get no outcome. Idempotent.
   *
   * @return {@code true} if every action got its outcome
   */
  public boolean close(Duration timeout) {
    lock.lock();
    try {
      closing = true;
    } finally {
      lock.unlock();
    }
    flush();
    boolean drained;
    try {
      drained = awaitIdle(timeout);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      drained = false;
    }
    stopped = true;
    senders.shutdownNow();
    onClosed.accept(this);
    if (!drained) {
      LOG.warn(
          "Bulk sink {} closed with {} actions without outcome; their records will be re-delivered",
          key,
          pendingActions());
    }
    return drained;
  }

  /** {@link #close(Duration)} with {@link #DEFAULT_CLOSE_TIMEOUT}. */
  @Override
  public void close() {
    close(DEFAULT_CLOSE_TIMEOUT);
  }

  // ---- batching (callers hold the lock) ----

  private List<Batch> addLocked(Entry entry) {
    List<Batch> sealed = new ArrayList<>(2);
    if (!open.entries.isEmpty() && open.bytes + entry.size > settings.maxBytes()) {
      sealed.add(sealLocked(FlushReason.BYTES));
    }
    if (open.entries.isEmpty()) {
      long batchGeneration = open.generation;
      try {
        open.ageTimer =
            timer.schedule(
                () -> sealIfOpen(batchGeneration),
                settings.maxAge().toNanos(),
                TimeUnit.NANOSECONDS);
      } catch (RejectedExecutionException e) {
        open.ageTimer = null; // the registry is closing; close() seals explicitly
      }
    }
    open.entries.add(entry);
    open.bytes += entry.size;
    if (open.entries.size() >= settings.maxDocs()) {
      sealed.add(sealLocked(FlushReason.DOCS));
    } else if (open.bytes >= settings.maxBytes()) {
      sealed.add(sealLocked(FlushReason.BYTES));
    } else if (closing) {
      sealed.add(sealLocked(FlushReason.EXPLICIT));
    }
    return sealed;
  }

  private Batch sealLocked(FlushReason reason) {
    Batch batch = open;
    batch.reason = reason;
    if (batch.ageTimer != null) {
      batch.ageTimer.cancel(false);
    }
    open = new Batch(++generation);
    return batch;
  }

  private void sealIfOpen(long batchGeneration) {
    Batch sealed = null;
    lock.lock();
    try {
      if (open.generation == batchGeneration && !open.entries.isEmpty()) {
        sealed = sealLocked(FlushReason.AGE);
      }
    } finally {
      lock.unlock();
    }
    if (sealed != null) {
      dispatch(List.of(sealed));
    }
  }

  private void dispatch(List<Batch> batches) {
    for (Batch batch : batches) {
      listener.onFlush(key, batch.reason, batch.entries.size(), batch.bytes);
      ready.add(batch);
      try {
        senders.execute(this::sendNext);
      } catch (RejectedExecutionException e) {
        LOG.debug("Bulk sink {} stopped; batch of {} not sent", key, batch.entries.size());
      }
    }
  }

  private void requeue(Entry entry) {
    if (stopped) {
      return;
    }
    List<Batch> sealed;
    lock.lock();
    try {
      sealed = addLocked(entry);
    } finally {
      lock.unlock();
    }
    dispatch(sealed);
  }

  private void scheduleRetry(Entry entry, Duration delay) {
    try {
      timer.schedule(() -> requeue(entry), delay.toNanos(), TimeUnit.NANOSECONDS);
    } catch (RejectedExecutionException e) {
      LOG.debug("Bulk sink {} stopped; retry not scheduled", key);
    }
  }

  // ---- sending (virtual threads) ----

  /**
   * One sender task per sealed batch. A task takes whichever batch is oldest once it holds a
   * permit, so batches start in the order they were sealed even though tasks wake in any order.
   */
  private void sendNext() {
    try {
      inFlight.acquire();
    } catch (InterruptedException e) {
      return; // stopped
    }
    Batch batch = ready.poll();
    if (batch == null) {
      inFlight.release();
      return;
    }
    try {
      send(batch.entries, batch.bytes);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // stopped; the actions get no outcome
    } catch (RuntimeException e) {
      // A bug, not an Elasticsearch answer. Retry the actions rather than leave them pending.
      LOG.error("Bulk sink {}: unexpected failure, retrying the batch", key, e);
      for (Entry entry : batch.entries) {
        if (!entry.done) {
          scheduleRetry(entry, settings.backoff().max());
        }
      }
    } finally {
      inFlight.release();
    }
  }

  private void send(List<Entry> entries, long bytes) throws InterruptedException {
    int attempt = 0;
    while (!stopped) {
      awaitBreakerPermission();
      byte[] body = BulkBody.build(entries, bytes);
      long start = System.nanoTime();
      BulkTransport.Result result;
      try {
        result = transport.send(body);
      } catch (IOException e) {
        breaker.onError(System.nanoTime() - start, TimeUnit.NANOSECONDS, e);
        attempt = retryRequest(0, e, attempt + 1, "transport error: " + e);
        continue;
      }
      long elapsed = System.nanoTime() - start;
      int status = result.status();
      listener.onBulkResponse(key, status, entries.size(), bytes, Duration.ofNanos(elapsed));
      switch (ErrorClassifier.request(status)) {
        case PARSE_ITEMS -> {
          BulkResponse response;
          try {
            response = BulkResponse.parse(result.body());
            if (response.errors() && response.items().size() != entries.size()) {
              throw new BulkResponse.MalformedResponseException(
                  response.items().size() + " items for " + entries.size() + " actions", null);
            }
          } catch (BulkResponse.MalformedResponseException e) {
            breaker.onError(elapsed, TimeUnit.NANOSECONDS, e);
            attempt = retryRequest(status, e, attempt + 1, "malformed response: " + e.getMessage());
            continue;
          }
          breaker.onSuccess(elapsed, TimeUnit.NANOSECONDS);
          if (authFailing.compareAndSet(true, false)) {
            LOG.info("Bulk sink {}: Elasticsearch accepts the identity again", key);
            listener.onAuthRecovered(key);
          }
          handleItems(entries, response);
          return;
        }
        case SPLIT -> {
          breaker.onSuccess(elapsed, TimeUnit.NANOSECONDS);
          if (entries.size() == 1) {
            Entry entry = entries.getFirst();
            entry.answered++;
            complete(
                entry,
                dlq(
                    entry,
                    SinkErrorType.TOO_LARGE,
                    "single-document bulk request of " + bytes + " bytes rejected with 413",
                    413));
            return;
          }
          int middle = entries.size() / 2;
          List<Entry> first = entries.subList(0, middle);
          List<Entry> second = entries.subList(middle, entries.size());
          LOG.info(
              "Bulk sink {}: 413 for {} actions ({} bytes), splitting", key, entries.size(), bytes);
          send(first, sizeOf(first));
          send(second, sizeOf(second));
          return;
        }
        case AUTH_FAILURE -> {
          breaker.releasePermission();
          authFailing.set(true);
          String reason = text(result.body());
          listener.onAuthFailure(key, status, reason);
          attempt = retryRequest(status, null, attempt + 1, "authentication failure: " + reason);
        }
        case RETRY_CLUSTER -> {
          breaker.onError(elapsed, TimeUnit.NANOSECONDS, new RequestFailure(status));
          attempt = retryRequest(status, null, attempt + 1, text(result.body()));
        }
        case RETRY_UNEXPECTED -> {
          breaker.releasePermission();
          attempt = retryRequest(status, null, attempt + 1, "unexpected: " + text(result.body()));
        }
        default -> throw new IllegalStateException("unhandled request verdict");
      }
    }
  }

  private int retryRequest(int status, @Nullable Throwable error, int attempt, String detail)
      throws InterruptedException {
    Duration delay = settings.backoff().delay(attempt);
    listener.onRequestRetry(key, status, error, attempt, delay);
    LOG.warn(
        "Bulk sink {}: request failed (status {}, attempt {}), retrying in {}ms: {}",
        key,
        status,
        attempt,
        delay.toMillis(),
        detail);
    Thread.sleep(delay);
    return attempt;
  }

  private void awaitBreakerPermission() throws InterruptedException {
    while (!breaker.tryAcquirePermission()) {
      Thread.sleep(breakerPoll);
    }
  }

  private void handleItems(List<Entry> entries, BulkResponse response) {
    if (!response.errors()) {
      for (Entry entry : entries) {
        entry.answered++;
        complete(entry, ActionOutcome.INDEXED);
      }
      return;
    }
    for (int i = 0; i < entries.size(); i++) {
      Entry entry = entries.get(i);
      BulkResponse.Item item = response.items().get(i);
      entry.answered++;
      ItemVerdict verdict = ErrorClassifier.item(item, entry.action, settings.onVersionConflict());
      String reason = item.errorJson() != null ? item.errorJson() : "status " + item.status();
      switch (verdict) {
        case INDEXED -> complete(entry, ActionOutcome.INDEXED);
        case STALE -> complete(entry, new ActionOutcome.Stale(reason));
        case DLQ_ES_ERROR ->
            complete(entry, dlq(entry, SinkErrorType.ES_ERROR, reason, item.status()));
        case DLQ_VERSION_CONFLICT ->
            complete(entry, dlq(entry, SinkErrorType.VERSION_CONFLICT, reason, item.status()));
        case RETRY -> retryItem(entry, item, reason);
        case AUTH_FAILURE -> {
          authFailing.set(true);
          listener.onAuthFailure(key, item.status(), reason);
          entry.markFailure(clock);
          entry.authRetries++;
          scheduleRetry(entry, settings.backoff().delay(entry.authRetries));
        }
        default -> throw new IllegalStateException("unhandled item verdict " + verdict);
      }
    }
  }

  private void retryItem(Entry entry, BulkResponse.Item item, String reason) {
    entry.markFailure(clock);
    entry.itemFailures++;
    if (!entry.retrying) {
      entry.retrying = true;
      entry.firstRetryableNanos = System.nanoTime();
    }
    String errorType = item.errorType() != null ? item.errorType() : "status_" + item.status();
    listener.onItemRetry(key, item.status(), errorType, entry.itemFailures);
    long elapsed = System.nanoTime() - entry.firstRetryableNanos;
    if (entry.itemFailures >= settings.maxAttempts()
        || elapsed >= settings.maxElapsed().toNanos()) {
      complete(entry, dlq(entry, SinkErrorType.RETRIES_EXHAUSTED, reason, item.status()));
    } else {
      scheduleRetry(entry, settings.backoff().delay(entry.itemFailures));
    }
  }

  private ActionOutcome.Dlq dlq(Entry entry, SinkErrorType type, String reason, int status) {
    entry.markFailure(clock);
    return new ActionOutcome.Dlq(type, reason, status, entry.answered, entry.firstFailure);
  }

  private void complete(Entry entry, ActionOutcome outcome) {
    if (stopped || entry.done) {
      return; // closed: the caller no longer expects outcomes; the record is re-delivered
    }
    entry.done = true;
    try {
      callback.onOutcome(entry.action, outcome);
    } catch (RuntimeException e) {
      LOG.error("Bulk sink {}: outcome callback failed", key, e);
    }
    lock.lock();
    try {
      pendingActions--;
      pendingBytes -= entry.size;
      if (pendingActions == 0) {
        idle.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  private long sizeOf(List<Entry> entries) {
    long size = 0;
    for (Entry entry : entries) {
      size += entry.size;
    }
    return size;
  }

  private static String text(byte[] body) {
    String text = new String(body, StandardCharsets.UTF_8);
    return text.length() <= LOGGED_BODY_CHARS ? text : text.substring(0, LOGGED_BODY_CHARS) + "…";
  }

  /** A sealed or open batch. */
  private final class Batch {
    final long generation;
    final List<Entry> entries = new ArrayList<>();
    long bytes;
    FlushReason reason = FlushReason.EXPLICIT;
    @Nullable ScheduledFuture<?> ageTimer;

    Batch(long generation) {
      this.generation = generation;
    }
  }

  /** One submitted action and its retry state; touched by one sender at a time. */
  private final class Entry implements BulkBody.Entry {
    final BulkAction<H> action;
    final byte[] metadataLine;
    final long size;
    int answered;
    int itemFailures;
    int authRetries;
    boolean retrying;
    long firstRetryableNanos;
    boolean done;
    @Nullable Instant firstFailure;

    Entry(BulkAction<H> action, byte[] metadataLine) {
      this.action = action;
      this.metadataLine = metadataLine;
      this.size = BulkBody.size(metadataLine, action.source());
    }

    void markFailure(Clock clock) {
      if (firstFailure == null) {
        firstFailure = clock.instant();
      }
    }

    @Override
    public byte[] metadataLine() {
      return metadataLine;
    }

    @Override
    public byte[] source() {
      return action.source();
    }
  }

  /** A request-level failure recorded by the breaker; no stack trace. */
  static final class RequestFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;

    RequestFailure(int status) {
      super("bulk request failed with HTTP " + status, null, false, false);
    }
  }
}
