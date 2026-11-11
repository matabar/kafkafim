package io.github.matabar.kafkafim.dlq;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dead-letters documents of one pipeline to its DLQ topic (DESIGN §9), through the shared producer
 * of the pipeline's {@code (source cluster, DLQ identity)}. Obtained from {@link
 * DlqProducers#writer(String)}; thread-safe.
 *
 * <p><b>Contract.</b> {@link #send} never blocks and returns a future that completes
 * <em>successfully only after the broker acknowledged the DLQ record</em> ({@code acks=all},
 * idempotent producer). The caller completes the source record (or one part of it, ADR-0025) in
 * that completion and only there. A failed attempt is never reported as an exceptional completion:
 * it is retried with backoff (100ms doubling to 30s) until it succeeds, and reported to the {@link
 * DlqListener} with its {@link DlqFailure.Kind kind}. The future completes exceptionally only with
 * {@link DlqClosedException}, when the writers are closed (or this writer is {@linkplain
 * DlqProducers#evict evicted}) before the record was acknowledged; the caller must then leave the
 * offset uncommitted, so the record is re-delivered.
 *
 * <p><b>Blocked state</b> (DESIGN §7, "DLQ produce fails"): from the first failure of a send until
 * every send that failed has been acknowledged, the writer is {@linkplain #isBlocked() blocked} and
 * the listener heard {@link DlqListener#onBlocked} once. The unacknowledged records' offsets cannot
 * be committed, so the partition's committed offset stays put; the supervisor pauses the pipeline
 * and alerts. Records kept for retry are already counted in the source's {@code
 * max-in-flight-bytes}, so memory stays bounded.
 *
 * <p><b>Duplicates and order.</b> A resend after kafka-clients' own {@code delivery.timeout.ms} can
 * duplicate a record whose acknowledgement was lost, and resends are not ordered with later sends.
 * Both are harmless: DLQ records are independent, and a replay is idempotent by {@code _id}
 * (ADR-0006).
 */
public final class DlqWriter {

  private static final Logger log = LoggerFactory.getLogger(DlqWriter.class);

  private final String pipeline;
  private final String topic;
  private final SharedProducer producer;
  private final DlqProducers owner;
  private final Object stateLock = new Object();
  private int failing;
  private boolean configurationReported;
  private volatile boolean evicted;

  DlqWriter(String pipeline, String topic, SharedProducer producer, DlqProducers owner) {
    this.pipeline = pipeline;
    this.topic = topic;
    this.producer = producer;
    this.owner = owner;
  }

  /** The pipeline this writer serves. */
  public String pipeline() {
    return pipeline;
  }

  /** The DLQ topic. */
  public String topic() {
    return topic;
  }

  /** Whether a send has failed and not yet been acknowledged since. */
  public boolean isBlocked() {
    synchronized (stateLock) {
      return failing > 0;
    }
  }

  /**
   * Dead-letters one document. Never blocks.
   *
   * @param request the original record and the failure
   * @return completes with the DLQ record's metadata once the broker acknowledged it; completes
   *     exceptionally only with {@link DlqClosedException}
   */
  public CompletableFuture<RecordMetadata> send(DlqRequest request) {
    Objects.requireNonNull(request, "request");
    if (evicted) {
      return CompletableFuture.failedFuture(closedException());
    }
    Send send = new Send(request, DlqRecordFormat.toProducerRecord(topic, pipeline, request));
    if (!owner.register(send)) {
      return CompletableFuture.failedFuture(closedException());
    }
    owner.dispatch(send::attempt, send::abandon);
    // A copy, so a caller completing its future cannot fake an acknowledgement.
    return send.result.copy();
  }

  /** Stops this writer for good; {@link DlqProducers#evict} abandons its pending sends. */
  void evict() {
    evicted = true;
  }

  private boolean stopped() {
    return evicted || owner.isClosed();
  }

  private DlqClosedException closedException() {
    return new DlqClosedException(
        "DLQ writer closed before the record to '"
            + topic
            + "' of pipeline '"
            + pipeline
            + "' was acknowledged");
  }

  private void blocked(DlqFailure failure) {
    synchronized (stateLock) {
      if (failing++ > 0) {
        retryFailed(failure);
        return;
      }
      configurationReported = failure.kind() == DlqFailure.Kind.CONFIGURATION;
      String message =
          "DLQ produce for pipeline '{}' to topic '{}' ({}) failed ({}), blocking until it"
              + " succeeds: {}";
      if (failure.kind() == DlqFailure.Kind.CONFIGURATION) {
        log.error(
            message,
            pipeline,
            topic,
            producer.describeIdentity(),
            failure.kind(),
            describe(failure.cause()));
      } else {
        log.warn(
            message,
            pipeline,
            topic,
            producer.describeIdentity(),
            failure.kind(),
            describe(failure.cause()));
      }
      owner.notify(listener -> listener.onBlocked(pipeline, failure));
    }
  }

  /** Logs at ERROR once per blocked period when a later failure is a configuration failure. */
  private void retryFailed(DlqFailure failure) {
    // Callers hold stateLock.
    if (failing == 0 || configurationReported || failure.kind() != DlqFailure.Kind.CONFIGURATION) {
      return;
    }
    configurationReported = true;
    log.error(
        "DLQ produce for pipeline '{}' to topic '{}' ({}) is still blocked, now by a"
            + " configuration failure: {}",
        pipeline,
        topic,
        producer.describeIdentity(),
        describe(failure.cause()));
  }

  private void recovered() {
    synchronized (stateLock) {
      if (--failing > 0) {
        return;
      }
      configurationReported = false;
      log.info("DLQ produce for pipeline '{}' to topic '{}' recovered", pipeline, topic);
      owner.notify(listener -> listener.onUnblocked(pipeline));
    }
  }

  private static String describe(Throwable failure) {
    return failure.getClass().getSimpleName() + ": " + failure.getMessage();
  }

  /** One request until it is acknowledged or abandoned. */
  final class Send {

    private final DlqRequest request;
    private final ProducerRecord<byte[], byte[]> record;
    private final CompletableFuture<RecordMetadata> result = new CompletableFuture<>();
    private final AtomicBoolean done = new AtomicBoolean();
    private volatile @Nullable ScheduledFuture<?> retry;
    private int attempts;
    private boolean failedBefore;

    Send(DlqRequest request, ProducerRecord<byte[], byte[]> record) {
      this.request = request;
      this.record = record;
    }

    /** The writer this send belongs to. */
    DlqWriter writer() {
      return DlqWriter.this;
    }

    /** One attempt, on a DLQ thread: check the topic, then hand the record to the producer. */
    void attempt() {
      if (stopped()) {
        abandon();
        return;
      }
      attempts++;
      try {
        TopicStatus status = producer.ensureTopic(topic, owner.options().topicCheckTimeout());
        if (status != TopicStatus.EXISTS) {
          failed(topicFailure(status));
          return;
        }
        producer
            .client()
            .send(
                record,
                (metadata, exception) -> {
                  if (exception == null) {
                    // Synchronously, so a close that follows sees this send as acknowledged.
                    owner.unregister(this);
                    owner.dispatch(() -> succeeded(metadata), () -> succeeded(metadata));
                  } else {
                    owner.dispatch(() -> failed(exception), this::abandon);
                  }
                });
      } catch (RuntimeException e) {
        failed(e);
      }
    }

    private DlqTopicException topicFailure(TopicStatus status) {
      String what =
          switch (status) {
            case MISSING -> "does not exist; kafkafim never creates it (ADR-0021)";
            case NOT_AUTHORIZED -> "cannot be described: authorization or authentication failed";
            default -> "could not be checked: no answer from the broker";
          };
      return new DlqTopicException(
          status, "DLQ topic '" + topic + "' " + what + ", " + producer.describeIdentity());
    }

    private void succeeded(RecordMetadata metadata) {
      if (!done.compareAndSet(false, true)) {
        return;
      }
      owner.unregister(this);
      // One line per DLQ record (DESIGN §10, logs): coordinates and error type only; the reason
      // and the record itself are in the DLQ record, never in the log.
      log.warn(
          "Pipeline '{}': dead-lettered {} to {}-{}@{} (target {}/{})",
          pipeline,
          request,
          metadata.topic(),
          metadata.partition(),
          metadata.offset(),
          request.targetCluster() == null ? "-" : request.targetCluster(),
          request.targetIndex() == null ? "-" : request.targetIndex());
      owner.notify(listener -> listener.onWritten(pipeline, request, metadata));
      if (failedBefore) {
        recovered();
      }
      result.complete(metadata);
    }

    private void failed(Throwable exception) {
      if (stopped()) {
        abandon();
        return;
      }
      if (DlqFailure.isTimeout(exception)) {
        // Possibly "not present in metadata": the topic may be gone, so check it again.
        producer.forget(topic);
      }
      Duration retryIn = owner.options().backoff(attempts);
      DlqFailure failure =
          new DlqFailure(DlqFailure.classify(exception), exception, attempts, retryIn);
      log.debug(
          "DLQ send of {} for pipeline '{}' failed, attempt {}, retrying in {}",
          request,
          pipeline,
          attempts,
          retryIn,
          exception);
      owner.notify(listener -> listener.onSendFailed(pipeline, request, failure));
      if (!failedBefore) {
        failedBefore = true;
        blocked(failure);
      } else {
        synchronized (stateLock) {
          retryFailed(failure);
        }
      }
      owner.schedule(this, retryIn);
    }

    /** Keeps the scheduled retry, so {@link #abandon} can cancel it. */
    void retryWith(ScheduledFuture<?> scheduled) {
      retry = scheduled;
      if (done.get()) {
        // Abandoned while the retry was being scheduled; abandon() may not have seen it.
        scheduled.cancel(false);
      }
    }

    /**
     * Completes exceptionally: the writer closed or was evicted before an acknowledgement. Cancels
     * a retry waiting on the timer, which would otherwise hold this send until its delay ran out.
     */
    void abandon() {
      if (!done.compareAndSet(false, true)) {
        return;
      }
      ScheduledFuture<?> scheduled = retry;
      if (scheduled != null) {
        scheduled.cancel(false);
      }
      owner.unregister(this);
      result.completeExceptionally(closedException());
    }
  }
}
