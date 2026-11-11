package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.dlq.DlqFailure;
import io.github.matabar.kafkafim.dlq.DlqListener;
import io.github.matabar.kafkafim.dlq.DlqRequest;
import io.github.matabar.kafkafim.sink.es.BatchReport;
import io.github.matabar.kafkafim.sink.es.FlushReason;
import io.github.matabar.kafkafim.sink.es.SinkKey;
import io.github.matabar.kafkafim.sink.es.SinkListener;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one {@link SinkListener} and {@link DlqListener} the building blocks are created with. It
 * exists before the {@link PipelineSupervisor} (which needs the sinks and DLQ producers), turns
 * events into supervisor pause decisions once the supervisor is attached, and forwards every event
 * to further listeners (the telemetry slice's meters).
 *
 * <p>Since hot reload, several {@code ElasticsearchSinks} and {@code DlqProducers} instances may be
 * alive at once (DESIGN §5.6): each gets its own listener, bound to its <em>owner</em> (the
 * instance itself), so the supervisor only applies a breaker or DLQ-blocked event to the pipelines
 * that currently use that instance. Auth events need no owner: the supervisor re-reads the current
 * sink's auth state for every one. Events forwarded to telemetry listeners are not filtered; a DLQ
 * writer a pipeline left behind is evicted, so it stops reporting (ADR-0035).
 */
final class ListenerRelay {

  private static final Logger LOG = LoggerFactory.getLogger(ListenerRelay.class);

  private final List<SinkListener> sinkListeners;
  private final List<DlqListener> dlqListeners;
  private volatile @Nullable PipelineSupervisor supervisor;

  ListenerRelay(List<SinkListener> sinkListeners, List<DlqListener> dlqListeners) {
    this.sinkListeners = List.copyOf(sinkListeners);
    this.dlqListeners = List.copyOf(dlqListeners);
  }

  /** Routes pause decisions to the supervisor from now on. */
  void attach(PipelineSupervisor attached) {
    this.supervisor = attached;
  }

  private void toSupervisor(Consumer<PipelineSupervisor> event) {
    PipelineSupervisor current = supervisor;
    if (current != null) {
      try {
        event.accept(current);
      } catch (RuntimeException e) {
        LOG.warn("Supervisor failed to handle an event", e);
      }
    }
  }

  private static <L> void forward(List<L> listeners, Consumer<L> event) {
    for (L listener : listeners) {
      try {
        event.accept(listener);
      } catch (RuntimeException e) {
        LOG.warn("Listener {} failed", listener, e);
      }
    }
  }

  /**
   * The listener for one {@code ElasticsearchSinks} instance.
   *
   * @param owner the instance, once constructed
   */
  SinkListener sinkListener(Supplier<Object> owner) {
    return new SinkListener() {
      @Override
      public void onFlush(SinkKey key, FlushReason reason, int docs, long bytes) {
        forward(sinkListeners, l -> l.onFlush(key, reason, docs, bytes));
      }

      @Override
      public void onBulkResponse(SinkKey key, int status, int docs, long bytes, Duration latency) {
        forward(sinkListeners, l -> l.onBulkResponse(key, status, docs, bytes, latency));
      }

      @Override
      public void onRequestRetry(
          SinkKey key, int status, @Nullable Throwable error, int attempt, Duration delay) {
        forward(sinkListeners, l -> l.onRequestRetry(key, status, error, attempt, delay));
      }

      @Override
      public void onAuthFailure(SinkKey key, int status, String reason) {
        toSupervisor(s -> s.onAuthChanged(key.pipeline(), key.cluster(), key.identity()));
        forward(sinkListeners, l -> l.onAuthFailure(key, status, reason));
      }

      @Override
      public void onAuthRecovered(SinkKey key) {
        toSupervisor(s -> s.onAuthChanged(key.pipeline(), key.cluster(), key.identity()));
        forward(sinkListeners, l -> l.onAuthRecovered(key));
      }

      @Override
      public void onItemRetry(SinkKey key, int status, String errorType, int attempts) {
        forward(sinkListeners, l -> l.onItemRetry(key, status, errorType, attempts));
      }

      @Override
      public void onItemError(SinkKey key, int status, String errorType) {
        forward(sinkListeners, l -> l.onItemError(key, status, errorType));
      }

      @Override
      public void onBatchSent(SinkKey key, BatchReport report) {
        forward(sinkListeners, l -> l.onBatchSent(key, report));
      }

      @Override
      public void onBreakerStateChange(
          String cluster, CircuitBreaker.State from, CircuitBreaker.State to) {
        toSupervisor(s -> s.onBreakerStateChange(owner.get(), cluster, to));
        forward(sinkListeners, l -> l.onBreakerStateChange(cluster, from, to));
      }
    };
  }

  /**
   * The listener for one {@code DlqProducers} instance.
   *
   * @param owner the instance, once constructed
   */
  DlqListener dlqListener(Supplier<Object> owner) {
    return new DlqListener() {
      @Override
      public void onWritten(String pipeline, DlqRequest request, RecordMetadata metadata) {
        forward(dlqListeners, l -> l.onWritten(pipeline, request, metadata));
      }

      @Override
      public void onSendFailed(String pipeline, DlqRequest request, DlqFailure failure) {
        forward(dlqListeners, l -> l.onSendFailed(pipeline, request, failure));
      }

      @Override
      public void onBlocked(String pipeline, DlqFailure failure) {
        toSupervisor(s -> s.onDlqBlocked(owner.get(), pipeline, true));
        forward(dlqListeners, l -> l.onBlocked(pipeline, failure));
      }

      @Override
      public void onUnblocked(String pipeline) {
        toSupervisor(s -> s.onDlqBlocked(owner.get(), pipeline, false));
        forward(dlqListeners, l -> l.onUnblocked(pipeline));
      }
    };
  }
}
