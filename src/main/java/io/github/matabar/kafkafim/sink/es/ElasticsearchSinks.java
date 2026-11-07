package io.github.matabar.kafkafim.sink.es;

import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the Elasticsearch side of a pod: one client per {@code (cluster, identity)}, created on
 * first use and shared by every pipeline that writes with it; one circuit breaker per cluster,
 * shared across identities (DESIGN §5.2.1); one timer thread for batch ages and retry delays; and
 * the open {@link BulkSink}s. The supervisor opens one sink per {@code (pipeline, cluster,
 * identity)}.
 */
public final class ElasticsearchSinks implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinks.class);

  private final Map<String, ElasticsearchCluster> clusters;
  private final ClientSettings clientSettings;
  private final BreakerSettings breakerSettings;
  private final SinkListener listener;
  private final Clock clock;
  private final CircuitBreakerRegistry breakers;
  private final ScheduledThreadPoolExecutor timer;
  private final Map<ClientKey, Rest5Client> clients = new ConcurrentHashMap<>();
  private final Set<BulkSink<?>> sinks = ConcurrentHashMap.newKeySet();

  /**
   * Creates the registry; no connection is opened until a sink needs it.
   *
   * @param clusters {@code kafkafim.elasticsearch.clusters}, validated
   * @param clientSettings HTTP client settings
   * @param breakerSettings per-cluster breaker settings
   * @param listener metrics and auth hook
   */
  public ElasticsearchSinks(
      Map<String, ElasticsearchCluster> clusters,
      ClientSettings clientSettings,
      BreakerSettings breakerSettings,
      SinkListener listener) {
    this(clusters, clientSettings, breakerSettings, listener, Clock.systemUTC());
  }

  ElasticsearchSinks(
      Map<String, ElasticsearchCluster> clusters,
      ClientSettings clientSettings,
      BreakerSettings breakerSettings,
      SinkListener listener,
      Clock clock) {
    this.clusters = Map.copyOf(clusters);
    this.clientSettings = Objects.requireNonNull(clientSettings, "clientSettings");
    this.breakerSettings = Objects.requireNonNull(breakerSettings, "breakerSettings");
    this.listener = Objects.requireNonNull(listener, "listener");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.breakers = CircuitBreakerRegistry.of(breakerSettings.toConfig());
    this.breakers.getEventPublisher().onEntryAdded(added -> watch(added.getAddedEntry()));
    this.timer =
        new ScheduledThreadPoolExecutor(
            1, Thread.ofPlatform().name("kafkafim-sink-timer").daemon().factory());
    this.timer.setRemoveOnCancelPolicy(true);
  }

  /**
   * Opens the sink for one bulk stream.
   *
   * @param key pipeline, cluster and resolved identity ({@code null} only for an {@code http}
   *     cluster without identity)
   * @param settings batching, poison bound and version-conflict policy
   * @param callback receives every action's outcome
   * @param <H> type of the actions' completion handle
   * @return a new sink; close it before closing this registry
   * @throws IllegalArgumentException if the cluster or identity is unknown
   */
  public <H> BulkSink<H> open(SinkKey key, SinkSettings settings, OutcomeCallback<H> callback) {
    Rest5Client client = client(key.cluster(), key.identity());
    BulkSink<H> sink =
        new BulkSink<>(
            key,
            settings,
            BulkTransport.of(client),
            breaker(key.cluster()),
            breakerSettings.pollWhileOpen(),
            timer,
            callback,
            listener,
            clock,
            sinks::remove);
    sinks.add(sink);
    return sink;
  }

  /** The circuit breaker of a cluster, created on first use. */
  public CircuitBreaker breaker(String cluster) {
    requireCluster(cluster);
    return breakers.circuitBreaker(cluster);
  }

  /**
   * Stops every open sink without waiting (their pending actions get no outcome), then closes the
   * clients and the timer. Close sinks with {@link BulkSink#close(Duration)} first to drain them.
   */
  @Override
  public void close() {
    for (BulkSink<?> sink : new ArrayList<>(sinks)) {
      sink.close(Duration.ZERO);
    }
    timer.shutdownNow();
    List<Rest5Client> toClose = new ArrayList<>(clients.values());
    clients.clear();
    for (Rest5Client client : toClose) {
      try {
        client.close();
      } catch (IOException e) {
        LOG.warn("Closing an Elasticsearch client failed", e);
      }
    }
  }

  private void watch(CircuitBreaker breaker) {
    String cluster = breaker.getName();
    breaker
        .getEventPublisher()
        .onStateTransition(event -> {
          CircuitBreaker.StateTransition transition = event.getStateTransition();
          LOG.warn("Elasticsearch cluster '{}' circuit breaker: {}", cluster, transition);
          listener.onBreakerStateChange(
              cluster, transition.getFromState(), transition.getToState());
        });
  }

  private Rest5Client client(String cluster, @Nullable String identity) {
    ElasticsearchCluster config = requireCluster(cluster);
    return clients.computeIfAbsent(
        new ClientKey(cluster, identity),
        k -> ElasticsearchClients.create(cluster, config, identity, clientSettings));
  }

  private ElasticsearchCluster requireCluster(String cluster) {
    ElasticsearchCluster config = clusters.get(cluster);
    if (config == null) {
      throw new IllegalArgumentException("Unknown Elasticsearch cluster '" + cluster + "'");
    }
    return config;
  }

  private record ClientKey(String cluster, @Nullable String identity) {}
}
