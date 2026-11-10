package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.config.KafkaCluster;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
import io.github.matabar.kafkafim.dlq.DlqOptions;
import io.github.matabar.kafkafim.dlq.DlqProducers;
import io.github.matabar.kafkafim.sink.es.BreakerSettings;
import io.github.matabar.kafkafim.sink.es.ClientSettings;
import io.github.matabar.kafkafim.sink.es.ElasticsearchSinks;
import io.github.matabar.kafkafim.source.kafka.ConsumerFactories;
import io.github.matabar.kafkafim.source.kafka.SourceOptions;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * The building blocks pipelines share, across configuration generations (DESIGN §5.6). The {@code
 * dlq}, {@code sink.es} and {@code source.kafka} registries are each built over one fixed
 * configuration; this pool keeps one <em>current</em> instance per cluster and hands it to every
 * pipeline that (re)starts, so pipelines share clients, producers and breakers as before:
 *
 * <ul>
 *   <li><b>Elasticsearch</b>: one {@link ElasticsearchSinks} per cluster definition (clients per
 *       {@code (cluster, identity)}, one breaker for the cluster). When the cluster's definition
 *       changes (including any identity or secret), a new instance becomes current; pipelines that
 *       restart use it, pipelines left untouched keep the old one.
 *   <li><b>Kafka</b>: one {@link DlqProducers} (shared producer per {@code (cluster, DLQ
 *       identity)}, a writer per pipeline) and one {@link ConsumerFactories} per cluster
 *       definition. {@code DlqProducers} caches a pipeline's writer with its topic, so a pipeline
 *       whose DLQ settings changed, or a pipeline the current instance does not know, also makes a
 *       new instance current. A restarted pipeline whose DLQ settings and cluster are unchanged
 *       keeps its writer (and with it a {@code dlq-blocked} state) while its instance is still the
 *       current one. A pipeline that leaves an instance (it moved to a new one or was removed) has
 *       its writer there evicted (ADR-0035).
 * </ul>
 *
 * <p>Every instance counts its pipelines and is closed when its last pipeline releases it (an
 * instance that is no longer current gets no new pipelines). Until then old and new instances
 * coexist, so for a while two producers or clients may exist for the same {@code (cluster,
 * identity)}, and a cluster's breaker is per instance (each pipeline follows the breaker of the
 * instance it sends through).
 *
 * <p><b>Lifecycle</b>: a {@link SmartLifecycle} in the DLQ producers' phase ({@link
 * DlqProducers#PHASE}): stopping closes every {@code DlqProducers} after the supervisor drained the
 * pipelines (DESIGN §7). {@link #close()} (bean destruction) closes the Elasticsearch side.
 */
final class PipelineResources implements SmartLifecycle, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineResources.class);

  /**
   * Timings and client settings.
   *
   * @param sourceOptions Kafka source timings
   * @param sinkCloseTimeout how long a stopping pipeline waits for its sinks
   * @param dlqOptions DLQ producer timings
   * @param clientSettings Elasticsearch HTTP client pool settings; timeouts come from each cluster
   * @param breakerSettings per-cluster breaker settings
   * @param clock clock of the record processor
   */
  record Settings(
      SourceOptions sourceOptions,
      Duration sinkCloseTimeout,
      DlqOptions dlqOptions,
      ClientSettings clientSettings,
      BreakerSettings breakerSettings,
      Clock clock) {

    static final Settings DEFAULTS =
        new Settings(
            SourceOptions.defaults(),
            PipelineSupervisor.SINK_CLOSE_TIMEOUT,
            DlqOptions.DEFAULTS,
            ClientSettings.DEFAULT,
            BreakerSettings.DEFAULT,
            Clock.systemUTC());
  }

  private final ListenerRelay relay;
  private final Function<KafkafimProperties, Preflight.AuthCheck> authChecks;
  private final Settings settings;

  private final Map<String, KafkaHandle> currentKafka = new HashMap<>(); // guarded by this
  private final Map<String, EsHandle> currentEs = new HashMap<>(); // guarded by this
  private final Map<Object, Handle> handles = new IdentityHashMap<>(); // guarded by this
  private final Map<PipelineRuntime.Context, String> pipelineOf =
      new IdentityHashMap<>(); // guarded by this
  private boolean closed; // guarded by this
  private volatile boolean running;

  PipelineResources(
      ListenerRelay relay,
      Function<KafkafimProperties, Preflight.AuthCheck> authChecks,
      Settings settings) {
    this.relay = relay;
    this.authChecks = authChecks;
    this.settings = settings;
  }

  /**
   * The context a pipeline (re)starts with, taking one use of every instance it gets.
   *
   * @param pipeline a pipeline of {@code properties}
   * @param properties the validated configuration the pipeline runs with
   * @throws IllegalStateException once closed
   */
  PipelineRuntime.Context acquire(String pipeline, KafkafimProperties properties) {
    Pipeline config = properties.pipelines().get(pipeline);
    if (config == null) {
      throw new IllegalArgumentException("Unknown pipeline '" + pipeline + "'");
    }
    List<Handle> retired = new ArrayList<>();
    PipelineRuntime.Context context;
    synchronized (this) {
      if (closed) {
        throw new IllegalStateException("Pipeline resources are closed");
      }
      KafkaHandle kafka = kafka(pipeline, config, properties, retired);
      Map<String, ElasticsearchSinks> sinks = new LinkedHashMap<>();
      List<EsHandle> es = new ArrayList<>();
      for (Pipeline.Route route : config.routes()) {
        if (!sinks.containsKey(route.cluster())) {
          EsHandle handle = elasticsearch(route.cluster(), properties, retired);
          sinks.put(route.cluster(), handle.sinks);
          es.add(handle);
        }
      }
      kafka.users++;
      kafka.pipelines.merge(pipeline, 1, Integer::sum);
      es.forEach(handle -> handle.users++);
      context =
          new PipelineRuntime.Context(
              properties,
              kafka.consumerFactories,
              kafka.dlq,
              sinks,
              authChecks.apply(properties),
              settings.sourceOptions(),
              settings.sinkCloseTimeout(),
              settings.clock());
      pipelineOf.put(context, pipeline);
    }
    closeAll(retired);
    return context;
  }

  /**
   * Returns the uses {@link #acquire} took; closes instances no pipeline uses any more. When the
   * pipeline's last use of a {@code DlqProducers} that stays open goes, its writer there is evicted
   * (ADR-0035), so a writer left behind by a reload stops retrying.
   */
  void release(PipelineRuntime.Context context) {
    List<Handle> toClose = new ArrayList<>();
    @Nullable String evict = null;
    DlqProducers evictFrom = context.dlqProducers();
    synchronized (this) {
      String pipeline = pipelineOf.remove(context);
      Handle dlq = handles.get(evictFrom);
      if (pipeline != null
          && dlq instanceof KafkaHandle kafka
          && kafka.pipelines.merge(pipeline, -1, Integer::sum) <= 0) {
        kafka.pipelines.remove(pipeline);
        evict = pipeline;
      }
      unuse(dlq, toClose);
      for (ElasticsearchSinks sinks : context.sinks().values()) {
        unuse(handles.get(sinks), toClose);
      }
    }
    // Outside the monitor, like closeAll: safe because PipelineSupervisor makes every acquire and
    // release under its lifecycle lock, so the pipeline cannot re-acquire this instance meanwhile.
    if (evict != null && toClose.stream().noneMatch(handle -> handle.instance() == evictFrom)) {
      try {
        evictFrom.evict(evict);
      } catch (RuntimeException e) {
        LOG.warn("Dropping the DLQ writer of pipeline '{}' failed", evict, e);
      }
    }
    closeAll(toClose);
  }

  /**
   * Retires current instances whose cluster is gone or defined differently in {@code properties},
   * so no later pipeline gets them; unused ones close now.
   */
  void retireStale(KafkafimProperties properties) {
    List<Handle> toClose = new ArrayList<>();
    synchronized (this) {
      for (KafkaHandle handle : List.copyOf(currentKafka.values())) {
        if (!handle.definition.equals(properties.kafka().clusters().get(handle.cluster))) {
          retire(handle, toClose);
        }
      }
      for (EsHandle handle : List.copyOf(currentEs.values())) {
        if (!handle.definition.equals(properties.elasticsearch().clusters().get(handle.cluster))) {
          retire(handle, toClose);
        }
      }
    }
    closeAll(toClose);
  }

  /** Number of live {@code DlqProducers} and {@code ElasticsearchSinks} instances, for tests. */
  synchronized int instances() {
    return handles.size();
  }

  private KafkaHandle kafka(
      String pipeline, Pipeline config, KafkafimProperties properties, List<Handle> retired) {
    String cluster = config.source().cluster();
    KafkaCluster definition = properties.kafka().clusters().get(cluster);
    KafkaHandle current = currentKafka.get(cluster);
    if (current != null
        && current.definition.equals(definition)
        && Objects.equals(dlqView(current.properties, pipeline), dlqView(properties, pipeline))) {
      return current;
    }
    if (current != null) {
      retire(current, retired);
    }
    KafkaHandle created = new KafkaHandle(cluster, definition, properties);
    currentKafka.put(cluster, created);
    handles.put(created.dlq, created);
    return created;
  }

  private EsHandle elasticsearch(
      String cluster, KafkafimProperties properties, List<Handle> retired) {
    ElasticsearchCluster definition = properties.elasticsearch().clusters().get(cluster);
    EsHandle current = currentEs.get(cluster);
    if (current != null && current.definition.equals(definition)) {
      return current;
    }
    if (current != null) {
      retire(current, retired);
    }
    EsHandle created = new EsHandle(cluster, definition);
    currentEs.put(cluster, created);
    handles.put(created.sinks, created);
    return created;
  }

  /** What a pipeline's {@code DlqWriter} is built from. */
  private static @Nullable List<Object> dlqView(KafkafimProperties properties, String pipeline) {
    Pipeline config = properties.pipelines().get(pipeline);
    return config == null ? null : List.of(config.source().cluster(), config.dlq());
  }

  private void retire(Handle handle, List<Handle> toClose) {
    if (handle.current) {
      handle.current = false;
      if (handle instanceof KafkaHandle kafka) {
        currentKafka.remove(kafka.cluster, kafka);
      } else if (handle instanceof EsHandle es) {
        currentEs.remove(es.cluster, es);
      }
    }
    if (handle.users == 0) {
      forget(handle, toClose);
    }
  }

  private void unuse(@Nullable Handle handle, List<Handle> toClose) {
    if (handle == null) {
      return;
    }
    handle.users--;
    if (handle.users <= 0) {
      retire(handle, toClose);
    }
  }

  private void forget(Handle handle, List<Handle> toClose) {
    if (handles.remove(handle.instance()) != null) {
      toClose.add(handle);
    }
  }

  private static void closeAll(List<Handle> toClose) {
    for (Handle handle : toClose) {
      try {
        LOG.info("Closing {} (no pipeline uses it any more)", handle);
        handle.close();
      } catch (RuntimeException e) {
        LOG.warn("Closing {} failed", handle, e);
      }
    }
  }

  // ---- lifecycle ----

  @Override
  public void start() {
    running = true;
  }

  /** Closes every DLQ producer, after the supervisor (a higher phase) drained the pipelines. */
  @Override
  public void stop() {
    running = false;
    List<KafkaHandle> kafka = new ArrayList<>();
    synchronized (this) {
      for (Handle handle : handles.values()) {
        if (handle instanceof KafkaHandle k) {
          kafka.add(k);
        }
      }
    }
    for (KafkaHandle handle : kafka) {
      handle.dlq.close(); // idempotent
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return DlqProducers.PHASE;
  }

  /** Closes every instance; nothing can be acquired afterwards. */
  @Override
  public void close() {
    List<Handle> all;
    synchronized (this) {
      closed = true;
      all = new ArrayList<>(handles.values());
      handles.clear();
      pipelineOf.clear();
      currentKafka.clear();
      currentEs.clear();
    }
    all.forEach(Handle::close);
  }

  // ---- handles ----

  private abstract static class Handle {
    final String cluster;
    int users; // guarded by the pool
    boolean current = true; // guarded by the pool

    Handle(String cluster) {
      this.cluster = cluster;
    }

    abstract Object instance();

    abstract void close();
  }

  private final class KafkaHandle extends Handle {
    final KafkaCluster definition;
    final KafkafimProperties properties;
    final ConsumerFactories consumerFactories;
    final DlqProducers dlq;
    final Map<String, Integer> pipelines =
        new HashMap<>(); // uses per pipeline, guarded by the pool

    KafkaHandle(String cluster, KafkaCluster definition, KafkafimProperties properties) {
      super(cluster);
      this.definition = definition;
      this.properties = properties;
      this.consumerFactories = new ConsumerFactories(properties);
      AtomicReference<Object> owner = new AtomicReference<>();
      this.dlq = new DlqProducers(properties, relay.dlqListener(owner::get), settings.dlqOptions());
      owner.set(dlq);
    }

    @Override
    Object instance() {
      return dlq;
    }

    @Override
    void close() {
      dlq.close();
    }

    @Override
    public String toString() {
      return "DLQ producers of Kafka cluster '" + cluster + "'";
    }
  }

  private final class EsHandle extends Handle {
    final ElasticsearchCluster definition;
    final ElasticsearchSinks sinks;

    EsHandle(String cluster, ElasticsearchCluster definition) {
      super(cluster);
      this.definition = definition;
      AtomicReference<Object> owner = new AtomicReference<>();
      this.sinks =
          new ElasticsearchSinks(
              Map.of(cluster, definition),
              settings.clientSettings(),
              settings.breakerSettings(),
              relay.sinkListener(owner::get));
      owner.set(sinks);
    }

    @Override
    Object instance() {
      return sinks;
    }

    @Override
    void close() {
      sinks.close();
    }

    @Override
    public String toString() {
      return "Elasticsearch clients of cluster '" + cluster + "'";
    }
  }
}
