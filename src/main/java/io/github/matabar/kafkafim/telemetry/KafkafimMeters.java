package io.github.matabar.kafkafim.telemetry;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * The §10 meters of the pod (DESIGN §10): one {@link PipelineMeters} per configured pipeline and
 * one {@code kafkafim_breaker_state} gauge per Elasticsearch cluster the pipelines write to.
 * Consumer lag is not read or exported; it is monitored outside kafkafim (ADR-0034).
 *
 * <p>{@link #sync} is called with the configuration in effect, at startup and on every applied
 * reload (ADR-0028): pipelines that are gone lose their series, new ones gain theirs, and a
 * pipeline whose labels changed (source cluster, topics, routes, Elasticsearch clusters) gets fresh
 * series. Pipelines whose labels stayed keep their counters across a restart.
 */
public final class KafkafimMeters implements AutoCloseable {

  private final MeterRegistry registry;
  private final Map<String, PipelineMeters> pipelines = new ConcurrentHashMap<>();
  private final Map<String, Gauge> breakers = new HashMap<>(); // guarded by this
  private volatile Function<String, CircuitBreaker.@Nullable State> breakerState = cluster -> null;

  /**
   * Creates the meters.
   *
   * @param registry where every series goes
   */
  public KafkafimMeters(MeterRegistry registry) {
    this.registry = registry;
  }

  /** The meters of a pipeline, or {@link PipelineMeters#NOOP} if it is not configured. */
  public PipelineMeters pipeline(String name) {
    return pipelines.getOrDefault(name, PipelineMeters.NOOP);
  }

  /**
   * Brings the series in line with the configuration in effect.
   *
   * @param specs every configured pipeline, by name
   * @param esClusters the Elasticsearch clusters with a breaker gauge (those the pipelines use)
   * @param breakerState the state of a cluster's breaker, or {@code null} when no pipeline sends
   *     through one yet; when several configuration generations' breakers are alive, the worst
   */
  public synchronized void sync(
      Map<String, PipelineSpec> specs,
      Set<String> esClusters,
      Function<String, CircuitBreaker.@Nullable State> breakerState) {
    this.breakerState = breakerState;
    for (String name : List.copyOf(pipelines.keySet())) {
      PipelineSpec spec = specs.get(name);
      PipelineMeters current = pipelines.get(name);
      if (spec == null || !current.spec().sameSeries(spec)) {
        pipelines.remove(name).close();
      }
    }
    specs.forEach((name, spec) -> {
      PipelineMeters current = pipelines.get(name);
      if (current == null) {
        pipelines.put(name, new PipelineMeters(registry, name, spec));
      } else {
        current.probe(spec.probe()); // same series: keep the counters, read the new probe
      }
    });
    for (String cluster : List.copyOf(breakers.keySet())) {
      if (!esClusters.contains(cluster)) {
        registry.remove(breakers.remove(cluster));
      }
    }
    for (String cluster : esClusters) {
      breakers.computeIfAbsent(
          cluster,
          c ->
              Gauge.builder(TelemetryNames.BREAKER_STATE, this, m -> m.breakerValue(c))
                  .description(TelemetryNames.description(TelemetryNames.BREAKER_STATE))
                  .tag(TelemetryNames.ES_CLUSTER, c)
                  .register(registry));
    }
  }

  private double breakerValue(String cluster) {
    CircuitBreaker.State state = breakerState.apply(cluster);
    return state == null ? 0 : breakerValue(state);
  }

  /** {@code kafkafim_breaker_state}: 0 closed (or disabled), 1 half-open, 2 open. */
  public static int breakerValue(CircuitBreaker.State state) {
    return switch (state) {
      case OPEN, FORCED_OPEN -> 2;
      case HALF_OPEN -> 1;
      default -> 0;
    };
  }

  /** Removes every series. */
  @Override
  public synchronized void close() {
    pipelines.values().forEach(PipelineMeters::close);
    pipelines.clear();
    breakers.values().forEach(registry::remove);
    breakers.clear();
  }
}
