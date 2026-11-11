package io.github.matabar.kafkafim.supervisor;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@code kafkafim_pipeline_state{pipeline,state}} (DESIGN §4.6, §10): one gauge per pipeline and
 * state, 1 for the pipeline's current state and 0 for the others, so an alert can fire on {@code
 * state=~"failed_preflight|failed"} and a dashboard can sum by state. Pipelines added by a reload
 * get their gauges, removed ones lose them. The rest of the §10 meters are in {@code telemetry},
 * fed by {@link TelemetryListener} and {@link TelemetryBinder}.
 */
final class PipelineStateMetrics implements MeterBinder {

  /** Meter name; Prometheus renders it as {@code kafkafim_pipeline_state}. */
  static final String NAME = "kafkafim.pipeline.state";

  private final PipelineSupervisor supervisor;
  private final List<MeterRegistry> registries = new ArrayList<>(); // guarded by this
  private final Map<String, List<Meter>> meters = new HashMap<>(); // guarded by this

  PipelineStateMetrics(PipelineSupervisor supervisor) {
    this.supervisor = supervisor;
    supervisor.onTopologyChange(this::sync);
  }

  @Override
  public synchronized void bindTo(MeterRegistry registry) {
    registries.add(registry);
    for (String pipeline : supervisor.pipelineNames()) {
      register(registry, pipeline);
    }
  }

  /** Registers gauges for new pipelines and removes those of pipelines that are gone. */
  synchronized void sync() {
    Set<String> names = supervisor.pipelineNames();
    for (var entry : new ArrayList<>(meters.entrySet())) {
      if (!names.contains(entry.getKey())) {
        for (Meter meter : entry.getValue()) {
          for (MeterRegistry registry : registries) {
            registry.remove(meter);
          }
        }
        meters.remove(entry.getKey());
      }
    }
    for (String pipeline : names) {
      if (!meters.containsKey(pipeline)) {
        registries.forEach(registry -> register(registry, pipeline));
      }
    }
  }

  private void register(MeterRegistry registry, String pipeline) {
    List<Meter> registered = meters.computeIfAbsent(pipeline, p -> new ArrayList<>());
    for (PipelineState state : PipelineState.values()) {
      registered.add(
          Gauge.builder(NAME, supervisor, s -> s.stateOrNull(pipeline) == state ? 1 : 0)
              .description("1 for the pipeline's current state, 0 for the others")
              .tag("pipeline", pipeline)
              .tag("state", state.label())
              .register(registry));
    }
  }
}
