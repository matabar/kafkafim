package io.github.matabar.kafkafim.supervisor;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * {@code kafkafim_pipeline_state{pipeline,state}} (DESIGN §4.6, §10): one gauge per pipeline and
 * state, 1 for the pipeline's current state and 0 for the others, so an alert can fire on {@code
 * state="failed_preflight"} and a dashboard can sum by state. The rest of the §10 meters belong to
 * the telemetry slice ({@link SupervisorListener} and the sink and DLQ listeners are its hooks).
 */
final class PipelineStateMetrics implements MeterBinder {

  /** Meter name; Prometheus renders it as {@code kafkafim_pipeline_state}. */
  static final String NAME = "kafkafim.pipeline.state";

  private final PipelineSupervisor supervisor;

  PipelineStateMetrics(PipelineSupervisor supervisor) {
    this.supervisor = supervisor;
  }

  @Override
  public void bindTo(MeterRegistry registry) {
    for (String pipeline : supervisor.pipelineNames()) {
      for (PipelineState state : PipelineState.values()) {
        Gauge.builder(NAME, supervisor, s -> s.state(pipeline) == state ? 1 : 0)
            .description("1 for the pipeline's current state, 0 for the others")
            .tag("pipeline", pipeline)
            .tag("state", state.label())
            .register(registry);
      }
    }
  }
}
