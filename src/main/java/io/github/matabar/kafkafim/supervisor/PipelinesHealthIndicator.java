package io.github.matabar.kafkafim.supervisor;

import java.util.Map;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * The {@code pipelines} health contributor, part of the readiness group (DESIGN §4.6, §10): up when
 * at least one pipeline is started (running or paused), so one pipeline in {@code failed_preflight}
 * never takes a pod out of service while its other pipelines work; the alert fires on {@code
 * kafkafim_pipeline_state} instead. Out of service while no pipeline has started yet (preflight
 * running or retrying an unreachable broker) or when every pipeline failed, which also stops a
 * rolling update that would replace working pods with broken ones. Up with zero pipelines
 * configured: an empty group is valid (a startup warning) and has nothing to wait for. Never gated
 * on Elasticsearch health: a breaker or auth pause leaves the pipeline started.
 */
final class PipelinesHealthIndicator implements HealthIndicator {

  private final PipelineSupervisor supervisor;

  PipelinesHealthIndicator(PipelineSupervisor supervisor) {
    this.supervisor = supervisor;
  }

  @Override
  public Health health() {
    Map<String, PipelineState> states = supervisor.states();
    boolean anyStarted = states.values().stream().anyMatch(PipelineState::isStarted);
    Health.Builder health = states.isEmpty() || anyStarted ? Health.up() : Health.outOfService();
    states.forEach((name, state) -> health.withDetail(name, state.label()));
    return health.build();
  }
}
