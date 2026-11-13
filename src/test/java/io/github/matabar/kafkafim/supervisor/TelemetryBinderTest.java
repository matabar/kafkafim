package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
import io.github.matabar.kafkafim.sink.es.SinkKey;
import io.github.matabar.kafkafim.telemetry.BatchTracer;
import io.github.matabar.kafkafim.telemetry.KafkafimMeters;
import io.github.matabar.kafkafim.telemetry.PauseReason;
import io.github.matabar.kafkafim.telemetry.TelemetryNames;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The series {@link TelemetryBinder} registers from a bound configuration, without starting any
 * pipeline (no broker needed).
 */
class TelemetryBinderTest {

  /** {@code es} has {@code default-identity: writer}; {@code es2} is unauthenticated. */
  private final KafkafimProperties properties =
      TestConfigs.withPipelines(
          """
          orders:
            source: { cluster: k, topics: [a] }
            route-mode: all
            routes:
              - { cluster: es, index: a }
              - { name: audit, cluster: es2, index: a }
              - { name: noise, when: { topic: a }, drop: true }
          """);

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final KafkafimMeters meters = new KafkafimMeters(registry);
  private final PipelineResources resources =
      new PipelineResources(
          new ListenerRelay(List.of(), List.of()),
          p -> (cluster, identity) -> Preflight.Status.OK,
          PipelineSupervisorTest.SETTINGS);
  private final PipelineSupervisor supervisor =
      new PipelineSupervisor(properties, resources, List.of());

  @AfterEach
  void close() {
    resources.close();
  }

  private List<String> authFailureSeries() {
    return registry.find(TelemetryNames.AUTH_FAILURES).counters().stream()
        .map(c -> c.getId().getTag("es_cluster") + "/" + c.getId().getTag("identity"))
        .toList();
  }

  @Test
  void registersAuthFailureSeriesUnderTheClustersDefaultIdentity() {
    new TelemetryBinder(supervisor, meters);

    // The first route names no identity: the series carries the cluster's default-identity.
    assertThat(authFailureSeries()).containsExactlyInAnyOrder("es/writer", "es2/none");

    // An auth failure reported under the sink's key counts on the series registered at startup.
    Pipeline.Route route = properties.pipelines().get("orders").routes().get(0);
    assertThat(route.identity()).isEqualTo("writer");
    SinkKey key = new SinkKey("orders", route.cluster(), route.identity());
    new TelemetryListener(meters, new BatchTracer(Tracer.NOOP)).onAuthFailure(key, 401, "{}");

    assertThat(authFailureSeries()).containsExactlyInAnyOrder("es/writer", "es2/none");
    Counter writer =
        registry
            .find(TelemetryNames.AUTH_FAILURES)
            .tags("pipeline", "orders", "es_cluster", "es", "identity", "writer")
            .counter();
    assertThat(writer).isNotNull();
    assertThat(writer.count()).isOne();
  }

  @Test
  void registersDocSeriesUnderEachRoutesLabel() {
    new TelemetryBinder(supervisor, meters);

    List<Pipeline.Route> routes = properties.pipelines().get("orders").routes();
    assertThat(
            registry.find(TelemetryNames.DOCS).tag("outcome", "indexed").counters().stream()
                .map(c -> c.getId().getTag("route"))
                .toList())
        .containsExactlyInAnyOrder(
            routes.get(0).label(0), routes.get(1).label(1), routes.get(2).label(2))
        .containsExactlyInAnyOrder("0", "audit", "noise");
  }

  @Test
  void pauseReasonKindsDropWhatTheReasonNames() {
    Set<String> reasons =
        Set.of(
            PipelineRuntime.BREAKER_REASON_PREFIX + "es",
            PipelineRuntime.BREAKER_REASON_PREFIX + "es2",
            PipelineRuntime.AUTH_REASON_PREFIX + "es/writer",
            PipelineRuntime.DLQ_BLOCKED_REASON);

    assertThat(TelemetryBinder.kinds(PipelineState.PAUSED, reasons))
        .containsExactlyInAnyOrder(PauseReason.values());
    assertThat(
            TelemetryBinder.kinds(
                PipelineState.PAUSED, Set.of(PipelineRuntime.AUTH_REASON_PREFIX + "es/none")))
        .containsExactly(PauseReason.ES_AUTH);
    assertThat(TelemetryBinder.kinds(PipelineState.PAUSED, Set.of())).isEmpty();
  }

  @Test
  void onlyPausedPipelinesHavePauseReasonKinds() {
    Set<String> reasons = Set.of(PipelineRuntime.DLQ_BLOCKED_REASON);
    for (PipelineState state : PipelineState.values()) {
      if (state != PipelineState.PAUSED) {
        assertThat(TelemetryBinder.kinds(state, reasons)).as(state.name()).isEmpty();
      }
    }
  }

  @Test
  void reasonHeldWhileThePipelineIsNotRunningReadsZero() {
    ListenerRelay relay = new ListenerRelay(List.of(), List.of());
    PipelineResources held =
        new PipelineResources(
            relay,
            p -> (cluster, identity) -> Preflight.Status.OK,
            PipelineSupervisorTest.SETTINGS);
    try {
      PipelineSupervisor idle = new PipelineSupervisor(properties, held, List.of());
      relay.attach(idle);
      new TelemetryBinder(idle, meters);
      Object owner = idle.runtimes().stream().findFirst().orElseThrow().context().dlqProducers();
      relay.dlqListener(() -> owner).onBlocked("orders", null);
      assertThat(idle.pauseReasons("orders")).containsExactly(PipelineRuntime.DLQ_BLOCKED_REASON);
      assertThat(idle.state("orders")).isNotEqualTo(PipelineState.PAUSED);

      assertThat(
              registry.find(TelemetryNames.PAUSE_REASON).gauges().stream()
                  .map(g -> g.getId().getTag("reason") + "=" + (int) g.value())
                  .toList())
          .containsExactlyInAnyOrder("es_breaker=0", "es_auth=0", "dlq_blocked=0");
    } finally {
      held.close();
    }
  }
}
