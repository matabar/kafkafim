package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Pause reasons of one {@link PipelineRuntime} against a change of its building blocks, without
 * starting it (no broker needed): events of the DLQ producers and registries a reload replaced.
 */
class PipelineRuntimeTest {

  private static final String A =
      """
      a:
        source: { cluster: k, topics: [a] }
        routes: [ { default: true, cluster: es, index: a } ]
      """;

  private final PipelineResources resources =
      new PipelineResources(
          new ListenerRelay(List.of(), List.of()),
          p -> (cluster, identity) -> Preflight.Status.OK,
          PipelineSupervisorTest.SETTINGS);

  /** The context of the startup configuration. */
  private final PipelineRuntime.Context first = resources.acquire("a", TestConfigs.bind(yaml(A)));

  /** A context with other DLQ producers (the Kafka cluster changed) and another es registry. */
  private final PipelineRuntime.Context second =
      resources.acquire(
          "a",
          TestConfigs.bind(
              yaml(A).replace("k1:9092", "k2:9092").replace("password: p", "password: p2")));

  private final PipelineRuntime runtime = new PipelineRuntime("a", first, r -> {});

  @AfterEach
  void close() {
    resources.close();
  }

  private static String yaml(String pipelines) {
    return TestConfigs.CLUSTERS + "  pipelines:\n" + pipelines.indent(4);
  }

  private void dlqEvent(PipelineRuntime.Context from, boolean blocked) {
    runtime.pauseIfOwner(
        r -> r.usesDlq(from.dlqProducers()), PipelineRuntime.DLQ_BLOCKED_REASON, blocked);
  }

  private void breakerEvent(PipelineRuntime.Context from, boolean open) {
    runtime.pauseIfOwner(
        r -> r.usesSinks("es", from.sinks().get("es")),
        PipelineRuntime.BREAKER_REASON_PREFIX + "es",
        open);
  }

  /**
   * The race {@link PipelineRuntime#pauseIfOwner} closes: an old DLQ writer's {@code blocked} event
   * passes the ownership check, the reload reconfigures the pipeline (dropping {@code dlq-blocked}
   * with the old writer), and only then the pause lands, with nothing left to clear it. Check and
   * pause are one step under the runtime's lock, so the reconfigure has to wait for both.
   */
  @Test
  void reconfigureWaitsForAnEventThatPassedTheOwnershipCheck() throws Exception {
    AtomicReference<Thread.State> duringEvent = new AtomicReference<>();
    Thread reconfigurer =
        Thread.ofPlatform().name("test-reconfigure").unstarted(() -> runtime.reconfigure(second));

    runtime.pauseIfOwner(
        r -> {
          final boolean owns = r.usesDlq(first.dlqProducers());
          // The reload gets its chance exactly between the ownership check and the pause.
          reconfigurer.start();
          await()
              .atMost(Duration.ofSeconds(10))
              .until(() ->
                  reconfigurer.getState() == Thread.State.BLOCKED
                      || reconfigurer.getState() == Thread.State.TERMINATED);
          duringEvent.set(reconfigurer.getState());
          return owns;
        },
        PipelineRuntime.DLQ_BLOCKED_REASON,
        true);
    reconfigurer.join(Duration.ofSeconds(10));

    assertThat(duringEvent.get()).isEqualTo(Thread.State.BLOCKED);
    assertThat(reconfigurer.isAlive()).isFalse();
    assertThat(runtime.context()).isSameAs(second);
    // The pause landed first, so the reconfigure dropped it with the old writer.
    assertThat(runtime.pauseReasons()).isEmpty();
  }

  @Test
  void dlqEventsOfTheWriterLeftBehindNeitherPauseNorResume() {
    assertThat(second.dlqProducers()).isNotSameAs(first.dlqProducers());
    dlqEvent(first, true);
    assertThat(runtime.pauseReasons()).containsExactly("dlq-blocked");

    assertThat(runtime.reconfigure(second)).isSameAs(first);
    assertThat(runtime.pauseReasons()).isEmpty();
    dlqEvent(first, true);
    assertThat(runtime.pauseReasons()).isEmpty();

    // The new writer blocks: the old writer's late `unblocked` must not clear that.
    dlqEvent(second, true);
    dlqEvent(first, false);
    assertThat(runtime.pauseReasons()).containsExactly("dlq-blocked");
    dlqEvent(second, false);
    assertThat(runtime.pauseReasons()).isEmpty();
  }

  @Test
  void reconfigureOnTheSameDlqProducersKeepsDlqBlocked() {
    // Only the pipeline definition changes: same Kafka cluster, same DLQ settings, same writer.
    PipelineRuntime.Context sameWriter =
        resources.acquire(
            "a",
            TestConfigs.bind(
                yaml(A.replace("index: a", "index: a2"))
                    .replace("k1:9092", "k2:9092")
                    .replace("password: p", "password: p2")));
    assertThat(sameWriter.dlqProducers()).isSameAs(second.dlqProducers());
    runtime.reconfigure(second);
    dlqEvent(second, true);

    runtime.reconfigure(sameWriter);

    assertThat(runtime.pauseReasons()).containsExactly("dlq-blocked");
  }

  @Test
  void breakerEventsOfTheRegistryLeftBehindNeitherPauseNorResume() {
    assertThat(second.sinks().get("es")).isNotSameAs(first.sinks().get("es"));
    breakerEvent(first, true);
    assertThat(runtime.pauseReasons()).containsExactly("es-breaker:es");

    // Unlike dlq-blocked, a breaker reason outlives the reconfigure; the next start drops it.
    runtime.reconfigure(second);
    assertThat(runtime.pauseReasons()).containsExactly("es-breaker:es");
    // The old registry's breaker closing says nothing about the new one.
    breakerEvent(first, false);
    assertThat(runtime.pauseReasons()).containsExactly("es-breaker:es");
    runtime.resetReasonsForStart();
    assertThat(runtime.pauseReasons()).isEmpty();

    breakerEvent(first, true);
    assertThat(runtime.pauseReasons()).isEmpty();
    breakerEvent(second, true);
    assertThat(runtime.pauseReasons()).containsExactly("es-breaker:es");
    breakerEvent(second, false);
    assertThat(runtime.pauseReasons()).isEmpty();
  }

  @Test
  void anEventWithoutOwnerOrForAnotherClusterIsNotThisPipelines() {
    assertThat(runtime.usesSinks("es", null)).isFalse();
    assertThat(runtime.usesSinks("es2", first.sinks().get("es"))).isFalse();
    assertThat(runtime.usesSinks("es", first.sinks().get("es"))).isTrue();
    assertThat(runtime.usesDlq(null)).isFalse();
    assertThat(runtime.usesDlq(first.dlqProducers())).isTrue();
  }

  @Test
  void pauseReasonDoesNotMakeStoppedPipelinePaused() {
    dlqEvent(first, true);
    assertThat(runtime.state()).isEqualTo(PipelineState.STOPPED);
  }

  @Test
  void reconfigureNeedsContextThatHasThePipeline() {
    PipelineRuntime.Context other =
        resources.acquire("b", TestConfigs.bind(yaml(A.replace("a:", "b:"))));
    assertThatThrownBy(() -> runtime.reconfigure(other))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown pipeline 'a'");
    assertThat(runtime.context()).isSameAs(first);
  }
}
