package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.dlq.DlqRequest;
import io.github.matabar.kafkafim.dlq.DlqWriter;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Which DLQ writers {@link PipelineResources} evicts (ADR-0035). The bootstrap address resolves, so
 * writers can be created; nothing is ever sent to it.
 */
class PipelineResourcesTest {

  private static final String A =
      """
      a:
        source: { cluster: k, topics: [a] }
        routes: [ { default: true, cluster: es, index: a } ]
      """;
  private static final String MOVED_A =
      A.replace("  routes:", "  dlq: { topic: a.dead }\n  routes:");
  private static final String B =
      """
      b:
        source: { cluster: k, topics: [b] }
        routes: [ { default: true, cluster: es, index: b } ]
      """;

  private static final String C =
      """
      c:
        source: { cluster: k, topics: [c] }
        routes:
          - { when: [ { field: /kind, equals: x } ], cluster: es2, index: c }
          - { default: true, cluster: es, index: c }
      """;

  private final PipelineResources resources =
      new PipelineResources(
          new ListenerRelay(List.of(), List.of()),
          p -> (cluster, identity) -> Preflight.Status.OK,
          PipelineSupervisorTest.SETTINGS);

  @AfterEach
  void close() {
    resources.stop();
    resources.close();
  }

  private static KafkafimProperties config(String pipelines) {
    return TestConfigs.bind(yaml(pipelines));
  }

  /** The same tree with another password for identity {@code writer} of {@code es}. */
  private static KafkafimProperties configNewPassword(String pipelines) {
    return TestConfigs.bind(yaml(pipelines).replace("password: p", "password: p2"));
  }

  private static String yaml(String pipelines) {
    return (TestConfigs.CLUSTERS + "  pipelines:\n" + pipelines.indent(4))
        .replace("k1:9092", "localhost:9");
  }

  @Test
  void pipelineLeavingDlqProducersStillInUseHasItsWriterEvicted() {
    KafkafimProperties first = config(A + B);
    final PipelineRuntime.Context a = resources.acquire("a", first);
    final PipelineRuntime.Context b = resources.acquire("b", first);
    final DlqWriter writerOfA = a.dlqProducers().writer("a");
    final DlqWriter writerOfB = b.dlqProducers().writer("b");

    // a's DLQ topic changes: a moves to new DLQ producers, b keeps the old ones.
    PipelineRuntime.Context moved = resources.acquire("a", config(MOVED_A + B));
    resources.release(a);

    assertThat(moved.dlqProducers()).isNotSameAs(a.dlqProducers());
    assertThat(writerOfA.send(request())).isCompletedExceptionally();
    assertThat(a.dlqProducers().writer("a")).isNotSameAs(writerOfA);
    assertThat(b.dlqProducers().writer("b")).isSameAs(writerOfB);
    assertThat(moved.dlqProducers().writer("a").topic()).isEqualTo("a.dead");

    // b was the old producers' last user: they close.
    resources.release(b);
    assertThatThrownBy(() -> a.dlqProducers().writer("b"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void pipelineRestartedOnTheSameDlqProducersKeepsItsWriter() {
    KafkafimProperties first = config(A + B);
    final PipelineRuntime.Context a = resources.acquire("a", first);
    resources.acquire("b", first);
    final DlqWriter writerOfA = a.dlqProducers().writer("a");

    // An index change restarts a; its DLQ settings and cluster are unchanged.
    PipelineRuntime.Context restarted =
        resources.acquire("a", config(A.replace("index: a", "index: a2") + B));
    resources.release(a);

    assertThat(restarted.dlqProducers()).isSameAs(a.dlqProducers());
    assertThat(restarted.dlqProducers().writer("a")).isSameAs(writerOfA);
  }

  @Test
  void removedPipelineHasItsWriterEvicted() {
    KafkafimProperties first = config(A + B);
    final PipelineRuntime.Context a = resources.acquire("a", first);
    final PipelineRuntime.Context b = resources.acquire("b", first);
    final DlqWriter writerOfA = a.dlqProducers().writer("a");

    resources.release(a);

    assertThat(writerOfA.send(request())).isCompletedExceptionally();
    assertThat(b.dlqProducers().writer("b").isBlocked()).isFalse();
  }

  @Test
  void failedAcquireTakesNothingAndLeavesTheCurrentInstancesCurrent() {
    KafkafimProperties first = config(A);
    KafkafimProperties failing = configNewPassword(A + C);
    PipelineResources pool =
        new PipelineResources(
            new ListenerRelay(List.of(), List.of()),
            p -> {
              if (p == failing) {
                throw new IllegalStateException("no client today");
              }
              return (cluster, identity) -> Preflight.Status.OK;
            },
            PipelineSupervisorTest.SETTINGS);
    try {
      final PipelineRuntime.Context a = pool.acquire("a", first);
      assertThat(pool.instances()).isEqualTo(2);

      // c is new to the DLQ producers of k and routes to es and es2; es is defined differently.
      // Its acquire creates new DLQ producers and two registries before it fails.
      assertThatThrownBy(() -> pool.acquire("c", failing))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("no client today");

      assertThat(pool.instances()).isEqualTo(2);
      PipelineRuntime.Context again = pool.acquire("a", first);
      assertThat(again.dlqProducers()).isSameAs(a.dlqProducers());
      assertThat(again.sinks().get("es")).isSameAs(a.sinks().get("es"));
    } finally {
      pool.stop();
      pool.close();
    }
  }

  @Test
  void rollbackReleasesTheContextsAndMakesTheCheckpointsInstancesCurrentAgain() {
    KafkafimProperties first = config(A + B);
    final PipelineRuntime.Context a = resources.acquire("a", first);
    final PipelineRuntime.Context b = resources.acquire("b", first);
    final DlqWriter writerOfA = a.dlqProducers().writer("a");

    // A reload moves a to new DLQ producers and both to a new registry of es, then fails.
    PipelineResources.Checkpoint checkpoint = resources.checkpoint();
    KafkafimProperties next = configNewPassword(MOVED_A + B);
    PipelineRuntime.Context movedA = resources.acquire("a", next);
    PipelineRuntime.Context movedB = resources.acquire("b", next);
    assertThat(movedA.dlqProducers()).isNotSameAs(a.dlqProducers());
    assertThat(resources.instances()).isEqualTo(4);

    resources.rollback(checkpoint, List.of(movedA, movedB));

    assertThat(resources.instances()).isEqualTo(2);
    // a never left its DLQ producers: its writer there is kept (ADR-0035).
    assertThat(a.dlqProducers().writer("a")).isSameAs(writerOfA);
    // The instances of the running tree are the current ones again.
    PipelineRuntime.Context again = resources.acquire("b", first);
    assertThat(again.dlqProducers()).isSameAs(b.dlqProducers());
    assertThat(again.sinks().get("es")).isSameAs(b.sinks().get("es"));
    assertThat(resources.instances()).isEqualTo(2);
  }

  private static DlqRequest request() {
    return new DlqRequest(
        "a",
        0,
        5,
        null,
        new byte[] {1},
        new byte[] {2},
        List.of(),
        "decode",
        "bad json",
        null,
        null,
        1,
        Instant.now(),
        null);
  }
}
