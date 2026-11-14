package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.config.ConfigDiff;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.dlq.DlqRequest;
import io.github.matabar.kafkafim.dlq.DlqWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A {@link PipelineSupervisor#apply} that fails is undone as a whole (ADR-0041, #161), without
 * brokers. The tests make it fail through the auth check factory, which {@link
 * PipelineResources#acquire} calls for every context it hands out: by throwing there, or by
 * starting a drained pipeline so that its reconfigure fails.
 */
class PipelineSupervisorApplyRollbackTest {

  private static final String A =
      """
      a:
        source: { cluster: k, topics: [a] }
        routes: [ { default: true, cluster: es, index: a } ]
      """;
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
        routes: [ { default: true, cluster: es2, index: c } ]
      """;

  /** Runs whenever a context is acquired, with the configuration it is acquired for. */
  private volatile Consumer<KafkafimProperties> onAcquire = properties -> {};

  /** Runs when a pipeline's pause reasons change, with its lock held. */
  private volatile Consumer<String> onReasons = pipeline -> {};

  private final ListenerRelay relay = new ListenerRelay(List.of(), List.of());
  private final PipelineResources resources =
      new PipelineResources(
          relay,
          properties -> {
            onAcquire.accept(properties);
            return (cluster, identity) -> Preflight.Status.OK;
          },
          PipelineSupervisorTest.FAST_SETTINGS);
  private PipelineSupervisor supervisor;

  @AfterEach
  void close() {
    if (supervisor != null) {
      supervisor.stop();
    }
    resources.close();
  }

  private PipelineSupervisor start(KafkafimProperties properties) {
    supervisor =
        new PipelineSupervisor(
            properties,
            resources,
            List.of(
                new SupervisorListener() {
                  @Override
                  public void onPauseReasonsChange(String pipeline, Set<String> reasons) {
                    onReasons.accept(pipeline);
                  }
                }));
    relay.attach(supervisor);
    supervisor.start();
    return supervisor;
  }

  /**
   * {@link TestConfigs#CLUSTERS} with the given pipelines, on a loopback address that refuses
   * connections: a started pipeline stays in {@code starting}, retrying its preflight.
   */
  private static KafkafimProperties deadBroker(String pipelines) {
    return TestConfigs.bind(
        (TestConfigs.CLUSTERS + "  pipelines:\n" + pipelines.indent(4))
            .replace("k1:9092", "127.0.0.1:9"));
  }

  /** The same tree with another password for identity {@code writer} of {@code es}. */
  private static KafkafimProperties deadBrokerNewPassword(String pipelines) {
    return TestConfigs.bind(
        (TestConfigs.CLUSTERS + "  pipelines:\n" + pipelines.indent(4))
            .replace("k1:9092", "127.0.0.1:9")
            .replace("password: p", "password: p2"));
  }

  private PipelineRuntime runtime(String pipeline) {
    return supervisor.runtimes().stream()
        .filter(r -> r.name().equals(pipeline))
        .findFirst()
        .orElseThrow();
  }

  private PipelineRuntime.Context context(String pipeline) {
    return supervisor.runtimes().stream()
        .filter(r -> r.name().equals(pipeline))
        .findFirst()
        .orElseThrow()
        .context();
  }

  /** Makes the {@code nth} acquire for {@code next} throw. */
  private void failAcquire(KafkafimProperties next, int nth) {
    AtomicInteger acquires = new AtomicInteger();
    onAcquire =
        properties -> {
          if (properties == next && acquires.incrementAndGet() == nth) {
            throw new IllegalStateException("no client today");
          }
        };
  }

  @Test
  void failedAcquireReleasesWhatWasAcquiredAndStartsTheDrainedPipelinesAgain() {
    start(deadBroker(A + B));
    supervisor.stopPipeline("b");
    final KafkafimProperties running = supervisor.properties();
    final PipelineRuntime.Context a = context("a");
    final PipelineRuntime.Context b = context("b");
    // The DLQ producers of Kafka cluster k and the registry of es.
    assertThat(resources.instances()).isEqualTo(2);

    // c is added first; a and b restart on a new registry for es (identity `writer` changed).
    KafkafimProperties next = deadBrokerNewPassword(C + A + B);
    ConfigDiff diff = ConfigDiff.between(running, next);
    assertThat(diff.added()).containsExactly("c");
    assertThat(diff.restarted()).containsOnlyKeys("a", "b");
    failAcquire(next, 3); // c and a get their contexts, b does not

    assertThatThrownBy(() -> supervisor.apply(next, diff))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("no client today");

    // Nothing of the tree took effect.
    assertThat(supervisor.properties()).isSameAs(running);
    assertThat(supervisor.pipelineNames()).containsExactly("a", "b");
    assertThat(context("a")).isSameAs(a);
    assertThat(context("b")).isSameAs(b);
    // What was acquired for c and a went back: not the new DLQ producers of k (c is new to
    // them), the new registry of es or the registry of es2.
    assertThat(resources.instances()).isEqualTo(2);
    // a was starting and is again; b was stopped by hand and stays so.
    assertThat(supervisor.state("a")).isEqualTo(PipelineState.STARTING);
    assertThat(supervisor.state("b")).isEqualTo(PipelineState.STOPPED);
  }

  @Test
  void failedApplyKeepsTheRunningConfigurationsInstancesCurrent() {
    start(deadBroker(A));
    final KafkafimProperties running = supervisor.properties();
    final PipelineRuntime.Context before = context("a");

    // c is new to the DLQ producers of k and a's registry of es changes: both get new instances.
    KafkafimProperties failing = deadBrokerNewPassword(A + C);
    failAcquire(failing, 2);
    assertThatThrownBy(() -> supervisor.apply(failing, ConfigDiff.between(running, failing)))
        .isInstanceOf(IllegalStateException.class);
    assertThat(resources.instances()).isEqualTo(2);

    // a restarts for a change of its own: its clusters are as the running tree defines them, so
    // it keeps the DLQ producers and the registry it has.
    onAcquire = properties -> {};
    KafkafimProperties next = deadBroker(A.replace("index: a", "index: a2"));
    supervisor.apply(next, ConfigDiff.between(running, next));

    assertThat(context("a")).isNotSameAs(before);
    assertThat(context("a").dlqProducers()).isSameAs(before.dlqProducers());
    assertThat(context("a").sinks().get("es")).isSameAs(before.sinks().get("es"));
    assertThat(resources.instances()).isEqualTo(2);
  }

  @Test
  void failedReconfigureGivesReconfiguredPipelinesTheirContextBack() {
    start(deadBroker(B + A));
    final KafkafimProperties running = supervisor.properties();
    final PipelineRuntime.Context a = context("a");
    final PipelineRuntime.Context b = context("b");

    // c is added; b and a restart, in that order. a is started while the contexts are acquired
    // (startPipeline does not take the lifecycle lock), so b is reconfigured and a then is not.
    KafkafimProperties next = deadBrokerNewPassword(C + B + A);
    ConfigDiff diff = ConfigDiff.between(running, next);
    assertThat(diff.restarted()).containsOnlyKeys("a", "b");
    AtomicInteger acquires = new AtomicInteger();
    onAcquire =
        properties -> {
          if (properties == next && acquires.incrementAndGet() == 1) {
            supervisor.startPipeline("a");
          }
        };

    assertThatThrownBy(() -> supervisor.apply(next, diff))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'a' must be stopped to reconfigure");

    assertThat(supervisor.properties()).isSameAs(running);
    assertThat(supervisor.pipelineNames()).containsExactly("b", "a");
    assertThat(context("a")).isSameAs(a);
    assertThat(context("b")).isSameAs(b);
    assertThat(resources.instances()).isEqualTo(2);
    assertThat(supervisor.states().values()).containsOnly(PipelineState.STARTING);
  }

  /**
   * A rollback restores {@code dlq-blocked}: b moved to new DLQ producers, a then failed to
   * reconfigure, and b's previous writer is blocked. The rollback gives b that writer back with its
   * pause; the writer reports only the change to blocked (#207).
   */
  @Test
  void failedReconfigureRestoresDlqBlockedOfTheWriterItGivesBack() {
    start(deadBroker(B + A));
    final KafkafimProperties running = supervisor.properties();
    final DlqWriter writer = context("b").dlqProducers().writer("b");
    writer.send(
        new DlqRequest(
            "b",
            0,
            0,
            null,
            null,
            null,
            List.of(),
            "test",
            null,
            null,
            null,
            1,
            Instant.now(),
            null));
    await().atMost(Duration.ofSeconds(60)).until(writer::isBlocked);
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> runtime("b").pauseReasons().contains(PipelineRuntime.DLQ_BLOCKED_REASON));

    KafkafimProperties next = deadBrokerNewPassword(C + B + A);
    ConfigDiff diff = ConfigDiff.between(running, next);
    AtomicInteger acquires = new AtomicInteger();
    onAcquire =
        properties -> {
          if (properties == next && acquires.incrementAndGet() == 1) {
            supervisor.startPipeline("a");
          }
        };

    assertThatThrownBy(() -> supervisor.apply(next, diff))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'a' must be stopped to reconfigure");

    assertThat(context("b").dlqProducers().writer("b")).isSameAs(writer);
    assertThat(runtime("b").pauseReasons()).contains(PipelineRuntime.DLQ_BLOCKED_REASON);
  }

  private PipelineRuntime runtime(String pipeline) {
    return supervisor.runtimes().stream()
        .filter(r -> r.name().equals(pipeline))
        .findFirst()
        .orElseThrow();
  }

  @Test
  void failedApplyStartsPipelinesTheTreeRemovesAgain() {
    start(deadBroker(A + B));
    final KafkafimProperties running = supervisor.properties();
    final PipelineRuntime.Context b = context("b");

    // b is removed, c is added, and c's context cannot be acquired.
    KafkafimProperties next = deadBroker(A + C);
    ConfigDiff diff = ConfigDiff.between(running, next);
    assertThat(diff.removed()).containsExactly("b");
    failAcquire(next, 1);

    assertThatThrownBy(() -> supervisor.apply(next, diff))
        .isInstanceOf(IllegalStateException.class);

    assertThat(supervisor.pipelineNames()).containsExactly("a", "b");
    assertThat(context("b")).isSameAs(b);
    assertThat(supervisor.state("b")).isEqualTo(PipelineState.STARTING);
    assertThat(resources.instances()).isEqualTo(2);
  }

  /** The shutdown began during the failing apply: nothing starts again, and the error stays. */
  @Test
  void shutdownDuringFailingApplyRestartsNothing() {
    start(deadBroker(A + B));
    final KafkafimProperties running = supervisor.properties();
    final KafkafimProperties next = deadBrokerNewPassword(A + B);
    onAcquire =
        properties -> {
          if (properties == next) {
            supervisor.stop();
            throw new IllegalStateException("no client today");
          }
        };

    assertThatThrownBy(() -> supervisor.apply(next, ConfigDiff.between(running, next)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("no client today");

    assertThat(supervisor.isRunning()).isFalse();
    assertThat(supervisor.states().values()).containsOnly(PipelineState.STOPPED);
    assertThat(supervisor.properties()).isSameAs(running);
  }

  /** A pipeline in {@code failed_preflight} is not among those started again. */
  @Test
  void failedApplyLeavesFailedPreflightPipelineFailed() {
    start(
        TestConfigs.bind(
            (TestConfigs.CLUSTERS + "  pipelines:\n" + (A + B).indent(4))
                .replace("k1:9092", "127.0.0.1:9")
                .replace(
                    "[127.0.0.1:9] }", "[127.0.0.1:9], properties: { max.poll.records: 5 } }")));
    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> supervisor.state("a") == PipelineState.FAILED_PREFLIGHT);
    final String failure = supervisor.failure("a");
    final KafkafimProperties running = supervisor.properties();
    final KafkafimProperties next =
        TestConfigs.bind(
            (TestConfigs.CLUSTERS
                    + "  pipelines:\n"
                    + (A.replace("index: a", "index: a2") + B).indent(4))
                .replace("k1:9092", "127.0.0.1:9")
                .replace(
                    "[127.0.0.1:9] }", "[127.0.0.1:9], properties: { max.poll.records: 5 } }"));
    ConfigDiff diff = ConfigDiff.between(running, next);
    assertThat(diff.restarted()).containsOnlyKeys("a");
    failAcquire(next, 1);

    assertThatThrownBy(() -> supervisor.apply(next, diff))
        .isInstanceOf(IllegalStateException.class);

    assertThat(supervisor.state("a")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
    assertThat(supervisor.failure("a")).isEqualTo(failure);
    assertThat(supervisor.properties()).isSameAs(running);
  }

  /** An {@link Error} while a client is built is rolled back too, and rethrown as it is. */
  @Test
  void errorWhileAcquiringIsRolledBackAndRethrown() {
    start(deadBroker(A));
    final KafkafimProperties running = supervisor.properties();
    final PipelineRuntime.Context before = context("a");
    final KafkafimProperties next = deadBrokerNewPassword(A + C);
    final NoClassDefFoundError error = new NoClassDefFoundError("some/Client");
    onAcquire =
        properties -> {
          if (properties == next) {
            throw error;
          }
        };

    assertThatThrownBy(() -> supervisor.apply(next, ConfigDiff.between(running, next)))
        .isSameAs(error);

    assertThat(supervisor.properties()).isSameAs(running);
    assertThat(context("a")).isSameAs(before);
    assertThat(resources.instances()).isEqualTo(2);
    assertThat(supervisor.state("a")).isEqualTo(PipelineState.STARTING);
  }

  /**
   * The catch branch of the rollback: b is started again while its reconfigure is reported (its
   * {@code dlq-blocked} goes with the writer), so the rollback cannot give it its context back. It
   * keeps the new one, which stays in use, and the rest of the rollback goes on.
   */
  @Test
  void pipelineStartedWhileTheRollbackGivesItsContextBackKeepsTheNewContext() {
    start(deadBroker(B + A));
    final KafkafimProperties running = supervisor.properties();
    final PipelineRuntime.Context a = context("a");
    final DlqWriter writer = context("b").dlqProducers().writer("b");
    writer.send(
        new DlqRequest(
            "b",
            0,
            0,
            null,
            null,
            null,
            List.of(),
            "test",
            null,
            null,
            null,
            1,
            Instant.now(),
            null));
    await().atMost(Duration.ofSeconds(60)).until(writer::isBlocked);
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> runtime("b").pauseReasons().contains(PipelineRuntime.DLQ_BLOCKED_REASON));
    supervisor.stopPipeline("b");

    final KafkafimProperties next = deadBrokerNewPassword(C + B + A);
    ConfigDiff diff = ConfigDiff.between(running, next);
    AtomicInteger acquires = new AtomicInteger();
    onAcquire =
        properties -> {
          if (properties == next && acquires.incrementAndGet() == 1) {
            supervisor.startPipeline("a"); // so that a's reconfigure fails after b's
          }
        };
    // The new writer of b has failed nothing: reconfigure drops dlq-blocked and reports it.
    onReasons =
        pipeline -> {
          if (pipeline.equals("b") && !runtime("b").pauseReasons().contains("dlq-blocked")) {
            onReasons = ignored -> {};
            runtime("b").start();
          }
        };

    assertThatThrownBy(() -> supervisor.apply(next, diff))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'a' must be stopped to reconfigure");

    assertThat(supervisor.properties()).isSameAs(running);
    assertThat(context("a")).isSameAs(a);
    assertThat(context("b").properties()).isSameAs(next); // kept the new context
    assertThat(context("b").dlqProducers().writer("b")).isNotSameAs(writer);
    // The new context's instances stay; the rest went back: DLQ producers of k (b's new ones),
    // the registry of es (b's new one), and a's previous ones, which a is using again.
    assertThat(resources.instances()).isEqualTo(4);
  }
}
