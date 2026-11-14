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
    supervisor = new PipelineSupervisor(properties, resources, List.of());
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
}
