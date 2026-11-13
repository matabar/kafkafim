package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.config.ConfigDiff;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * What {@link PipelineSupervisor#apply} does when a reload does not go to plan (DESIGN §5.6),
 * without brokers: a reconfigure that fails, and a shutdown that begins while the reload is being
 * applied. The tests step in between the drains and the reconfigure through the auth check factory,
 * which {@link PipelineResources#acquire} calls for every context it hands out.
 */
class PipelineSupervisorReloadFailureTest {

  private static final String A =
      """
      a:
        source: { cluster: k, topics: [a] }
        routes: [ { default: true, cluster: es, index: a } ]
      """;
  private static final String C =
      """
      c:
        source: { cluster: k, topics: [c] }
        routes: [ { default: true, cluster: es2, index: c } ]
      """;
  private static final String D =
      """
      d:
        source: { cluster: k, topics: [d] }
        routes: [ { default: true, cluster: es, index: d } ]
      """;

  /** Runs whenever a context is acquired, with the configuration it is acquired for. */
  private volatile Consumer<KafkafimProperties> onAcquire = properties -> {};

  /** Every state change, as {@code pipeline=STATE}. */
  private final List<String> transitions = new CopyOnWriteArrayList<>();

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
                  public void onStateChange(String pipeline, PipelineState from, PipelineState to) {
                    transitions.add(pipeline + "=" + to);
                  }
                }));
    relay.attach(supervisor);
    supervisor.start();
    return supervisor;
  }

  /**
   * {@link TestConfigs#CLUSTERS} with the given pipelines, as YAML, on a Kafka address that
   * resolves and refuses connections: a started pipeline stays in {@code starting}, retrying its
   * preflight.
   */
  private static String deadBrokerYaml(String pipelines) {
    return (TestConfigs.CLUSTERS + "  pipelines:\n" + pipelines.indent(4))
        .replace("k1:9092", "127.0.0.1:9");
  }

  private PipelineRuntime.Context context(String pipeline) {
    return supervisor.runtimes().stream()
        .filter(r -> r.name().equals(pipeline))
        .findFirst()
        .orElseThrow()
        .context();
  }

  @Test
  void failedReconfigureReleasesWhatItAcquiredAndTheNextApplySucceeds() {
    start(TestConfigs.bind(deadBrokerYaml(A)));
    final PipelineRuntime.Context before = context("a");
    final KafkafimProperties running = supervisor.properties();
    // The DLQ producers of Kafka cluster k and the registry of es.
    assertThat(resources.instances()).isEqualTo(2);

    // Identity `writer` of es changes: a restarts on a new registry for es.
    KafkafimProperties next =
        TestConfigs.bind(deadBrokerYaml(A).replace("password: p", "password: p2"));
    ConfigDiff diff = ConfigDiff.between(running, next);
    assertThat(diff.restarted()).containsOnlyKeys("a");
    // startPipeline does not take the lifecycle lock: a start that lands after the drain, while the
    // new context is being acquired, leaves a in `starting` when apply reconfigures it.
    onAcquire =
        properties -> {
          if (properties == next) {
            supervisor.startPipeline("a");
          }
        };

    assertThatThrownBy(() -> supervisor.apply(next, diff))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'a' must be stopped to reconfigure");

    // Nothing of the tree took effect, and the registry acquired for it was released and closed:
    // what is left is what a still runs with.
    assertThat(supervisor.properties()).isSameAs(running);
    assertThat(supervisor.pipelineNames()).containsExactly("a");
    assertThat(context("a")).isSameAs(before);
    assertThat(resources.instances()).isEqualTo(2);
    assertThat(supervisor.state("a")).isEqualTo(PipelineState.STARTING);

    // The same tree applies once nothing interferes.
    onAcquire = properties -> {};
    supervisor.apply(next, diff);
    assertThat(supervisor.properties()).isSameAs(next);
    assertThat(context("a").properties()).isSameAs(next);
    assertThat(context("a").sinks().get("es")).isNotSameAs(before.sinks().get("es"));
    assertThat(context("a").dlqProducers()).isSameAs(before.dlqProducers());
    // The registry a left was its alone: closed.
    assertThat(resources.instances()).isEqualTo(2);
  }

  @Test
  void shutdownDuringApplyAppliesTheTreeButStartsNothing() throws Exception {
    // Kafka cluster k does not resolve here: every start fails preflight at once.
    start(TestConfigs.withPipelines(A + C));
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> !supervisor.states().containsValue(PipelineState.STARTING));
    assertThat(supervisor.states().values()).containsOnly(PipelineState.FAILED_PREFLIGHT);
    transitions.clear();

    // a is unchanged and failed (it would be retried), c restarts, d is added.
    KafkafimProperties next = TestConfigs.withPipelines(A + C.replace("index: c", "index: c2") + D);
    ConfigDiff diff = ConfigDiff.between(supervisor.properties(), next);
    final PipelineRuntime.Context c = context("c");
    AtomicReference<Thread> stopper = new AtomicReference<>();
    onAcquire =
        properties -> {
          if (properties == next && stopper.get() == null) {
            // stop() marks the supervisor as not running, then waits for apply's lifecycle lock.
            stopper.set(Thread.ofPlatform().name("test-shutdown").start(supervisor::stop));
            await().atMost(Duration.ofSeconds(10)).until(() -> !supervisor.isRunning());
          }
        };

    List<String> retried = supervisor.apply(next, diff);

    assertThat(retried).isEmpty();
    assertThat(stopper.get()).isNotNull();
    stopper.get().join(Duration.ofSeconds(20));
    assertThat(stopper.get().isAlive()).isFalse();
    // The tree is in effect, so the stop that follows stops and releases what it describes.
    assertThat(supervisor.properties()).isSameAs(next);
    assertThat(supervisor.pipelineNames()).containsExactly("a", "c", "d");
    assertThat(context("c")).isNotSameAs(c);
    assertThat(context("c").properties()).isSameAs(next);
    // Neither the restarted, the added nor the failed unchanged pipeline was started.
    assertThat(transitions).noneMatch(transition -> transition.endsWith("=STARTING"));
    assertThat(supervisor.state("d")).isEqualTo(PipelineState.STOPPED);
    assertThat(supervisor.state("a")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
    assertThat(supervisor.state("c")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
    assertThat(supervisor.isShuttingDown()).isTrue();
  }

  /**
   * The same failure with a pipeline added earlier in the tree: what was acquired for the added
   * pipeline has to go back too. It does not today (#161); enable this with the fix.
   */
  @Test
  @Disabled("#161: apply does not release what it acquired for added pipelines when it fails")
  void failedApplyReleasesWhatItAcquiredForAddedPipelines() {
    start(TestConfigs.bind(deadBrokerYaml(A)));
    final KafkafimProperties running = supervisor.properties();
    assertThat(resources.instances()).isEqualTo(2);

    // c is added, and comes before a, which restarts on a new registry for es.
    KafkafimProperties next =
        TestConfigs.bind(deadBrokerYaml(C + A).replace("password: p", "password: p2"));
    ConfigDiff diff = ConfigDiff.between(running, next);
    assertThat(next.pipelines().keySet()).containsExactly("c", "a");
    assertThat(diff.added()).containsExactly("c");
    AtomicBoolean started = new AtomicBoolean();
    onAcquire =
        properties -> {
          if (properties == next && started.compareAndSet(false, true)) {
            supervisor.startPipeline("a");
          }
        };

    assertThatThrownBy(() -> supervisor.apply(next, diff))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'a' must be stopped to reconfigure");

    assertThat(supervisor.pipelineNames()).containsExactly("a");
    // Only what a runs with is left: not the DLQ producers and the es2 registry acquired for c.
    assertThat(resources.instances()).isEqualTo(2);
  }
}
