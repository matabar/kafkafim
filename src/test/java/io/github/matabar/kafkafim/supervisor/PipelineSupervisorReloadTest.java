package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.config.ConfigDiff;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Applying reloaded configurations without brokers (DESIGN §5.6): which pipelines stop and start,
 * which building blocks they keep, get anew or close. The Kafka cluster's host does not resolve, so
 * every started pipeline fails preflight at once and stays stopped.
 */
class PipelineSupervisorReloadTest {

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
  private static final String D =
      """
      d:
        source: { cluster: k, topics: [d] }
        routes: [ { default: true, cluster: es, index: d } ]
      """;

  private final ListenerRelay relay = new ListenerRelay(List.of(), List.of());
  private final PipelineResources resources =
      new PipelineResources(
          relay, p -> (cluster, identity) -> Preflight.Status.OK, PipelineSupervisorTest.SETTINGS);
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
    awaitSettled();
    return supervisor;
  }

  private void awaitSettled() {
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> !supervisor.states().containsValue(PipelineState.STARTING));
  }

  private void apply(KafkafimProperties next) {
    supervisor.apply(next, ConfigDiff.between(supervisor.properties(), next));
    awaitSettled();
  }

  /** {@link TestConfigs#CLUSTERS} with the given pipelines, as YAML. */
  private static String yaml(String pipelines) {
    return TestConfigs.CLUSTERS + "  pipelines:\n" + pipelines.indent(4);
  }

  private PipelineRuntime.Context context(String pipeline) {
    return supervisor.runtimes().stream()
        .filter(r -> r.name().equals(pipeline))
        .findFirst()
        .orElseThrow()
        .context();
  }

  @Test
  void addedRemovedChangedAndUnchangedPipelines() {
    start(TestConfigs.withPipelines(A + B + C));
    assertThat(supervisor.states()).containsOnlyKeys("a", "b", "c");
    assertThat(supervisor.states().values()).containsOnly(PipelineState.FAILED_PREFLIGHT);
    final PipelineRuntime.Context a = context("a");
    final PipelineRuntime.Context c = context("c");
    // One registry per cluster and one DLQ producers instance for Kafka cluster k.
    assertThat(resources.instances()).isEqualTo(3);

    KafkafimProperties next = TestConfigs.withPipelines(A.replace("index: a", "index: a2") + C + D);
    apply(next);

    assertThat(supervisor.pipelineNames()).containsExactly("a", "c", "d");
    assertThat(supervisor.properties()).isSameAs(next);
    assertThat(context("c")).isSameAs(c);
    assertThat(context("a")).isNotSameAs(a);
    assertThat(context("a").properties()).isSameAs(next);
    // a's clusters are unchanged: it keeps its Elasticsearch registry and DLQ writer.
    assertThat(context("a").sinks().get("es")).isSameAs(a.sinks().get("es"));
    assertThat(context("a").dlqProducers()).isSameAs(a.dlqProducers());
    // d is new to the old DLQ producers (its writer is cached by name), so it gets new ones.
    assertThat(context("d").dlqProducers()).isNotSameAs(a.dlqProducers());
    assertThat(context("d").sinks().get("es")).isSameAs(a.sinks().get("es"));
    assertThatThrownBy(() -> supervisor.state("b")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void changedIdentityGetsNewClientsAndTheOldOnesCloseWithTheirLastPipeline() {
    start(TestConfigs.withPipelines(A + C));
    PipelineRuntime.Context a = context("a");
    PipelineRuntime.Context c = context("c");

    // Identity `writer` of es changes: a restarts on a new registry for es; c (es2) is untouched.
    apply(TestConfigs.bind(yaml(A + C).replace("password: p", "password: p2")));
    assertThat(context("c")).isSameAs(c);
    assertThat(context("a").sinks().get("es")).isNotSameAs(a.sinks().get("es"));
    // The old es registry had no other user: closed. Kafka k is unchanged: a keeps its writer.
    assertThat(context("a").dlqProducers()).isSameAs(a.dlqProducers());
    assertThat(resources.instances()).isEqualTo(3);
  }

  @Test
  void changedKafkaClusterGetsNewDlqProducersAndTheOldOnesClose() {
    start(TestConfigs.withPipelines(A + C));
    PipelineRuntime.Context a = context("a");
    apply(TestConfigs.bind(yaml(A + C).replace("k1:9092", "k2:9092")));
    assertThat(context("a").dlqProducers()).isNotSameAs(a.dlqProducers());
    assertThat(context("c").dlqProducers()).isSameAs(context("a").dlqProducers());
    // Both pipelines left the old producers: closed.
    assertThatThrownBy(() -> a.dlqProducers().writer("a"))
        .isInstanceOf(IllegalStateException.class);
    assertThat(resources.instances()).isEqualTo(3);
  }

  @Test
  void removedPipelineReleasesWhatOnlyItUsed() {
    start(TestConfigs.withPipelines(A + C));
    PipelineRuntime.Context a = context("a");
    apply(TestConfigs.withPipelines(A));
    assertThat(supervisor.pipelineNames()).containsExactly("a");
    assertThat(context("a")).isSameAs(a);
    // es2's registry belonged to c alone: closed. Kafka k and es are a's.
    assertThat(resources.instances()).isEqualTo(2);
    // Removing the last pipeline closes everything.
    apply(TestConfigs.withPipelines("{}"));
    assertThat(resources.instances()).isZero();
    assertThatThrownBy(() -> a.dlqProducers().writer("a"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void eventsOfBuildingBlocksLeftBehindAreIgnored() {
    start(TestConfigs.withPipelines(A));
    PipelineRuntime.Context before = context("a");
    // A blocked DLQ writer pauses a; the breaker of its es registry too.
    relay.dlqListener(before::dlqProducers).onBlocked("a", null);
    assertThat(supervisor.pauseReasons("a")).containsExactly("dlq-blocked");

    apply(
        TestConfigs.bind(
            yaml(A).replace("k1:9092", "k2:9092").replace("password: p", "password: p2")));
    PipelineRuntime.Context after = context("a");
    assertThat(after.dlqProducers()).isNotSameAs(before.dlqProducers());
    // The new writer has failed nothing: dlq-blocked is dropped with the old writer.
    assertThat(supervisor.pauseReasons("a")).isEmpty();
    relay.dlqListener(before::dlqProducers).onBlocked("a", null);
    relay
        .sinkListener(() -> before.sinks().get("es"))
        .onBreakerStateChange("es", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    assertThat(supervisor.pauseReasons("a")).isEmpty();

    relay
        .sinkListener(() -> after.sinks().get("es"))
        .onBreakerStateChange("es", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    relay.dlqListener(after::dlqProducers).onBlocked("a", null);
    assertThat(supervisor.pauseReasons("a"))
        .containsExactlyInAnyOrder("dlq-blocked", "es-breaker:es");
  }

  @Test
  void retriesOnlyFailedPipelinesAndOnlyWhileRunning() {
    start(TestConfigs.withPipelines(A + C));
    // Only candidates are retried, in configuration order; unknown names are ignored.
    assertThat(supervisor.retryFailedPreflight(List.of("c", "gone"))).containsExactly("c");
    awaitSettled();
    assertThat(supervisor.retryFailedPreflight(List.of("c", "a"))).containsExactly("a", "c");
    awaitSettled();
    assertThat(supervisor.states().values()).containsOnly(PipelineState.FAILED_PREFLIGHT);

    supervisor.stop();
    assertThat(supervisor.retryFailedPreflight(List.of("a"))).isEmpty();
    assertThat(supervisor.state("a")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
  }

  @Test
  void applyRetriesUnchangedFailedPipelinesButNotRestartedOrAddedOnes() {
    start(TestConfigs.withPipelines(A + C));
    KafkafimProperties next = TestConfigs.withPipelines(A + C.replace("index: c", "index: c2") + D);
    List<String> retried =
        supervisor.apply(next, ConfigDiff.between(supervisor.properties(), next));
    assertThat(retried).containsExactly("a");
    awaitSettled();
    assertThat(supervisor.states().values()).containsOnly(PipelineState.FAILED_PREFLIGHT);
  }

  @Test
  void applyNeedsRunningSupervisor() {
    KafkafimProperties properties = TestConfigs.withPipelines(A);
    supervisor = new PipelineSupervisor(properties, resources, List.of());
    assertThatThrownBy(
            () -> supervisor.apply(properties, ConfigDiff.between(properties, properties)))
        .isInstanceOf(IllegalStateException.class);
  }
}
