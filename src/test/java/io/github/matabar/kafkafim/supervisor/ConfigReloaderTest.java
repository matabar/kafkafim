package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ByteArrayResource;

/** The reload path without files or brokers: validation, metrics, generation, endpoint status. */
class ConfigReloaderTest {

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
        routes: [ { default: true, cluster: es2, index: b } ]
      """;

  /** Starts of any pipeline: transitions into {@code starting}. */
  private final AtomicInteger starts = new AtomicInteger();

  private final SupervisorListener countStarts =
      new SupervisorListener() {
        @Override
        public void onStateChange(String pipeline, PipelineState from, PipelineState to) {
          if (to == PipelineState.STARTING) {
            starts.incrementAndGet();
          }
        }
      };

  private final ListenerRelay relay = new ListenerRelay(List.of(), List.of());
  private final PipelineResources resources =
      new PipelineResources(
          relay, p -> (cluster, identity) -> Preflight.Status.OK, PipelineSupervisorTest.SETTINGS);
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private PipelineSupervisor supervisor;
  private volatile Supplier<Environment> disk;

  @AfterEach
  void close() {
    if (supervisor != null) {
      supervisor.stop();
    }
    resources.close();
  }

  /** {@link TestConfigs#CLUSTERS} (literal test passwords allowed) with the given pipelines. */
  private static String yaml(String pipelines) {
    return TestConfigs.CLUSTERS
        + "  allow-inline-secrets: true\n"
        + "  pipelines:\n"
        + pipelines.indent(4);
  }

  private static Environment environment(String yaml) {
    StandardEnvironment environment = new StandardEnvironment();
    try {
      new YamlPropertySourceLoader()
          .load("disk.yaml", new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8)))
          .forEach(environment.getPropertySources()::addLast);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return environment;
  }

  private ConfigReloader start(String pipelines) {
    KafkafimProperties properties = TestConfigs.bind(yaml(pipelines));
    supervisor = new PipelineSupervisor(properties, resources, List.of(countStarts));
    relay.attach(supervisor);
    supervisor.start();
    onDisk(pipelines);
    return new ConfigReloader(() -> disk.get(), supervisor, registry, List.of(), null);
  }

  private void onDisk(String pipelines) {
    Environment environment = environment(yaml(pipelines));
    disk = () -> environment;
  }

  private double reloads(String result) {
    return registry.get(ConfigReloader.RELOADS).tag("result", result).counter().count();
  }

  private double generationGauge() {
    return registry.get(ConfigReloader.GENERATION).gauge().value();
  }

  private void awaitSettled() {
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> !supervisor.states().containsValue(PipelineState.STARTING));
  }

  private static List<String> concat(List<String> first, List<String> second) {
    return Stream.concat(first.stream(), second.stream()).toList();
  }

  @Test
  void unchangedTreeSucceedsWithoutNewGeneration() {
    ConfigReloader reloader = start(A);
    ReloadResult result = reloader.reload("test");
    assertThat(result.result()).isEqualTo(ReloadResult.UNCHANGED);
    assertThat(result.generation()).isEqualTo(1);
    // a is either still starting (unchanged) or failed preflight already (retried).
    assertThat(concat(result.unchanged(), result.retried())).containsExactly("a");
    assertThat(reloads("success")).isEqualTo(1);
    assertThat(generationGauge()).isEqualTo(1);
  }

  @Test
  void anUnchangedTreeRetriesFailedPreflightOncePerReload() {
    final ConfigReloader reloader = start(A);
    awaitSettled();
    assertThat(supervisor.state("a")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
    assertThat(starts.get()).isEqualTo(1);

    ReloadResult result = reloader.reload("test");
    assertThat(result.result()).isEqualTo(ReloadResult.UNCHANGED);
    assertThat(result.generation()).isEqualTo(1);
    assertThat(result.retried()).containsExactly("a");
    assertThat(result.unchanged()).isEmpty();
    assertThat(reloads("success")).isEqualTo(1);
    assertThat(generationGauge()).isEqualTo(1);
    awaitSettled();
    // Still failing: back in failed_preflight after exactly one more attempt, and no further one.
    assertThat(supervisor.state("a")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
    assertThat(starts.get()).isEqualTo(2);
    await().during(Duration.ofSeconds(2)).until(() -> starts.get() == 2);

    assertThat(new ReloadEndpoint(reloader).reload().getBody().retried()).containsExactly("a");
  }

  @Test
  void anAppliedTreeRetriesTheFailedPipelinesItLeavesUnchanged() {
    ConfigReloader reloader = start(A + B);
    awaitSettled();
    onDisk(A + B.replace("index: b", "index: b2"));
    ReloadResult result = reloader.reload("test");
    assertThat(result.result()).isEqualTo(ReloadResult.APPLIED);
    assertThat(result.restarted()).extracting(ReloadResult.Restart::pipeline).containsExactly("b");
    assertThat(result.retried()).containsExactly("a");
    assertThat(result.unchanged()).isEmpty();
    awaitSettled();
  }

  @Test
  void rejectedTreeRetriesNothing() {
    ConfigReloader reloader = start(A);
    awaitSettled();
    onDisk(A.replace("cluster: es,", "cluster: nope,"));
    ReloadResult result = reloader.reload("test");
    assertThat(result.result()).isEqualTo(ReloadResult.REJECTED);
    assertThat(result.retried()).isEmpty();
    await().during(Duration.ofSeconds(1)).until(() -> starts.get() == 1);
    assertThat(supervisor.state("a")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
  }

  @Test
  void changedTreeIsAppliedAsNewGeneration() {
    ConfigReloader reloader = start(A);
    onDisk(A.replace("index: a", "index: a2") + B);
    ReloadResult result = reloader.reload("test");
    assertThat(result.result()).isEqualTo(ReloadResult.APPLIED);
    assertThat(result.generation()).isEqualTo(2);
    assertThat(result.added()).containsExactly("b");
    assertThat(result.restarted())
        .containsExactly(new ReloadResult.Restart("a", List.of("pipeline definition changed")));
    assertThat(result.errors()).isEmpty();
    assertThat(supervisor.pipelineNames()).containsExactly("a", "b");
    assertThat(generationGauge()).isEqualTo(2);
    assertThat(reloads("success")).isEqualTo(1);
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> !supervisor.states().containsValue(PipelineState.STARTING));
  }

  /**
   * A change that no pipeline uses (here: cluster {@code es2}, which only {@code B} routes to, and
   * the startup-only watcher settings) is still a new tree in effect: {@code applied}, a new
   * generation, nothing restarted. The generation is how an operator sees a config-only change land
   * (RUNBOOK §2.2), so it must move even when no pipeline does.
   */
  @Test
  void changeNoPipelineUsesIsAppliedAsNewGenerationWithoutRestarts() {
    ConfigReloader reloader = start(A);
    awaitSettled();
    Environment changed =
        environment(
            yaml(A)
                .replace("http://es2:9200", "http://es2-new:9200")
                .replace("  pipelines:\n", "  reload: { debounce: 5s }\n  pipelines:\n"));
    disk = () -> changed;
    ReloadResult result = reloader.reload("test");
    assertThat(result.result()).isEqualTo(ReloadResult.APPLIED);
    assertThat(result.generation()).isEqualTo(2);
    assertThat(result.added()).isEmpty();
    assertThat(result.removed()).isEmpty();
    assertThat(result.restarted()).isEmpty();
    assertThat(concat(result.unchanged(), result.retried())).containsExactly("a");
    assertThat(supervisor.properties().elasticsearch().clusters().get("es2").uris())
        .containsExactly("http://es2-new:9200");
    assertThat(supervisor.properties().reload().debounce()).isEqualTo(Duration.ofSeconds(5));
    assertThat(generationGauge()).isEqualTo(2);
    assertThat(reloads("success")).isEqualTo(1);

    assertThat(reloader.reload("test").result()).isEqualTo(ReloadResult.UNCHANGED);
    assertThat(generationGauge()).isEqualTo(2);
    awaitSettled();
  }

  @Test
  void anInvalidTreeIsRejectedWholeAndTheRunningOneStays() {
    ConfigReloader reloader = start(A);
    final KafkafimProperties running = supervisor.properties();
    // b is fine, but a now routes to an unknown cluster: nothing of the tree is applied.
    onDisk(A.replace("cluster: es,", "cluster: nope,") + B);
    ReloadResult result = reloader.reload("test");
    assertThat(result.result()).isEqualTo(ReloadResult.REJECTED);
    assertThat(result.accepted()).isFalse();
    assertThat(result.generation()).isEqualTo(1);
    assertThat(result.errors()).anySatisfy(e -> assertThat(e).contains("nope"));
    assertThat(supervisor.properties()).isSameAs(running);
    assertThat(supervisor.pipelineNames()).containsExactly("a");
    assertThat(reloads("failure")).isEqualTo(1);
    assertThat(reloads("success")).isZero();

    ReloadEndpoint endpoint = new ReloadEndpoint(reloader);
    WebEndpointResponse<ReloadResult> response = endpoint.reload();
    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.getBody().errors()).isNotEmpty();
    assertThat(reloads("failure")).isEqualTo(2);
  }

  @Test
  void anUnreadableTreeIsRejected() {
    ConfigReloader reloader = start(A);
    disk =
        () -> {
          throw new IllegalStateException("mapping values are not allowed here");
        };
    ReloadResult result = reloader.reload("test");
    assertThat(result.result()).isEqualTo(ReloadResult.REJECTED);
    assertThat(result.errors())
        .singleElement()
        .asString()
        .startsWith("configuration cannot be read")
        .contains("mapping values");
    assertThat(reloads("failure")).isEqualTo(1);
  }

  /**
   * A valid tree whose apply throws (here: the building blocks are closed, so the added pipeline
   * cannot get any) is a failed reload: counted as {@code failure}, the generation stays, and the
   * exception reaches the caller. The endpoint answers 503 for an {@link IllegalStateException}.
   */
  @Test
  void treeThatFailsWhileBeingAppliedCountsAsFailureAndKeepsTheGeneration() {
    final ConfigReloader reloader = start(A);
    awaitSettled();
    final KafkafimProperties running = supervisor.properties();
    resources.close();
    onDisk(A + B);

    assertThatThrownBy(() -> reloader.reload("test"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Pipeline resources are closed");

    assertThat(supervisor.isRunning()).isTrue();
    assertThat(reloads("failure")).isEqualTo(1);
    assertThat(reloads("success")).isZero();
    assertThat(reloader.generation()).isEqualTo(1);
    assertThat(generationGauge()).isEqualTo(1);
    assertThat(supervisor.properties()).isSameAs(running);
    assertThat(supervisor.pipelineNames()).containsExactly("a");

    WebEndpointResponse<ReloadResult> response = new ReloadEndpoint(reloader).reload();
    assertThat(response.getStatus()).isEqualTo(503);
    assertThat(response.getBody().result()).isEqualTo(ReloadResult.REJECTED);
    assertThat(response.getBody().generation()).isEqualTo(1);
    assertThat(response.getBody().errors()).containsExactly("Pipeline resources are closed");
    assertThat(reloads("failure")).isEqualTo(2);
    assertThat(reloads("success")).isZero();
  }

  /**
   * A shutdown that begins after the reload saw a running supervisor, before {@code apply} took
   * over: nothing was applied and nothing is wrong with the tree, so it is neither a failure nor a
   * success.
   */
  @Test
  void shutdownBeforeApplyIsNotCountedAsFailedReload() {
    ConfigReloader reloader = start(A);
    awaitSettled();
    final KafkafimProperties running = supervisor.properties();
    Environment changed = environment(yaml(A + B));
    disk =
        () -> {
          supervisor.stop();
          return changed;
        };

    assertThatThrownBy(() -> reloader.reload("test"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("The pipeline supervisor is not running");

    assertThat(reloads("failure")).isZero();
    assertThat(reloads("success")).isZero();
    assertThat(reloader.generation()).isEqualTo(1);
    assertThat(generationGauge()).isEqualTo(1);
    assertThat(supervisor.properties()).isSameAs(running);
    assertThat(supervisor.pipelineNames()).containsExactly("a");
  }

  @Test
  void parseErrorsKeepOnlyTheirFirstLine() {
    assertThat(
            ConfigReloader.firstLine(
                new IllegalStateException(
                    "mapping values are not allowed here\n in 'reader', line 3:\n    password: x")))
        .isEqualTo("IllegalStateException: mapping values are not allowed here");
  }

  @Test
  void reloadsNeedRunningSupervisor() {
    ConfigReloader reloader = start(A);
    assertThat(new ReloadEndpoint(reloader).reload().getStatus()).isEqualTo(200);
    supervisor.stop();
    assertThatThrownBy(() -> reloader.reload("test")).isInstanceOf(IllegalStateException.class);
    assertThat(new ReloadEndpoint(reloader).reload().getStatus()).isEqualTo(503);
  }
}
