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
import java.util.function.Supplier;
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
    supervisor = new PipelineSupervisor(properties, resources, List.of());
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

  @Test
  void unchangedTreeSucceedsWithoutNewGeneration() {
    ConfigReloader reloader = start(A);
    ReloadResult result = reloader.reload("test");
    assertThat(result.result()).isEqualTo(ReloadResult.UNCHANGED);
    assertThat(result.generation()).isEqualTo(1);
    assertThat(result.unchanged()).containsExactly("a");
    assertThat(reloads("success")).isEqualTo(1);
    assertThat(generationGauge()).isEqualTo(1);
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
