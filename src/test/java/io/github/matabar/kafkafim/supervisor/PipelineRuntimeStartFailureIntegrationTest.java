package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.KafkaTestContainers;
import io.github.matabar.kafkafim.KafkaTopics;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.telemetry.PipelineMeters;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * A start that fails unexpectedly after the preflight passed leaves nothing open (#209): neither
 * the sinks it opened nor the source it created or started. Kafka only; the Elasticsearch identity
 * check is stubbed and no record is produced.
 */
@Testcontainers
class PipelineRuntimeStartFailureIntegrationTest {

  @Container static final KafkaContainer KAFKA = KafkaTestContainers.kafka();

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private PipelineResources resources;
  private PipelineRuntime.Context context;

  @BeforeEach
  void context() throws Exception {
    String topic = "t-" + UUID.randomUUID().toString().substring(0, 8);
    try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      KafkaTopics.create(
          admin,
          List.of(
              new NewTopic(topic, 1, (short) 1),
              new NewTopic(topic + ".kafkafim-dlq", 1, (short) 1)));
    }
    KafkafimProperties properties =
        TestConfigs.bind(
            """
            kafkafim:
              kafka:
                clusters:
                  k: { bootstrap-servers: ["%s"] }
              elasticsearch:
                clusters:
                  es: { uris: ["http://127.0.0.1:9"] }
              pipelines:
                p:
                  source: { cluster: k, topics: [%s] }
                  routes: [ { default: true, cluster: es, index: a } ]
            """
                .formatted(KAFKA.getBootstrapServers(), topic));
    resources =
        new PipelineResources(
            new ListenerRelay(List.of(), List.of()),
            p -> (cluster, identity) -> Preflight.Status.OK,
            PipelineSupervisorTest.SETTINGS);
    context = resources.acquire("p", properties);
  }

  @AfterEach
  void close() {
    resources.close();
  }

  private void awaitFailed(PipelineRuntime runtime) {
    await().atMost(TIMEOUT).until(() -> runtime.state() == PipelineState.FAILED_PREFLIGHT);
    assertThat(runtime.failure()).contains("boom");
    assertThat(runtime.holdsResources()).isFalse();
    assertThat(runtime.source()).isNull();
  }

  /** The sinks are open when the meters are looked up; nothing has published them yet. */
  @Test
  void unexpectedExceptionAfterTheSinksOpenedClosesThem() {
    Supplier<PipelineMeters> boom =
        () -> {
          throw new IllegalStateException("boom");
        };
    PipelineRuntime runtime = new PipelineRuntime("p", context, r -> {}, boom);

    runtime.start();

    awaitFailed(runtime);
    assertThat(runtime.retryIfFailed()).isTrue();
    runtime.stop();
  }

  /** The source is started when the state change to {@code running} is reported. */
  @Test
  void unexpectedExceptionWhileReportingRunningStopsTheSourceAndSinks() {
    Consumer<PipelineRuntime> boom =
        r -> {
          if (r.state() == PipelineState.RUNNING) {
            throw new IllegalStateException("boom");
          }
        };
    PipelineRuntime runtime = new PipelineRuntime("p", context, boom);
    runtime.start();
    // The source of the failed start, read while it still exists.
    await().atMost(TIMEOUT).until(() -> runtime.source() != null || runtime.failure() != null);
    var started = runtime.source();

    awaitFailed(runtime);
    if (started != null) {
      await().atMost(TIMEOUT).until(() -> !started.isRunning());
    }
    runtime.stop();
  }
}
