package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.KafkaTestContainers;
import io.github.matabar.kafkafim.KafkaTopics;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.source.kafka.KafkaPipelineSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * A pipeline that fails while running (ADR-0036): {@code failed} in the state, the gauge and
 * readiness, its source stopped, and one more start on an accepted reload. Kafka only: the
 * Elasticsearch identity check is stubbed and no record is produced, so no bulk request is sent.
 * That the source reports a handler failure or a stopped consumer is {@code
 * KafkaPipelineSourceIntegrationTest}'s part.
 */
@Testcontainers
class PipelineFailureIntegrationTest {

  @Container static final KafkaContainer KAFKA = KafkaTestContainers.kafka();

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private final ListenerRelay relay = new ListenerRelay(List.of(), List.of());
  private PipelineResources resources;
  private PipelineSupervisor supervisor;

  @BeforeEach
  void startPipeline() throws Exception {
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
            relay,
            p -> (cluster, identity) -> Preflight.Status.OK,
            PipelineSupervisorTest.SETTINGS);
    supervisor = new PipelineSupervisor(properties, resources, List.of());
    relay.attach(supervisor);
    supervisor.start();
    awaitState(PipelineState.RUNNING);
  }

  @AfterEach
  void close() {
    supervisor.stop();
    resources.close();
  }

  @Test
  void failureWhileRunningStopsThePipelineUntilAnAcceptedReloadRetriesIt() {
    PipelineRuntime runtime = supervisor.runtimes().iterator().next();
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    new PipelineStateMetrics(supervisor).bindTo(registry);
    PipelinesHealthIndicator readiness = new PipelinesHealthIndicator(supervisor);
    KafkaPipelineSource failed = runtime.source();
    assertThat(failed).isNotNull();
    assertThat(readiness.health().getStatus()).isEqualTo(Status.UP);

    runtime.sourceFailed(failed, "Kafka consumer stopped (ERROR)");

    // Failed at once, before the stop finishes; the stop then closes the source.
    assertThat(supervisor.state("p")).isEqualTo(PipelineState.FAILED);
    assertThat(supervisor.failure("p")).isEqualTo("Kafka consumer stopped (ERROR)");
    assertThat(gauge(registry, "failed")).isEqualTo(1);
    assertThat(gauge(registry, "running")).isZero();
    assertThat(readiness.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    await().atMost(TIMEOUT).until(() -> runtime.source() == null);
    assertThat(failed.isRunning()).isFalse();
    // A plain stop (shutdown, or a reload that restarts it) keeps the failed state.
    supervisor.stopPipeline("p");
    assertThat(supervisor.state("p")).isEqualTo(PipelineState.FAILED);

    // An accepted reload that leaves the pipeline unchanged starts it once more.
    assertThat(supervisor.retryFailedPipelines(List.of("p"))).containsExactly("p");
    awaitState(PipelineState.RUNNING);
    assertThat(supervisor.failure("p")).isNull();
    assertThat(readiness.health().getStatus()).isEqualTo(Status.UP);
    assertThat(supervisor.retryFailedPipelines(List.of("p"))).isEmpty();

    // A late report from the previous run's source changes nothing.
    runtime.sourceFailed(failed, "late");
    assertThat(supervisor.state("p")).isEqualTo(PipelineState.RUNNING);
    assertThat(runtime.source()).isNotNull().isNotSameAs(failed);
  }

  @Test
  void stopWaitsForTheStopAfterFailure() {
    PipelineRuntime runtime = supervisor.runtimes().iterator().next();
    runtime.sourceFailed(runtime.source(), "record handler failed: boom");
    supervisor.stopPipeline("p");
    // stop() returned, so the failure's stop is done too: nothing is left to close.
    assertThat(runtime.source()).isNull();
    assertThat(supervisor.state("p")).isEqualTo(PipelineState.FAILED);
    supervisor.startPipeline("p");
    awaitState(PipelineState.RUNNING);
  }

  private void awaitState(PipelineState state) {
    await().atMost(TIMEOUT).until(() -> supervisor.state("p") == state);
  }

  private static double gauge(SimpleMeterRegistry registry, String state) {
    return registry
        .get(PipelineStateMetrics.NAME)
        .tag("pipeline", "p")
        .tag("state", state)
        .gauge()
        .value();
  }
}
