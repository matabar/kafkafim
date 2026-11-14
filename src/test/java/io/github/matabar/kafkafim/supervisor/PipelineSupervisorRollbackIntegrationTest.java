package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.KafkaTestContainers;
import io.github.matabar.kafkafim.KafkaTopics;
import io.github.matabar.kafkafim.config.ConfigDiff;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * A failed apply starts a drained pipeline again (ADR-0041) with a running source, which needs a
 * broker: a pipeline that was {@code paused} comes back and is paused again for the same reason.
 * Kafka only; the Elasticsearch identity check is stubbed and no record is produced.
 */
@Testcontainers
class PipelineSupervisorRollbackIntegrationTest {

  @Container static final KafkaContainer KAFKA = KafkaTestContainers.kafka();

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private volatile Consumer<KafkafimProperties> onAcquire = properties -> {};
  private final ListenerRelay relay = new ListenerRelay(List.of(), List.of());
  private final PipelineResources resources =
      new PipelineResources(
          relay,
          properties -> {
            onAcquire.accept(properties);
            return (cluster, identity) -> Preflight.Status.OK;
          },
          PipelineSupervisorTest.SETTINGS);
  private PipelineSupervisor supervisor;

  @AfterEach
  void close() {
    if (supervisor != null) {
      supervisor.stop();
    }
    resources.close();
  }

  private KafkafimProperties tree(String topic, String index) {
    return TestConfigs.bind(
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
              routes: [ { default: true, cluster: es, index: %s } ]
        """
            .formatted(KAFKA.getBootstrapServers(), topic, index));
  }

  @Test
  void pausedPipelineIsStartedAgainByFailedApplyAndPausedAgain() throws Exception {
    String topic = "t-" + UUID.randomUUID().toString().substring(0, 8);
    try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      KafkaTopics.create(
          admin,
          List.of(
              new NewTopic(topic, 1, (short) 1),
              new NewTopic(topic + ".kafkafim-dlq", 1, (short) 1)));
    }
    KafkafimProperties running = tree(topic, "a");
    supervisor = new PipelineSupervisor(running, resources, List.of());
    relay.attach(supervisor);
    supervisor.start();
    await().atMost(TIMEOUT).until(() -> supervisor.state("p") == PipelineState.RUNNING);
    supervisor.runtimes().iterator().next().pause(PipelineRuntime.DLQ_BLOCKED_REASON);
    assertThat(supervisor.state("p")).isEqualTo(PipelineState.PAUSED);

    final KafkafimProperties next = tree(topic, "a2");
    ConfigDiff diff = ConfigDiff.between(running, next);
    assertThat(diff.restarted()).containsOnlyKeys("p");
    onAcquire =
        properties -> {
          if (properties == next) {
            throw new IllegalStateException("no client today");
          }
        };

    assertThatThrownBy(() -> supervisor.apply(next, diff))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("no client today");

    // Started again on the running tree: a new start, and paused again at once, since
    // dlq-blocked outlives a restart (the breaker and auth reasons would not).
    await().atMost(TIMEOUT).until(() -> supervisor.state("p") == PipelineState.PAUSED);
    assertThat(supervisor.properties()).isSameAs(running);
  }
}
