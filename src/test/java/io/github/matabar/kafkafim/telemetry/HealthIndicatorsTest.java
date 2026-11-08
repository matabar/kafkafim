package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;

class HealthIndicatorsTest {

  @Test
  void livenessIsUpWhileRunningAndWhileShuttingDown() {
    assertThat(new SupervisorHealthIndicator(() -> true, () -> false).health().getStatus())
        .isEqualTo(Status.UP);
    assertThat(new SupervisorHealthIndicator(() -> false, () -> true).health().getStatus())
        .isEqualTo(Status.UP);
    assertThat(new SupervisorHealthIndicator(() -> false, () -> false).health().getStatus())
        .isEqualTo(Status.DOWN);
  }

  @Test
  void kafkaReadinessNeedsOneConnectedStartedPipeline() {
    Map<String, Boolean> connections = new LinkedHashMap<>();
    KafkaHealthIndicator kafka = new KafkaHealthIndicator(() -> connections);

    assertThat(kafka.health().getStatus()).as("nothing started").isEqualTo(Status.UP);

    connections.put("orders", false);
    connections.put("audit", false);
    assertThat(kafka.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    assertThat(kafka.health().getDetails()).containsEntry("orders", "disconnected");

    connections.put("audit", true);
    assertThat(kafka.health().getStatus()).isEqualTo(Status.UP);
    assertThat(kafka.health().getDetails()).containsEntry("audit", "connected");
  }
}
