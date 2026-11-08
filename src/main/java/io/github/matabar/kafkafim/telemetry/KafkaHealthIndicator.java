package io.github.matabar.kafkafim.telemetry;

import java.util.Map;
import java.util.function.Supplier;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * The {@code kafka} health contributor, part of the readiness group (DESIGN §10): "Kafka clients
 * connected". Up while at least one started pipeline's consumers hold broker connections, or while
 * no pipeline is started (the {@code pipelines} contributor decides then); out of service when
 * every started pipeline lost its brokers. As with {@code pipelines}, one pipeline on an
 * unreachable cluster does not take a pod that does other work out of service, and Elasticsearch is
 * never consulted.
 */
public final class KafkaHealthIndicator implements HealthIndicator {

  private final Supplier<Map<String, Boolean>> connections;

  /**
   * Creates the indicator.
   *
   * @param connections for every started pipeline, whether its consumers are connected
   */
  public KafkaHealthIndicator(Supplier<Map<String, Boolean>> connections) {
    this.connections = connections;
  }

  @Override
  public Health health() {
    Map<String, Boolean> connected = connections.get();
    boolean any = connected.values().stream().anyMatch(Boolean::booleanValue);
    Health.Builder health = connected.isEmpty() || any ? Health.up() : Health.outOfService();
    connected.forEach(
        (pipeline, up) -> health.withDetail(pipeline, up ? "connected" : "disconnected"));
    return health.build();
  }
}
