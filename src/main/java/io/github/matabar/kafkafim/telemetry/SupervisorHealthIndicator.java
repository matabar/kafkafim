package io.github.matabar.kafkafim.telemetry;

import java.util.function.BooleanSupplier;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * The {@code supervisor} health contributor, part of the liveness group (DESIGN §10): up while the
 * pipeline supervisor runs, and also once a graceful shutdown stopped it (a pod that drains must
 * not fail its liveness probe and be killed early). Down otherwise. Pipeline and Elasticsearch
 * problems never affect liveness; a restart would not fix them.
 */
public final class SupervisorHealthIndicator implements HealthIndicator {

  private final BooleanSupplier running;
  private final BooleanSupplier shuttingDown;

  /**
   * Creates the indicator.
   *
   * @param running whether the supervisor runs
   * @param shuttingDown whether a shutdown stopped (or is stopping) it
   */
  public SupervisorHealthIndicator(BooleanSupplier running, BooleanSupplier shuttingDown) {
    this.running = running;
    this.shuttingDown = shuttingDown;
  }

  @Override
  public Health health() {
    if (running.getAsBoolean()) {
      return Health.up().build();
    }
    if (shuttingDown.getAsBoolean()) {
      return Health.up().withDetail("shutdown", "in progress").build();
    }
    return Health.down().withDetail("supervisor", "not running").build();
  }
}
