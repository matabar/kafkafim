package io.github.matabar.kafkafim.sink.es;

import java.time.Duration;
import java.util.Objects;

/**
 * HTTP client settings shared by every {@code (cluster, identity)} client. Not configurable per
 * cluster in the MVP; the defaults are documented in DESIGN §7.
 *
 * @param connectTimeout TCP and TLS connect timeout per node
 * @param responseTimeout socket inactivity timeout while waiting for a response; a bulk request of
 *     {@code max-bytes} must be answered within it
 * @param connectionRequestTimeout wait for a pooled connection
 * @param maxConnectionsPerNode pooled connections per node
 * @param maxConnectionsTotal pooled connections per client
 */
public record ClientSettings(
    Duration connectTimeout,
    Duration responseTimeout,
    Duration connectionRequestTimeout,
    int maxConnectionsPerNode,
    int maxConnectionsTotal) {

  /** Connect 5s, response 60s, pool lease 30s, 32 connections per node, 128 per client. */
  public static final ClientSettings DEFAULT =
      new ClientSettings(
          Duration.ofSeconds(5), Duration.ofSeconds(60), Duration.ofSeconds(30), 32, 128);

  /** Validates the settings. */
  public ClientSettings {
    Objects.requireNonNull(connectTimeout, "connectTimeout");
    Objects.requireNonNull(responseTimeout, "responseTimeout");
    Objects.requireNonNull(connectionRequestTimeout, "connectionRequestTimeout");
    if (maxConnectionsPerNode < 1 || maxConnectionsTotal < maxConnectionsPerNode) {
      throw new IllegalArgumentException("need 1 <= maxConnectionsPerNode <= maxConnectionsTotal");
    }
  }
}
