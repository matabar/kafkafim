package io.github.matabar.kafkafim.sink.es;

import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import java.time.Duration;
import java.util.Objects;

/**
 * HTTP client settings of a {@code (cluster, identity)} client. The connect and response timeouts
 * come from the cluster's {@code client} block ({@link #forCluster}); the pool is a code default
 * (DESIGN §7, ADR-0031). {@link ElasticsearchSinks} always replaces the two timeouts here with the
 * cluster's; only callers that build a client directly (the preflight check) use them.
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

  /**
   * These settings with a cluster's connect and response timeouts.
   *
   * @param client the cluster's validated {@code client} block
   * @return the settings for that cluster's clients
   */
  public ClientSettings forCluster(ElasticsearchCluster.Client client) {
    return new ClientSettings(
        client.connectTimeout(),
        client.responseTimeout(),
        connectionRequestTimeout,
        maxConnectionsPerNode,
        maxConnectionsTotal);
  }
}
