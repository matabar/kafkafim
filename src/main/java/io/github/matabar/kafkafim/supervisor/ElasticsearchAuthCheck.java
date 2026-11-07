package io.github.matabar.kafkafim.supervisor;

import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.ResponseException;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.sink.es.ClientSettings;
import io.github.matabar.kafkafim.sink.es.ElasticsearchClients;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * The preflight check that an Elasticsearch identity authenticates (DESIGN §4.6): one {@code GET
 * /_security/_authenticate} with a short-lived client built exactly like the sink's ({@link
 * ElasticsearchClients}), so trust, credentials and URIs are the ones bulk requests will use. The
 * endpoint needs no privilege, so it never fails for a correctly scoped writer role.
 */
final class ElasticsearchAuthCheck implements Preflight.AuthCheck {

  /** Shorter than the sink's: preflight must not hold a pipeline's start for a minute. */
  static final ClientSettings SETTINGS =
      new ClientSettings(
          Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(10), 1, 1);

  private final Map<String, ElasticsearchCluster> clusters;

  ElasticsearchAuthCheck(Map<String, ElasticsearchCluster> clusters) {
    this.clusters = Map.copyOf(clusters);
  }

  @Override
  public Preflight.Status check(String clusterName, @Nullable String identity) {
    ElasticsearchCluster cluster = clusters.get(clusterName);
    if (cluster == null) {
      return Preflight.Status.rejected("unknown Elasticsearch cluster");
    }
    Rest5Client client;
    try {
      client = ElasticsearchClients.create(clusterName, cluster, identity, SETTINGS);
    } catch (IllegalArgumentException | IllegalStateException e) {
      // Unknown identity, unreadable CA certificate or keystore: configuration.
      return Preflight.Status.rejected(e.getMessage());
    }
    try {
      client.performRequest(new Request("GET", "/_security/_authenticate"));
      return Preflight.Status.OK;
    } catch (ResponseException e) {
      return classify(e.getResponse().getStatusCode());
    } catch (IOException | RuntimeException e) {
      return Preflight.Status.unreachable(e.toString());
    } finally {
      try {
        client.close();
      } catch (IOException e) {
        // Nothing to do; the client held one connection at most.
      }
    }
  }

  /**
   * 401/403: the identity is rejected. 5xx: the cluster or a proxy is unhealthy, try again. Any
   * other status (Elasticsearch answers 405 when security is disabled, a proxy may answer 404)
   * means the cluster does not enforce authentication on this path, which the bulk requests will
   * show soon enough.
   */
  static Preflight.Status classify(int status) {
    if (status == 401 || status == 403) {
      return Preflight.Status.rejected("HTTP " + status + " from _security/_authenticate");
    }
    if (status >= 500) {
      return Preflight.Status.unreachable("HTTP " + status + " from _security/_authenticate");
    }
    return Preflight.Status.OK;
  }
}
