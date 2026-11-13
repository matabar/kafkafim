package io.github.matabar.kafkafim.supervisor;

import co.elastic.clients.transport.rest5_client.low_level.Cancellable;
import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.Response;
import co.elastic.clients.transport.rest5_client.low_level.ResponseException;
import co.elastic.clients.transport.rest5_client.low_level.ResponseListener;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.sink.es.ClientSettings;
import io.github.matabar.kafkafim.sink.es.ElasticsearchClients;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
      return authenticate(client);
    } catch (RuntimeException e) {
      return failure(e);
    } finally {
      close(client);
    }
  }

  /**
   * Sends the request and waits for its answer. A stop interrupts a check in progress (DESIGN
   * §4.6): the request is then cancelled, so that closing the client finds no exchange open (a
   * graceful close would wait for it, a forced one makes every I/O thread log a {@code
   * ClosedSelectorException} at ERROR), and the check answers unreachable at once.
   */
  private static Preflight.Status authenticate(Rest5Client client) {
    CompletableFuture<Response> answer = new CompletableFuture<>();
    Cancellable request =
        client.performRequestAsync(
            new Request("GET", "/_security/_authenticate"),
            new ResponseListener() {
              @Override
              public void onSuccess(Response response) {
                answer.complete(response);
              }

              @Override
              public void onFailure(Exception exception) {
                answer.completeExceptionally(exception);
              }
            });
    try {
      answer.get();
      return Preflight.Status.OK;
    } catch (InterruptedException e) {
      request.cancel();
      Thread.currentThread().interrupt();
      return Preflight.Status.unreachable("check interrupted");
    } catch (ExecutionException e) {
      return failure(e.getCause() == null ? e : e.getCause());
    }
  }

  private static Preflight.Status failure(Throwable e) {
    if (e instanceof ResponseException response) {
      return classify(response.getResponse().getStatusCode());
    }
    if (isTlsRejection(e)) {
      // The cluster answered, but its certificate does not match the configured trust (or the
      // client certificate was refused): configuration, retrying would never help.
      return Preflight.Status.rejected("TLS handshake failed: " + e);
    }
    return Preflight.Status.unreachable(e.toString());
  }

  /**
   * Closes the client with the thread's interrupt flag cleared, then restores it: with the flag
   * set, the I/O reactor's shutdown would be cut short and its threads would log a {@code
   * ClosedSelectorException} at ERROR.
   */
  private static void close(Rest5Client client) {
    boolean interrupted = Thread.interrupted();
    try {
      client.close();
    } catch (IOException e) {
      // Nothing to do; the client held one connection at most.
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  static boolean isTlsRejection(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
      if (t instanceof javax.net.ssl.SSLHandshakeException
          || t instanceof javax.net.ssl.SSLPeerUnverifiedException) {
        return true;
      }
    }
    return false;
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
