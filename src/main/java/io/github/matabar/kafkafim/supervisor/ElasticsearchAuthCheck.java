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
import java.io.Closeable;
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
      if (Thread.currentThread().isInterrupted()) {
        closeAfterInterrupt(client);
      } else {
        close(client);
      }
    }
  }

  /**
   * Sends the request, waits for its answer and classifies its status: the client hands back every
   * status below 500 as an ordinary response, 401 and 403 included, and reports only 5xx as a
   * {@link ResponseException}. A stop interrupts a check in progress (DESIGN §4.6): the request is
   * then cancelled, so that closing the client usually finds no exchange open, and the check
   * answers unreachable at once ({@link #closeAfterInterrupt} covers the cancel that has not taken
   * effect yet).
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
      return classify(answer.get().getStatusCode());
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

  /** Closes the client of a check that ran to its end. */
  private static void close(Rest5Client client) {
    try {
      client.close();
    } catch (IOException e) {
      // Nothing to do; the client held one connection at most.
    }
  }

  /**
   * Closes the client of an interrupted check on a thread of its own, so the stop that interrupted
   * it does not wait for the close. The client only closes gracefully: it waits up to 5s for its
   * I/O reactor to shut down, and that takes the whole 5s when the cancelled request has not been
   * torn down yet (an interrupt within milliseconds of the check's start, on a busy machine; #181).
   * The new thread also starts with its interrupt flag clear: with the flag set, the reactor's
   * shutdown would be cut short at once and forced. A close that does wait out the 5s is forced
   * too, and a forced shutdown can make an I/O thread log a {@code ClosedSelectorException} at
   * ERROR; that line is harmless, and rare.
   *
   * @param client the client to close
   * @return the closing thread, for tests
   */
  static Thread closeAfterInterrupt(Closeable client) {
    return Thread.ofVirtual()
        .name("kafkafim-preflight-close")
        .start(() -> {
          try {
            client.close();
          } catch (IOException e) {
            // Nothing to do; the client held one connection at most.
          }
        });
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
