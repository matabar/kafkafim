package io.github.matabar.kafkafim.supervisor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The identity check against a server that answers {@code GET /_security/_authenticate} with a
 * chosen status (DESIGN §4.6). The low-level client returns every status below 500 as an ordinary
 * response ({@code Rest5ClientStatusContractTest}), so a rejected identity arrives without an
 * exception and the check must read the status itself (#149).
 */
class ElasticsearchAuthCheckStatusTest {

  static final AtomicInteger STATUS = new AtomicInteger();
  static final AtomicReference<String> REQUESTED = new AtomicReference<>();
  static HttpServer server;
  static ElasticsearchAuthCheck check;

  @BeforeAll
  static void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          REQUESTED.set(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
          byte[] body = "{}".getBytes(UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(STATUS.get(), body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
    check =
        new ElasticsearchAuthCheck(
            TestConfigs.bind(
                    """
                    kafkafim:
                      elasticsearch:
                        clusters:
                          es: { uris: ["http://127.0.0.1:%d"] }
                    """
                        .formatted(server.getAddress().getPort()))
                .elasticsearch()
                .clusters());
  }

  @AfterAll
  static void stop() {
    server.stop(0);
  }

  @BeforeEach
  void forget() {
    REQUESTED.set(null);
  }

  @ParameterizedTest
  @ValueSource(ints = {401, 403})
  void rejectedIdentityIsConfigurationProblem(int status) {
    Preflight.Status verdict = answering(status);

    assertThat(verdict.kind()).isEqualTo(Preflight.Kind.CONFIGURATION);
    assertThat(verdict.detail()).isEqualTo("HTTP " + status + " from _security/_authenticate");
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 404, 405})
  void acceptedIdentityOrClusterWithoutSecurityPasses(int status) {
    assertThat(answering(status)).isEqualTo(Preflight.Status.OK);
  }

  @ParameterizedTest
  @ValueSource(ints = {500, 503})
  void unhealthyClusterIsTransient(int status) {
    Preflight.Status verdict = answering(status);

    assertThat(verdict.kind()).isEqualTo(Preflight.Kind.TRANSIENT);
    assertThat(verdict.detail()).isEqualTo("HTTP " + status + " from _security/_authenticate");
  }

  private static Preflight.Status answering(int status) {
    STATUS.set(status);
    Preflight.Status verdict = check.check("es", null);
    assertThat(REQUESTED).hasValue("GET /_security/_authenticate");
    return verdict;
  }
}
