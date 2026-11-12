package io.github.matabar.kafkafim.sink.es;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.ResponseException;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Pins how the low-level Elasticsearch client reports HTTP statuses: unlike the HttpClient 4 {@code
 * RestClient}, {@link Rest5Client#performRequest} returns every status below 500 as a normal
 * response and throws {@link ResponseException} only for 5xx. Code that treats "no exception" as
 * success silently accepts 4xx replies; that is how the T49 provisioning flake (#96) hid a 4xx as a
 * 2xx. If a client upgrade changes this, this test says so first.
 */
class Rest5ClientStatusContractTest {

  static HttpServer server;
  static Rest5Client client;

  @BeforeAll
  static void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    // Answers /<status> with that status and a small JSON body.
    server.createContext(
        "/",
        exchange -> {
          int status = Integer.parseInt(exchange.getRequestURI().getPath().substring(1));
          byte[] body = ("{\"status\":" + status + "}").getBytes(UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(status, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
    client =
        Rest5Client.builder(
                new HttpHost(
                    "http", server.getAddress().getHostString(), server.getAddress().getPort()))
            .build();
  }

  @AfterAll
  static void stop() throws IOException {
    client.close();
    server.stop(0);
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 201, 400, 401, 403, 404, 409, 429})
  void statusesBelow500AreReturnedNotThrown(int status) throws IOException {
    assertThat(client.performRequest(new Request("GET", "/" + status)).getStatusCode())
        .isEqualTo(status);
  }

  @ParameterizedTest
  @ValueSource(ints = {500, 503})
  void statusesFrom500AreThrown(int status) {
    assertThatThrownBy(() -> client.performRequest(new Request("GET", "/" + status)))
        .isInstanceOfSatisfying(
            ResponseException.class,
            e -> assertThat(e.getResponse().getStatusCode()).isEqualTo(status));
  }
}
