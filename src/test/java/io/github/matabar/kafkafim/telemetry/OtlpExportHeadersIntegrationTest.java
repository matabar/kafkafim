package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import io.github.matabar.kafkafim.KafkafimApplication;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves the OTLP header contract the Helm chart renders (docs/HELM.md, {@code otel}): a non-secret
 * header from {@code OTEL_EXPORTER_OTLP_HEADERS} and a secret header from a {@code configtree} file
 * both arrive on the OTLP metrics ({@code /v1/metrics}) and trace ({@code /v1/traces}) export
 * requests.
 *
 * <p>The application runs in a child JVM because Spring Boot maps {@code OTEL_*} variables from the
 * real process environment only, which a test cannot set in its own JVM. The child gets exactly the
 * environment a pod gets from the chart, with the secret header files laid out as the chart's
 * {@code otel.secretHeaders} projected volume mounts them, and exports to a local HTTP capture. No
 * Docker is needed: nothing connects to Kafka or Elasticsearch at startup yet.
 */
class OtlpExportHeadersIntegrationTest {

  /** Mount paths below the configtree root, as in charts/kafkafim/templates/deployment.yaml. */
  private static final List<String> SECRET_HEADER_DIRS =
      List.of(
          "management/otlp/metrics/export/headers",
          "management/opentelemetry/tracing/export/otlp/headers");

  private static final String SECRET_VALUE = "Bearer s3cr3t-apm-token";

  private record Captured(String path, Headers headers) {}

  private final Queue<Captured> captured = new ConcurrentLinkedQueue<>();

  @TempDir Path temp;

  private HttpServer collector;
  private Process app;
  private Path appLog;

  @BeforeEach
  void startCollector() throws IOException {
    collector = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    collector.createContext(
        "/",
        exchange -> {
          try (InputStream body = exchange.getRequestBody()) {
            body.readAllBytes();
          }
          captured.add(
              new Captured(exchange.getRequestURI().getPath(), exchange.getRequestHeaders()));
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });
    collector.start();
  }

  @AfterEach
  void stop() throws InterruptedException {
    if (app != null) {
      app.destroy();
      if (!app.waitFor(30, TimeUnit.SECONDS)) {
        app.destroyForcibly();
      }
    }
    collector.stop(0);
  }

  @Test
  void plainAndSecretHeadersReachMetricsAndTraceExport() throws Exception {
    Path secrets = temp.resolve("secrets");
    for (String dir : SECRET_HEADER_DIRS) {
      Path headerDir = Files.createDirectories(secrets.resolve(dir));
      // Kubernetes Secret files often end with a newline; configtree trims it.
      Files.writeString(headerDir.resolve("Authorization"), SECRET_VALUE + "\n");
    }
    final int serverPort = freePort();
    appLog = temp.resolve("app.log");

    ProcessBuilder builder =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                KafkafimApplication.class.getName())
            .redirectErrorStream(true)
            .redirectOutput(appLog.toFile());
    Map<String, String> env = builder.environment();
    env.keySet()
        .removeIf(name ->
            name.startsWith("OTEL_")
                || name.startsWith("SPRING_")
                || name.startsWith("MANAGEMENT_")
                || name.startsWith("KUBERNETES_"));
    // As rendered by the chart (otel.endpoint, otel.headers, the configtree import).
    env.put("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:" + collector.getAddress().getPort());
    env.put("OTEL_EXPORTER_OTLP_HEADERS", "x-kafkafim-tenant=ingest");
    env.put("SPRING_CONFIG_IMPORT", "configtree:" + secrets.toAbsolutePath() + "/");
    // Test-only: no compose stack, a free port, every request sampled, fast export.
    env.put("SPRING_DOCKER_COMPOSE_ENABLED", "false");
    env.put("SERVER_PORT", Integer.toString(serverPort));
    env.put("MANAGEMENT_OTLP_METRICS_EXPORT_STEP", "1s");
    env.put("MANAGEMENT_TRACING_SAMPLING_PROBABILITY", "1.0");
    env.put("OTEL_BSP_SCHEDULE_DELAY", "100");
    app = builder.start();

    try (HttpClient http = HttpClient.newHttpClient()) {
      HttpRequest probe =
          HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + serverPort + "/otlp-probe"))
              .timeout(Duration.ofSeconds(2))
              .build();
      await()
          .atMost(Duration.ofSeconds(90))
          .pollInterval(Duration.ofMillis(500))
          .untilAsserted(() -> {
            assertThat(app.isAlive()).as("application process is running").isTrue();
            try {
              // A non-actuator request is observed and produces a server span.
              http.send(probe, HttpResponse.BodyHandlers.discarding());
            } catch (IOException e) {
              // Not listening yet.
            }
            assertThat(paths()).contains("/v1/metrics", "/v1/traces");
          });
    } catch (ConditionTimeoutException | AssertionError e) {
      throw new AssertionError(
          "No metrics and trace export seen; paths="
              + paths()
              + "\n--- application output ---\n"
              + Files.readString(appLog, StandardCharsets.UTF_8),
          e);
    }

    for (String path : List.of("/v1/metrics", "/v1/traces")) {
      List<Headers> requests =
          captured.stream().filter(c -> c.path().equals(path)).map(Captured::headers).toList();
      assertThat(requests)
          .as("%s export requests", path)
          .isNotEmpty()
          .allSatisfy(headers -> {
            assertThat(headers.getFirst("x-kafkafim-tenant")).isEqualTo("ingest");
            assertThat(headers.getFirst("Authorization")).isEqualTo(SECRET_VALUE);
          });
    }
  }

  private List<String> paths() {
    return captured.stream().map(Captured::path).distinct().toList();
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
