package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import io.github.matabar.kafkafim.KafkafimApplication;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves the OTLP export contract against the real application.
 *
 * <p>It checks four things:
 *
 * <ul>
 *   <li>the header contract the Helm chart renders (docs/HELM.md, {@code otel}): a non-secret
 *       header from {@code OTEL_EXPORTER_OTLP_HEADERS} and a secret header from a {@code
 *       configtree} file both arrive on the OTLP metrics ({@code /v1/metrics}), trace ({@code
 *       /v1/traces}) and log ({@code /v1/logs}) export requests;
 *   <li>the three resources carry {@code service.instance.id} from the chart's {@code POD_NAME};
 *   <li>log records leave over OTLP, the ones logged before the OpenTelemetry SDK existed included
 *       (DESIGN §10 Logs);
 *   <li>timers export in seconds (unit {@code seconds}, bucket bounds in seconds), so the OTLP
 *       names match {@code /actuator/prometheus} and the PromQL in deploy/grafana/ (DESIGN §10).
 * </ul>
 *
 * <p>The application runs in a child JVM because Spring Boot maps {@code OTEL_*} variables from the
 * real process environment only, which a test cannot set in its own JVM. The child gets exactly the
 * environment a pod gets from the chart, with the secret header files laid out as the chart's
 * {@code otel.secretHeaders} projected volume mounts them, and exports to a local HTTP capture. No
 * Docker is needed: nothing connects to Kafka or Elasticsearch at startup yet.
 */
class OtlpExportIntegrationTest {

  /**
   * Mount paths below the configtree root. Keep in sync with {@code
   * kafkafim.otelSecretHeaderMountPaths} in charts/kafkafim/templates/_helpers.tpl (asserted by
   * charts/kafkafim/tests/deployment_test.yaml).
   */
  private static final List<String> SECRET_HEADER_DIRS =
      List.of(
          "management/otlp/metrics/export/headers",
          "management/opentelemetry/tracing/export/otlp/headers",
          "management/opentelemetry/logging/export/otlp/headers");

  private static final String SECRET_VALUE = "Bearer s3cr3t-apm-token";

  /** Tomcat's startup line, which names the port the child bound for {@code SERVER_PORT=0}. */
  private static final Pattern SERVER_STARTED = Pattern.compile("Tomcat started on port (\\d+)");

  private static final String POD_NAME = "kafkafim-default-7d9f8c6b5-x2x4q";

  private record Captured(String path, Headers headers, byte[] body) {}

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
          byte[] body;
          try (InputStream in = exchange.getRequestBody()) {
            body = in.readAllBytes();
          }
          captured.add(
              new Captured(exchange.getRequestURI().getPath(), exchange.getRequestHeaders(), body));
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
  void exportCarriesHeadersInstanceIdAndSecondsUnits() throws Exception {
    Path secrets = temp.resolve("secrets");
    for (String dir : SECRET_HEADER_DIRS) {
      Path headerDir = Files.createDirectories(secrets.resolve(dir));
      // Kubernetes Secret files often end with a newline; configtree trims it.
      Files.writeString(headerDir.resolve("Authorization"), SECRET_VALUE + "\n");
      // A dashed header name, as an API-key style header would be.
      Files.writeString(headerDir.resolve("X-Api-Key"), "k1\n");
    }
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
                || name.startsWith("KUBERNETES_")
                || name.startsWith("KAFKAFIM_")
                || name.equals("JAVA_TOOL_OPTIONS")
                || name.equals("POD_NAME")
                || name.equals("HOSTNAME")
                || name.equals("JDK_JAVA_OPTIONS"));
    // As rendered by the chart (otel.endpoint, otel.headers, the configtree import).
    env.put("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:" + collector.getAddress().getPort());
    // The chart percent-encodes "%" and "," in otel.headers values: "a,b%c" -> "a%2Cb%25c".
    env.put("OTEL_EXPORTER_OTLP_HEADERS", "x-kafkafim-tenant=ingest,x-kafkafim-list=a%2Cb%25c");
    env.put("SPRING_CONFIG_IMPORT", "configtree:" + secrets.toAbsolutePath() + "/");
    env.put("POD_NAME", POD_NAME); // the chart's downward API value (metadata.name)
    // Test-only: no compose stack, every request sampled, fast export. The child binds an
    // ephemeral port itself and logs it, so no other process can take it between check and use.
    env.put("SPRING_DOCKER_COMPOSE_ENABLED", "false");
    env.put("SERVER_PORT", "0");
    env.put("MANAGEMENT_OTLP_METRICS_EXPORT_STEP", "1s");
    env.put("MANAGEMENT_TRACING_SAMPLING_PROBABILITY", "1.0");
    env.put("OTEL_BSP_SCHEDULE_DELAY", "100");
    env.put("OTEL_BLRP_SCHEDULE_DELAY", "100");
    app = builder.start();

    try (HttpClient http = HttpClient.newHttpClient()) {
      await()
          .atMost(Duration.ofSeconds(90))
          .pollInterval(Duration.ofMillis(500))
          .untilAsserted(() -> {
            assertThat(app.isAlive()).as("application process is running").isTrue();
            OptionalInt serverPort = serverPort();
            assertThat(serverPort).as("server port in the application output").isPresent();
            HttpRequest probe =
                HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + serverPort.getAsInt() + "/otlp-probe"))
                    .timeout(Duration.ofSeconds(2))
                    .build();
            try {
              // A non-actuator request is observed and produces a server span.
              http.send(probe, HttpResponse.BodyHandlers.discarding());
            } catch (IOException e) {
              // Not answering yet.
            }
            assertThat(paths()).contains("/v1/metrics", "/v1/traces", "/v1/logs");
            // Exports before the pipeline meters were registered lack the e2e timer.
            assertThat(e2eMetrics(parsedMetrics()))
                .as("%s in the OTLP metrics", TelemetryNames.E2E_LATENCY)
                .isNotEmpty();
          });
    } catch (ConditionTimeoutException | AssertionError e) {
      throw new AssertionError(
          "No metrics, trace and log export seen; paths="
              + paths()
              + "\n--- application output ---\n"
              + Files.readString(appLog, StandardCharsets.UTF_8),
          e);
    }

    for (String path : List.of("/v1/metrics", "/v1/traces", "/v1/logs")) {
      List<Headers> requests =
          captured.stream().filter(c -> c.path().equals(path)).map(Captured::headers).toList();
      assertThat(requests)
          .as("%s export requests", path)
          .isNotEmpty()
          .allSatisfy(headers -> {
            assertThat(headers.getFirst("x-kafkafim-tenant")).isEqualTo("ingest");
            assertThat(headers.getFirst("x-kafkafim-list")).isEqualTo("a,b%c");
            assertThat(headers.getFirst("Authorization")).isEqualTo(SECRET_VALUE);
            assertThat(headers.getFirst("X-Api-Key")).isEqualTo("k1");
          });
    }

    List<ExportMetricsServiceRequest> metrics = parsedMetrics();
    List<ExportTraceServiceRequest> traces = new ArrayList<>();
    for (Captured c : captured) {
      if (c.path().equals("/v1/traces")) {
        traces.add(ExportTraceServiceRequest.parseFrom(c.body()));
      }
    }
    List<ExportLogsServiceRequest> logs = new ArrayList<>();
    for (Captured c : captured) {
      if (c.path().equals("/v1/logs")) {
        logs.add(ExportLogsServiceRequest.parseFrom(c.body()));
      }
    }
    List<Resource> resources = new ArrayList<>();
    metrics.forEach(r ->
        r.getResourceMetricsList().stream()
            .map(ResourceMetrics::getResource)
            .forEach(resources::add));
    traces.forEach(r ->
        r.getResourceSpansList().stream().map(ResourceSpans::getResource).forEach(resources::add));
    logs.forEach(r ->
        r.getResourceLogsList().stream().map(ResourceLogs::getResource).forEach(resources::add));
    assertThat(resources)
        .as("metric, trace and log resources")
        .isNotEmpty()
        .allSatisfy(resource ->
            assertThat(attributes(resource))
                .containsEntry("service.instance.id", POD_NAME)
                .containsEntry("service.name", "kafkafim")
                .containsEntry("service.namespace", "kafkafim"));

    // Spring Boot logs this line before the application context, and so the OpenTelemetry SDK,
    // exists: the appender kept it and replayed it.
    List<LogRecord> records =
        logs.stream()
            .flatMap(r -> r.getResourceLogsList().stream())
            .flatMap(r -> r.getScopeLogsList().stream())
            .flatMap(scope -> scope.getLogRecordsList().stream())
            .toList();
    assertThat(records)
        .as("exported log records")
        .anySatisfy(record -> {
          assertThat(record.getBody().getStringValue()).startsWith("Starting KafkafimApplication");
          assertThat(record.getSeverityText()).isEqualTo("INFO");
        });

    // The demo pipeline of the local profile registers its e2e timer at startup (DESIGN §10).
    List<Metric> e2e = e2eMetrics(metrics);
    assertThat(e2e)
        .as("%s in the OTLP metrics", TelemetryNames.E2E_LATENCY)
        .isNotEmpty()
        .allSatisfy(m -> {
          // Micrometer sends the unit as a word; Prometheus' OTLP translation appends it as the
          // _seconds suffix (the default "milliseconds" became _milliseconds).
          assertThat(m.getUnit()).isEqualTo("seconds");
          assertThat(m.hasHistogram()).isTrue();
          assertThat(m.getHistogram().getDataPointsList())
              .isNotEmpty()
              .allSatisfy(p -> assertThat(p.getExplicitBoundsList()).contains(0.05, 3600.0));
        });
  }

  private List<ExportMetricsServiceRequest> parsedMetrics() throws IOException {
    List<ExportMetricsServiceRequest> metrics = new ArrayList<>();
    for (Captured c : captured) {
      if (c.path().equals("/v1/metrics")) {
        metrics.add(ExportMetricsServiceRequest.parseFrom(c.body()));
      }
    }
    return metrics;
  }

  private static List<Metric> e2eMetrics(List<ExportMetricsServiceRequest> metrics) {
    return metrics.stream()
        .flatMap(r -> r.getResourceMetricsList().stream())
        .flatMap(r -> r.getScopeMetricsList().stream())
        .map(ScopeMetrics::getMetricsList)
        .flatMap(List::stream)
        .filter(m -> m.getName().equals(TelemetryNames.E2E_LATENCY))
        .toList();
  }

  private static Map<String, String> attributes(Resource resource) {
    Map<String, String> attributes = new HashMap<>();
    for (KeyValue kv : resource.getAttributesList()) {
      attributes.put(kv.getKey(), kv.getValue().getStringValue());
    }
    return attributes;
  }

  private List<String> paths() {
    return captured.stream().map(Captured::path).distinct().toList();
  }

  private OptionalInt serverPort() throws IOException {
    Matcher started = SERVER_STARTED.matcher(Files.readString(appLog, StandardCharsets.UTF_8));
    return started.find()
        ? OptionalInt.of(Integer.parseInt(started.group(1)))
        : OptionalInt.empty();
  }
}
