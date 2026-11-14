package io.github.matabar.kafkafim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.grafana.LgtmStackContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Import(TestcontainersConfiguration.class)
@SpringBootTest(
    properties = {
      // The chart's downward-API value; application.yaml makes it service.instance.id.
      "POD_NAME=" + KafkafimApplicationIntegrationTest.POD_NAME,
      // Log correlation must not depend on the trace being sampled.
      "management.tracing.sampling.probability=0.0",
      "management.opentelemetry.logging.export.schedule-delay=200ms"
    })
// Close the context (and stop its containers) after the class instead of caching it until the
// fork exits; with reuseForks the fork goes on to run other container-heavy classes.
@DirtiesContext
@ExtendWith(OutputCaptureExtension.class)
class KafkafimApplicationIntegrationTest {

  static final String POD_NAME = "kafkafim-default-7d9f8c6b5-l0g5x";

  private static final Logger LOG =
      LoggerFactory.getLogger(KafkafimApplicationIntegrationTest.class);

  @Autowired private Tracer tracer;
  @Autowired private LgtmStackContainer lgtm;

  @Test
  void contextLoads() {}

  /**
   * DESIGN §10 Logs against a real OTLP backend (ADR-0044): a line logged through SLF4J inside a
   * span reaches the Loki of the Grafana LGTM container over OTLP, under the resource attributes
   * metrics and traces carry, with the span's trace and span id, its logger, thread and exception.
   * The same line on stdout is plain text. {@code OtlpExportIntegrationTest} covers the endpoint
   * property and the export headers.
   */
  @Test
  void logLineReachesTheOtlpBackendAsRecordAndStdoutAsText(CapturedOutput output) {
    String marker = "otlp-log-export-" + UUID.randomUUID();
    String loggingThread = Thread.currentThread().getName();
    Span span = tracer.nextSpan().name("otlp-log-export-test").start();
    try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
      LOG.warn("{} logged inside a span", marker, new IllegalStateException("with a cause"));
    } finally {
      span.end();
    }
    String traceId = span.context().traceId();
    String spanId = span.context().spanId();

    assertThat(output.getOut().lines().filter(line -> line.contains(marker)))
        .singleElement()
        .satisfies(line -> {
          assertThat(line).doesNotStartWith("{");
          assertThat(line).contains(" WARN ", traceId, spanId);
        });

    try (HttpClient http = HttpClient.newHttpClient()) {
      await()
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofSeconds(1))
          .untilAsserted(() -> {
            JsonNode stream = lokiStream(http, "{service_name=\"kafkafim\"} |= \"" + marker + "\"");
            assertThat(stream).as("Loki stream of the marker line").isNotNull();
            assertThat(stream.path("service_namespace").asString()).isEqualTo("kafkafim");
            assertThat(stream.path("service_instance_id").asString()).isEqualTo(POD_NAME);
            assertThat(stream.path("severity_text").asString()).isEqualTo("WARN");
            assertThat(stream.path("trace_id").asString()).isEqualTo(traceId);
            assertThat(stream.path("span_id").asString()).isEqualTo(spanId);
            assertThat(stream.path("scope_name").asString())
                .as("stream %s", stream)
                .isEqualTo(KafkafimApplicationIntegrationTest.class.getName());
            assertThat(stream.path("thread_name").asString()).isEqualTo(loggingThread);
            assertThat(stream.path("exception_type").asString())
                .isEqualTo(IllegalStateException.class.getName());
            assertThat(stream.path("exception_stacktrace").asString())
                .contains("KafkafimApplicationIntegrationTest");
          });
    }
  }

  /** Labels and structured metadata of the first stream the LogQL query returns, or null. */
  private JsonNode lokiStream(HttpClient http, String logQl) throws Exception {
    URI uri =
        URI.create(
            lgtm.getLokiUrl()
                + "/loki/api/v1/query_range?since=15m&query="
                + URLEncoder.encode(logQl, StandardCharsets.UTF_8));
    HttpResponse<String> response =
        http.send(
            HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5)).build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as("Loki answer: %s", response.body()).isEqualTo(200);
    JsonNode result = JsonMapper.shared().readTree(response.body()).path("data").path("result");
    return result.isEmpty() ? null : result.get(0).path("stream");
  }
}
