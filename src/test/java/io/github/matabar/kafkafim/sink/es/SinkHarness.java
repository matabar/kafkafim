package io.github.matabar.kafkafim.sink.es;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.Response;
import co.elastic.clients.transport.rest5_client.low_level.ResponseException;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.github.matabar.kafkafim.config.Pipeline.Op;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Shared helpers for the sink integration tests: outcome capture and a raw admin client. */
final class SinkHarness {

  static final JsonMapper JSON = JsonMapper.builder().build();

  private SinkHarness() {}

  static BulkAction<String> doc(String index, String id, String source) {
    return doc(index, id, Op.INDEX, null, source);
  }

  static BulkAction<String> doc(
      String index, String id, Op op, @Nullable Long version, String source) {
    return new BulkAction<>(index, id, op, version, null, source.getBytes(UTF_8), id);
  }

  /** Performs a request as the admin and returns the parsed body; fails on a non-2xx status. */
  static JsonNode call(Rest5Client admin, String method, String path, @Nullable String json)
      throws IOException {
    return JSON.readTree(raw(admin, method, path, json));
  }

  /**
   * Performs a request as the admin and returns the raw body; fails on a non-2xx status.
   *
   * <p>{@link Rest5Client#performRequest} throws {@link ResponseException} only for 5xx: it returns
   * every status below 500 as a normal response (see {@code Rest5ClientStatusContractTest}), so the
   * status has to be checked here, not by catching the exception.
   */
  static String raw(Rest5Client admin, String method, String path, @Nullable String json)
      throws IOException {
    Request request = new Request(method, path);
    if (json != null) {
      request.setJsonEntity(json);
    }
    Response response;
    try {
      response = admin.performRequest(request);
    } catch (ResponseException e) {
      response = e.getResponse();
    }
    String body;
    try {
      body = response.getEntity() == null ? "" : EntityUtils.toString(response.getEntity(), UTF_8);
    } catch (org.apache.hc.core5.http.ParseException e) {
      throw new IOException(e);
    }
    int status = response.getStatusCode();
    if (status / 100 != 2) {
      throw new AssertionError(method + " " + path + " failed: HTTP " + status + ", body: " + body);
    }
    return body;
  }

  /** Records outcomes by handle; fails on a second outcome for one handle. */
  static final class Outcomes implements OutcomeCallback<String> {
    final Map<String, ActionOutcome> byHandle = new ConcurrentHashMap<>();

    @Override
    public void onOutcome(BulkAction<String> action, ActionOutcome outcome) {
      ActionOutcome previous = byHandle.putIfAbsent(action.handle(), outcome);
      assertThat(previous).as("second outcome for %s", action.handle()).isNull();
    }

    List<ActionOutcome.Dlq> dlq() {
      return byHandle.values().stream()
          .filter(ActionOutcome.Dlq.class::isInstance)
          .map(ActionOutcome.Dlq.class::cast)
          .toList();
    }
  }

  /** Counts listener events. */
  static final class Events implements SinkListener {
    final AtomicInteger requestRetries = new AtomicInteger();
    final AtomicInteger authFailures = new AtomicInteger();
    final List<String> authReasons = new CopyOnWriteArrayList<>();
    final List<CircuitBreaker.State> breakerStates = new CopyOnWriteArrayList<>();

    @Override
    public void onRequestRetry(
        SinkKey key, int status, @Nullable Throwable error, int attempt, Duration delay) {
      requestRetries.incrementAndGet();
    }

    @Override
    public void onAuthFailure(SinkKey key, int status, String reason) {
      authFailures.incrementAndGet();
      authReasons.add(status + " " + reason);
    }

    @Override
    public void onBreakerStateChange(
        String cluster, CircuitBreaker.State from, CircuitBreaker.State to) {
      breakerStates.add(to);
    }
  }
}
