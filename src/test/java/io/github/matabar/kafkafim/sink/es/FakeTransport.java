package io.github.matabar.kafkafim.sink.es;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * A scripted {@link BulkTransport}: decodes every request body into its actions, records it, and
 * answers with what the test's {@link Handler} returns.
 */
final class FakeTransport implements BulkTransport {

  static final JsonMapper JSON = JsonMapper.builder().build();

  /** One decoded action of a request. */
  record Sent(String op, JsonNode metadata, String source) {
    String id() {
      JsonNode id = metadata.get("_id");
      return id == null ? null : id.asString();
    }
  }

  /** One request as the transport saw it. */
  record Request(List<Sent> actions, int bodyBytes) {
    List<String> ids() {
      return actions.stream().map(Sent::id).toList();
    }
  }

  /** Answers a request. */
  @FunctionalInterface
  interface Handler {
    Result answer(Request request, int call) throws IOException;
  }

  private final Handler handler;
  private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
  private final AtomicInteger calls = new AtomicInteger();
  private final AtomicInteger concurrent = new AtomicInteger();
  private final AtomicInteger maxConcurrent = new AtomicInteger();

  FakeTransport(Handler handler) {
    this.handler = handler;
  }

  @Override
  public Result send(byte[] body) throws IOException {
    int now = concurrent.incrementAndGet();
    maxConcurrent.accumulateAndGet(now, Math::max);
    try {
      Request request = decode(body);
      requests.add(request);
      return handler.answer(request, calls.incrementAndGet());
    } finally {
      concurrent.decrementAndGet();
    }
  }

  List<Request> requests() {
    synchronized (requests) {
      return List.copyOf(requests);
    }
  }

  int maxConcurrent() {
    return maxConcurrent.get();
  }

  static Request decode(byte[] body) {
    String text = new String(body, UTF_8);
    if (!text.endsWith("\n")) {
      throw new AssertionError("body must end with a line feed");
    }
    String[] lines = text.substring(0, text.length() - 1).split("\n", -1);
    if (lines.length % 2 != 0) {
      throw new AssertionError("odd number of lines");
    }
    List<Sent> actions = new ArrayList<>();
    for (int i = 0; i < lines.length; i += 2) {
      JsonNode line = JSON.readTree(lines[i]);
      String op = line.propertyNames().iterator().next();
      actions.add(new Sent(op, line.get(op), lines[i + 1]));
    }
    return new Request(actions, body.length);
  }

  /** A 200 bulk response with {@code errors: false}. */
  static Result allOk() {
    return new Result(200, "{\"errors\":false,\"items\":[]}".getBytes(UTF_8));
  }

  /** A 200 bulk response with one item per status; non-2xx items get an error of that type. */
  static Result items(Request request, ItemAnswer answer) {
    StringBuilder json = new StringBuilder("{\"errors\":true,\"items\":[");
    for (int i = 0; i < request.actions().size(); i++) {
      Sent sent = request.actions().get(i);
      String[] item = answer.answer(sent);
      int status = Integer.parseInt(item[0]);
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"").append(sent.op()).append("\":{\"status\":").append(status);
      if (item.length > 1) {
        json.append(",\"error\":{\"type\":\"")
            .append(item[1])
            .append("\",\"reason\":\"scripted\"}");
      }
      json.append("}}");
    }
    json.append("]}");
    return new Result(200, json.toString().getBytes(UTF_8));
  }

  /** Returns {@code {status}} or {@code {status, errorType}} for an action. */
  @FunctionalInterface
  interface ItemAnswer {
    String[] answer(Sent sent);
  }

  static Result status(int status) {
    return new Result(status, ("{\"status\":" + status + "}").getBytes(UTF_8));
  }
}
