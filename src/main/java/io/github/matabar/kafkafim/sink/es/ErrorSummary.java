package io.github.matabar.kafkafim.sink.es;

import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.json.JsonFactory;

/**
 * What a WARN log line may say about an Elasticsearch error body (DESIGN §7, ADR-0030): {@code
 * error.type} and the {@code error.root_cause[].type}s, never {@code error.reason}, which can quote
 * the document's field values. Only an authentication or authorization failure keeps its reason
 * ({@code withReason}): it names the user, roles, action and index an operator needs to fix the
 * grant, and no document data. A body that is not JSON, or JSON without an {@code error} object, is
 * summarized by its size alone.
 */
final class ErrorSummary {

  /** At most this many distinct root-cause types are named. */
  static final int MAX_ROOT_CAUSES = 5;

  /** A type longer than this is cut; Elasticsearch's are exception names. */
  static final int MAX_TYPE_CHARS = 128;

  /** A kept reason longer than this is cut. */
  static final int MAX_REASON_CHARS = 1024;

  private static final JsonFactory JSON = JsonFactory.builder().build();

  private ErrorSummary() {}

  /**
   * Summarizes a request-level response body such as {@code {"error": {...}, "status": 429}}.
   *
   * @param body the response body
   * @param withReason whether to keep {@code error.reason} (401/403 only)
   */
  static String ofResponse(byte[] body, boolean withReason) {
    if (body.length == 0) {
      return "empty body";
    }
    try (JsonParser parser = JSON.createParser(ObjectReadContext.empty(), body)) {
      if (parser.nextToken() == JsonToken.START_OBJECT) {
        while (parser.nextToken() == JsonToken.PROPERTY_NAME) {
          String name = parser.currentName();
          JsonToken value = parser.nextToken();
          if ("error".equals(name) && value == JsonToken.START_OBJECT) {
            return error(parser, withReason);
          }
          parser.skipChildren();
        }
        return "JSON without an error object, " + body.length + " bytes";
      }
    } catch (JacksonException e) {
      // not JSON after all; fall through
    }
    return "non-JSON, " + body.length + " bytes";
  }

  /**
   * Summarizes an item's {@code error} value, as {@link BulkResponse.Item#errorJson()} holds it.
   *
   * @param errorJson the item's {@code error} value as JSON
   * @param withReason whether to keep {@code reason} (401/403 only)
   */
  static String ofError(String errorJson, boolean withReason) {
    try (JsonParser parser = JSON.createParser(ObjectReadContext.empty(), errorJson)) {
      if (parser.nextToken() == JsonToken.START_OBJECT) {
        return error(parser, withReason);
      }
    } catch (JacksonException e) {
      // BulkResponse copied it from valid JSON; a summary must not throw anyway
    }
    return "error is not an object";
  }

  /** Reads the error object the parser is at, up to its end or the first invalid token. */
  private static String error(JsonParser parser, boolean withReason) {
    String type = null;
    String reason = null;
    List<String> rootCauses = new ArrayList<>();
    while (parser.nextToken() == JsonToken.PROPERTY_NAME) {
      String name = parser.currentName();
      JsonToken value = parser.nextToken();
      if ("type".equals(name) && value == JsonToken.VALUE_STRING) {
        type = clip(parser.getString(), MAX_TYPE_CHARS);
      } else if ("reason".equals(name) && value == JsonToken.VALUE_STRING && withReason) {
        reason = clip(parser.getString(), MAX_REASON_CHARS);
      } else if ("root_cause".equals(name) && value == JsonToken.START_ARRAY) {
        rootCauses(parser, rootCauses);
      } else {
        parser.skipChildren();
      }
    }
    StringBuilder summary = new StringBuilder("type=").append(type == null ? "unknown" : type);
    if (!rootCauses.isEmpty()) {
      summary.append(", root_cause=").append(rootCauses);
    }
    if (reason != null) {
      summary.append(", reason=").append(reason);
    }
    return summary.toString();
  }

  private static void rootCauses(JsonParser parser, List<String> types) {
    JsonToken element;
    while ((element = parser.nextToken()) != JsonToken.END_ARRAY && element != null) {
      if (element != JsonToken.START_OBJECT) {
        parser.skipChildren();
        continue;
      }
      while (parser.nextToken() == JsonToken.PROPERTY_NAME) {
        String name = parser.currentName();
        JsonToken value = parser.nextToken();
        if ("type".equals(name) && value == JsonToken.VALUE_STRING) {
          String type = clip(parser.getString(), MAX_TYPE_CHARS);
          if (types.size() < MAX_ROOT_CAUSES && !types.contains(type)) {
            types.add(type);
          }
        } else {
          parser.skipChildren();
        }
      }
    }
  }

  private static String clip(@Nullable String text, int max) {
    if (text == null) {
      return "";
    }
    return text.length() <= max ? text : text.substring(0, max) + "…";
  }
}
