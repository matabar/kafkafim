package io.github.matabar.kafkafim.sink.es;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.json.JsonFactory;

/**
 * A parsed {@code _bulk} response body with HTTP status 2xx. Parsing is streaming: with {@code
 * "errors": false} (Elasticsearch writes it before {@code items}) parsing stops there and every
 * action is acked without reading the items. Otherwise items map back to actions by position.
 *
 * @param errors the response's {@code errors} flag
 * @param items per-action results in request order; empty when {@code errors} is {@code false}
 */
record BulkResponse(boolean errors, List<Item> items) {

  /** The {@code filter_path} kafkafim sends, so a success response is a few bytes per item. */
  static final String FILTER_PATH = "errors,items.*.status,items.*.error";

  private static final JsonFactory JSON = JsonFactory.builder().build();

  private static final BulkResponse ALL_OK = new BulkResponse(false, List.of());

  /**
   * One item of the response.
   *
   * @param status the item's HTTP status
   * @param errorType {@code error.type}, or {@code null} when the item has no error
   * @param errorJson the item's {@code error} object as JSON, or {@code null}
   */
  record Item(int status, @Nullable String errorType, @Nullable String errorJson) {}

  /** The response body is not a bulk response. */
  static final class MalformedResponseException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    MalformedResponseException(String message, @Nullable Throwable cause) {
      super(message, cause, false, false);
    }
  }

  /**
   * Parses a 2xx {@code _bulk} response body.
   *
   * @throws MalformedResponseException if the body is not a JSON object with {@code errors} or
   *     {@code items}
   */
  static BulkResponse parse(byte[] body) {
    try (JsonParser parser = JSON.createParser(ObjectReadContext.empty(), body)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        throw new MalformedResponseException("bulk response is not a JSON object", null);
      }
      Boolean errors = null;
      List<Item> items = null;
      while (parser.nextToken() == JsonToken.PROPERTY_NAME) {
        String name = parser.currentName();
        JsonToken value = parser.nextToken();
        if ("errors".equals(name) && value.isBoolean()) {
          errors = value == JsonToken.VALUE_TRUE;
          if (!errors) {
            return ALL_OK;
          }
        } else if ("items".equals(name) && value == JsonToken.START_ARRAY) {
          items = parseItems(parser);
        } else {
          parser.skipChildren();
        }
      }
      if (items == null) {
        throw new MalformedResponseException("bulk response has no items", null);
      }
      return new BulkResponse(true, List.copyOf(items));
    } catch (JacksonException e) {
      throw new MalformedResponseException("bulk response is not valid JSON", e);
    }
  }

  private static List<Item> parseItems(JsonParser parser) {
    List<Item> items = new ArrayList<>();
    while (parser.nextToken() == JsonToken.START_OBJECT) {
      // {"index": {...}} — one property named after the op
      if (parser.nextToken() != JsonToken.PROPERTY_NAME
          || parser.nextToken() != JsonToken.START_OBJECT) {
        throw new MalformedResponseException("bulk item is not {\"<op>\": {...}}", null);
      }
      items.add(parseItem(parser));
      if (parser.nextToken() != JsonToken.END_OBJECT) {
        throw new MalformedResponseException("bulk item has more than one op", null);
      }
    }
    if (parser.currentToken() != JsonToken.END_ARRAY) {
      throw new MalformedResponseException("bulk items element is not an object", null);
    }
    return items;
  }

  private static Item parseItem(JsonParser parser) {
    int status = -1;
    String errorType = null;
    String errorJson = null;
    while (parser.nextToken() == JsonToken.PROPERTY_NAME) {
      String name = parser.currentName();
      JsonToken value = parser.nextToken();
      if ("status".equals(name) && value == JsonToken.VALUE_NUMBER_INT) {
        status = parser.getIntValue();
      } else if ("error".equals(name)) {
        errorJson = copy(parser);
        errorType = value == JsonToken.START_OBJECT ? topLevelType(errorJson) : null;
        if (errorType == null) {
          errorType = "unknown";
        }
      } else {
        parser.skipChildren();
      }
    }
    if (status < 0) {
      throw new MalformedResponseException("bulk item has no status", null);
    }
    return new Item(status, errorType, errorJson);
  }

  /** Copies the current value (object, array or scalar) as compact JSON. */
  private static String copy(JsonParser parser) {
    StringWriter out = new StringWriter();
    try (JsonGenerator generator = JSON.createGenerator(ObjectWriteContext.empty(), out)) {
      generator.copyCurrentStructure(parser);
    }
    return out.toString();
  }

  private static @Nullable String topLevelType(String errorJson) {
    try (JsonParser parser = JSON.createParser(ObjectReadContext.empty(), errorJson)) {
      parser.nextToken();
      while (parser.nextToken() == JsonToken.PROPERTY_NAME) {
        String name = parser.currentName();
        JsonToken value = parser.nextToken();
        if ("type".equals(name) && value == JsonToken.VALUE_STRING) {
          return parser.getString();
        }
        parser.skipChildren();
      }
      return null;
    }
  }
}
