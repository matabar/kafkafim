package io.github.matabar.kafkafim.route;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import tools.jackson.core.JsonPointer;
import tools.jackson.databind.JsonNode;

/** A compiled {@link ConditionSpec}: all parsing done, evaluation never throws. */
@FunctionalInterface
interface Condition {

  boolean test(JsonNode document, String topic, HeaderLookup headers);

  /**
   * Compiles a spec.
   *
   * @throws IllegalArgumentException for an invalid pointer or regex
   */
  static Condition compile(ConditionSpec spec) {
    Specs.require(spec != null, "condition must not be null");
    return switch (spec) {
      case ConditionSpec.FieldEquals(String field, String value) -> {
        JsonPointer pointer = Specs.pointer(field);
        ScalarValue expected = ScalarValue.of(value);
        yield (document, topic, headers) -> expected.matches(document.at(pointer));
      }
      case ConditionSpec.FieldIn(String field, List<String> values) -> {
        JsonPointer pointer = Specs.pointer(field);
        ScalarValue[] expected = values.stream().map(ScalarValue::of).toArray(ScalarValue[]::new);
        yield (document, topic, headers) -> {
          JsonNode node = document.at(pointer);
          for (ScalarValue value : expected) {
            if (value.matches(node)) {
              return true;
            }
          }
          return false;
        };
      }
      case ConditionSpec.FieldExists(String field, boolean exists) -> {
        JsonPointer pointer = Specs.pointer(field);
        yield (document, topic, headers) -> {
          JsonNode node = document.at(pointer);
          return (!node.isMissingNode() && !node.isNull()) == exists;
        };
      }
      case ConditionSpec.FieldPrefix(String field, String prefix) -> {
        JsonPointer pointer = Specs.pointer(field);
        yield (document, topic, headers) -> {
          JsonNode node = document.at(pointer);
          return node.isString() && node.stringValue().startsWith(prefix);
        };
      }
      case ConditionSpec.FieldMatches(String field, String regex) -> {
        JsonPointer pointer = Specs.pointer(field);
        Pattern pattern = Pattern.compile(regex);
        yield (document, topic, headers) -> {
          JsonNode node = document.at(pointer);
          return node.isString() && matches(pattern, node.stringValue());
        };
      }
      case ConditionSpec.HeaderEquals(String name, String value) -> {
        byte[] expected = value.getBytes(StandardCharsets.UTF_8);
        yield (document, topic, headers) -> {
          byte[] actual = headers.lastValue(name);
          return actual != null && Arrays.equals(actual, expected);
        };
      }
      case ConditionSpec.TopicEquals(String expected) ->
          (document, topic, headers) -> expected.equals(topic);
    };
  }

  /**
   * Anchored match. {@code java.util.regex} recurses per repetition of some constructs (e.g. {@code
   * (a|b)*}), so a long enough value overflows the stack; that counts as no match rather than
   * killing the consumer thread on document content.
   */
  private static boolean matches(Pattern pattern, String value) {
    try {
      return pattern.matcher(value).matches();
    } catch (StackOverflowError e) {
      return false;
    }
  }
}
