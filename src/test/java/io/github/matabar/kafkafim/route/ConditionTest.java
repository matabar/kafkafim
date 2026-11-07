package io.github.matabar.kafkafim.route;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.matabar.kafkafim.route.ConditionSpec.FieldEquals;
import io.github.matabar.kafkafim.route.ConditionSpec.FieldExists;
import io.github.matabar.kafkafim.route.ConditionSpec.FieldIn;
import io.github.matabar.kafkafim.route.ConditionSpec.FieldMatches;
import io.github.matabar.kafkafim.route.ConditionSpec.FieldPrefix;
import io.github.matabar.kafkafim.route.ConditionSpec.HeaderEquals;
import io.github.matabar.kafkafim.route.ConditionSpec.TopicEquals;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.JsonNodeFactory;

/** Every condition type: match, no match, missing field, wrong type. */
class ConditionTest {

  private static final JsonMapper JSON = JsonMapper.builder().build();

  static JsonNode doc(String json) {
    return JSON.readTree(json);
  }

  static boolean test(ConditionSpec spec, String json) {
    return Condition.compile(spec).test(doc(json), "t", HeaderLookup.NONE);
  }

  @Nested
  class Equals {

    @Test
    void stringField() {
      assertThat(test(new FieldEquals("/region", "eu"), "{\"region\":\"eu\"}")).isTrue();
      assertThat(test(new FieldEquals("/region", "eu"), "{\"region\":\"us\"}")).isFalse();
      assertThat(test(new FieldEquals("/region", "eu"), "{\"region\":\"EU\"}")).isFalse();
    }

    @Test
    void nestedPointerAndArrayIndex() {
      assertThat(test(new FieldEquals("/a/b/1", "y"), "{\"a\":{\"b\":[\"x\",\"y\"]}}")).isTrue();
      assertThat(test(new FieldEquals("/a~1b/c~0d", "v"), "{\"a/b\":{\"c~d\":\"v\"}}")).isTrue();
    }

    @ParameterizedTest(name = "document {0} equals ''{1}'' → {2}")
    @CsvSource({
      "1, 1, true",
      "1, 1.0, true",
      "1.0, 1, true",
      "1, 1e0, true",
      "100, 1e2, true",
      "1.50, 1.5, true",
      "0.1, 0.1, true",
      "-7, -7, true",
      "1, 2, false",
      "1.5, 1, false",
      "1, one, false",
      "9223372036854775807, 9223372036854775807, true",
      "9223372036854775808, 9223372036854775808, true",
      "9223372036854775808, 9223372036854775807, false",
      "1e400, 1e400, false",
    })
    void numericFieldComparesByValue(String json, String value, boolean expected) {
      assertThat(test(new FieldEquals("/n", value), "{\"n\":" + json + "}")).isEqualTo(expected);
    }

    @Test
    void bigDecimalAndFloatNodes() {
      JsonNodeFactory nodes = JsonNodeFactory.instance;
      Condition equals = Condition.compile(new FieldEquals("/n", "0.1"));
      assertThat(
              equals.test(
                  nodes.objectNode().put("n", new BigDecimal("0.10")), "t", HeaderLookup.NONE))
          .isTrue();
      assertThat(equals.test(nodes.objectNode().put("n", 0.1f), "t", HeaderLookup.NONE)).isTrue();
      assertThat(equals.test(nodes.objectNode().put("n", Double.NaN), "t", HeaderLookup.NONE))
          .isFalse();
      assertThat(
              Condition.compile(new FieldEquals("/n", "NaN"))
                  .test(nodes.objectNode().put("n", Double.NaN), "t", HeaderLookup.NONE))
          .isFalse();
    }

    @Test
    void booleanField() {
      assertThat(test(new FieldEquals("/b", "true"), "{\"b\":true}")).isTrue();
      assertThat(test(new FieldEquals("/b", "false"), "{\"b\":false}")).isTrue();
      assertThat(test(new FieldEquals("/b", "true"), "{\"b\":false}")).isFalse();
      assertThat(test(new FieldEquals("/b", "TRUE"), "{\"b\":true}")).isFalse();
    }

    @Test
    void stringsAndNumbersNeverEqualEachOther() {
      assertThat(test(new FieldEquals("/n", "1"), "{\"n\":\"1\"}")).isTrue();
      assertThat(test(new FieldEquals("/n", "1.0"), "{\"n\":\"1\"}")).isFalse();
      assertThat(test(new FieldEquals("/b", "true"), "{\"b\":\"true\"}")).isTrue();
    }

    @Test
    void missingNullAndContainersNeverMatch() {
      assertThat(test(new FieldEquals("/x", "eu"), "{}")).isFalse();
      assertThat(test(new FieldEquals("/x", "null"), "{\"x\":null}")).isFalse();
      assertThat(test(new FieldEquals("/x", "{}"), "{\"x\":{}}")).isFalse();
      assertThat(test(new FieldEquals("/x", "[]"), "{\"x\":[]}")).isFalse();
      assertThat(test(new FieldEquals("/x/y", "eu"), "{\"x\":\"eu\"}")).isFalse();
    }
  }

  @Nested
  class In {

    @Test
    void matchesAnyValueWithEqualsSemantics() {
      FieldIn in = new FieldIn("/region", List.of("us", "ca", "1"));
      assertThat(test(in, "{\"region\":\"ca\"}")).isTrue();
      assertThat(test(in, "{\"region\":1.0}")).isTrue();
      assertThat(test(in, "{\"region\":\"eu\"}")).isFalse();
      assertThat(test(in, "{}")).isFalse();
      assertThat(test(in, "{\"region\":[\"us\"]}")).isFalse();
    }

    @Test
    void rejectsEmptyList() {
      assertThatThrownBy(() -> new FieldIn("/r", List.of()))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class Exists {

    @Test
    void presentMeansNotMissingAndNotNull() {
      FieldExists present = new FieldExists("/tier", true);
      assertThat(test(present, "{\"tier\":\"gold\"}")).isTrue();
      assertThat(test(present, "{\"tier\":\"\"}")).isTrue();
      assertThat(test(present, "{\"tier\":0}")).isTrue();
      assertThat(test(present, "{\"tier\":{}}")).isTrue();
      assertThat(test(present, "{\"tier\":null}")).isFalse();
      assertThat(test(present, "{}")).isFalse();
    }

    @Test
    void absentIsTheNegation() {
      FieldExists absent = new FieldExists("/tier", false);
      assertThat(test(absent, "{}")).isTrue();
      assertThat(test(absent, "{\"tier\":null}")).isTrue();
      assertThat(test(absent, "{\"tier\":false}")).isFalse();
    }
  }

  @Nested
  class Prefix {

    @Test
    void stringFieldOnly() {
      FieldPrefix prefix = new FieldPrefix("/svc", "billing-");
      assertThat(test(prefix, "{\"svc\":\"billing-api\"}")).isTrue();
      assertThat(test(prefix, "{\"svc\":\"billing-\"}")).isTrue();
      assertThat(test(prefix, "{\"svc\":\"Billing-api\"}")).isFalse();
      assertThat(test(prefix, "{}")).isFalse();
      assertThat(test(new FieldPrefix("/n", "12"), "{\"n\":123}")).isFalse();
    }
  }

  @Nested
  class Matches {

    @Test
    void stackOverflowOnLongValueIsNoMatch() throws InterruptedException {
      Condition condition = Condition.compile(new FieldMatches("/s", "(a|b)*c"));
      String value = "ab".repeat(200_000) + "c";
      JsonNode longValue = JsonNodeFactory.instance.objectNode().put("s", value);
      // A small, fixed stack makes the overflow deterministic whatever -Xss the JVM runs with.
      boolean[] overflowed = new boolean[1];
      boolean[] matched = new boolean[1];
      Thread small =
          new Thread(
              null,
              () -> {
                try {
                  Pattern.compile("(a|b)*c").matcher(value).matches();
                } catch (StackOverflowError e) {
                  overflowed[0] = true;
                }
                matched[0] = condition.test(longValue, "t", HeaderLookup.NONE);
              },
              "small-stack",
              256 * 1024);
      small.start();
      small.join();
      assertThat(overflowed[0]).as("the raw matcher overflows").isTrue();
      assertThat(matched[0]).isFalse();
      assertThat(condition.test(doc("{\"s\":\"ababc\"}"), "t", HeaderLookup.NONE)).isTrue();
    }

    @Test
    void anchoredOnBothEnds() {
      FieldMatches matches = new FieldMatches("/host", "web-[0-9]+");
      assertThat(test(matches, "{\"host\":\"web-12\"}")).isTrue();
      assertThat(test(matches, "{\"host\":\"xweb-12\"}")).isFalse();
      assertThat(test(matches, "{\"host\":\"web-12x\"}")).isFalse();
      assertThat(test(matches, "{}")).isFalse();
      assertThat(test(new FieldMatches("/n", "[0-9]+"), "{\"n\":123}")).isFalse();
    }

    @Test
    void invalidRegexFailsAtCompile() {
      assertThatThrownBy(() -> Condition.compile(new FieldMatches("/h", "web-(")))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class Header {

    private final Condition condition = Condition.compile(new HeaderEquals("source", "zürich"));

    private boolean test(Map<String, byte[]> headers) {
      return condition.test(doc("{}"), "t", HeaderLookup.of(headers));
    }

    @Test
    void comparesUtf8Bytes() {
      assertThat(test(Map.of("source", "zürich".getBytes(StandardCharsets.UTF_8)))).isTrue();
      assertThat(test(Map.of("source", "zürich".getBytes(StandardCharsets.ISO_8859_1)))).isFalse();
      assertThat(test(Map.of("source", "zurich".getBytes(StandardCharsets.UTF_8)))).isFalse();
    }

    @Test
    void missingOrNullHeaderNeverMatches() {
      assertThat(test(Map.of())).isFalse();
      assertThat(test(Map.of("Source", "zürich".getBytes(StandardCharsets.UTF_8)))).isFalse();
      assertThat(condition.test(doc("{}"), "t", name -> null)).isFalse();
      assertThat(
              Condition.compile(new HeaderEquals("h", ""))
                  .test(doc("{}"), "t", name -> new byte[0]))
          .isTrue();
    }
  }

  @Nested
  class Topic {

    @Test
    void exactTopicEquality() {
      Condition topic = Condition.compile(new TopicEquals("orders.v2"));
      assertThat(topic.test(doc("{}"), "orders.v2", HeaderLookup.NONE)).isTrue();
      assertThat(topic.test(doc("{}"), "orders.backfill", HeaderLookup.NONE)).isFalse();
      assertThat(topic.test(doc("{}"), null, HeaderLookup.NONE)).isFalse();
    }
  }

  @Nested
  class Garbage {

    @Test
    void invalidPointersFailAtCompile() {
      for (String pointer : List.of("region", "", "/a~2", "/a~")) {
        assertThatThrownBy(() -> Condition.compile(new FieldEquals(pointer, "x")))
            .as(pointer)
            .isInstanceOf(IllegalArgumentException.class);
      }
    }

    @Test
    void missingComponentsFailAtConstruction() {
      assertThatThrownBy(() -> new FieldEquals(null, "x"))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> new FieldEquals("/a", null))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> new HeaderEquals(" ", "x"))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> new TopicEquals(null)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> Condition.compile(null))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
