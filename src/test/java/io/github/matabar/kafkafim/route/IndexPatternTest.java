package io.github.matabar.kafkafim.route;

import static io.github.matabar.kafkafim.route.ConditionTest.doc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.matabar.kafkafim.route.IndexResolution.InvalidName;
import io.github.matabar.kafkafim.route.IndexResolution.Resolved;
import io.github.matabar.kafkafim.route.IndexResolution.Unresolved;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.node.JsonNodeFactory;

/** Index pattern compilation and resolution. */
class IndexPatternTest {

  private static final Instant T = Instant.parse("2026-11-07T13:45:30.123Z");

  private static IndexResolution resolve(String pattern, String json, Instant time) {
    return IndexPattern.compile(pattern).resolve(doc(json), time);
  }

  private static String resolved(String pattern, String json) {
    return resolved(pattern, json, T);
  }

  private static String resolved(String pattern, String json, Instant time) {
    IndexResolution result = resolve(pattern, json, time);
    assertThat(result).isInstanceOf(Resolved.class);
    return ((Resolved) result).index();
  }

  @Nested
  class Literals {

    @Test
    void constantPatternIsResolvedOnceAndLowerCased() {
      IndexPattern pattern = IndexPattern.compile("Orders-Unrouted");
      IndexResolution first = pattern.resolve(doc("{}"), null);
      assertThat(first).isEqualTo(new Resolved("orders-unrouted"));
      assertThat(pattern.resolve(doc("{\"x\":1}"), null)).isSameAs(first);
      assertThat(pattern.usesEventTime()).isFalse();
    }

    @Test
    void escapeFreeLiteralText() {
      assertThat(resolved("a%b}c{d", "{}")).isEqualTo("a%b}c{d");
      assertThat(resolved("100%", "{}")).isEqualTo("100%");
      assertThat(resolved("a%{x}%", "{\"x\":\"y\"}")).isEqualTo("ay%");
      assertThat(resolved("%%{x}", "{\"x\":\"y\"}")).isEqualTo("%y");
    }
  }

  @Nested
  class Fields {

    @Test
    void topLevelNameAndPointer() {
      String json = "{\"region\":\"eu\",\"a\":{\"b\":\"deep\"},\"a.b\":\"dotted\"}";
      assertThat(resolved("orders-%{region}", json)).isEqualTo("orders-eu");
      assertThat(resolved("x-%{/a/b}", json)).isEqualTo("x-deep");
      assertThat(resolved("x-%{a.b}", json)).isEqualTo("x-dotted");
      assertThat(resolved("x-%{/arr/1}", "{\"arr\":[\"p\",\"q\"]}")).isEqualTo("x-q");
    }

    @Test
    void adjacentTokens() {
      assertThat(resolved("%{a}%{b}%{/c}", "{\"a\":\"x\",\"b\":\"y\",\"c\":\"z\"}"))
          .isEqualTo("xyz");
      assertThat(resolved("%{a}%{+yyyy}%{a}", "{\"a\":\"x\"}")).isEqualTo("x2026x");
    }

    @Test
    void scalarsAreStringified() {
      assertThat(resolved("t-%{n}", "{\"n\":42}")).isEqualTo("t-42");
      assertThat(resolved("t-%{n}", "{\"n\":-1.5}")).isEqualTo("t--1.5");
      assertThat(resolved("t-%{n}", "{\"n\":12345678901234567890}"))
          .isEqualTo("t-12345678901234567890");
      assertThat(resolved("t-%{b}", "{\"b\":true}")).isEqualTo("t-true");
    }

    @Test
    void valuesAreLowerCasedWithLocaleRoot() {
      assertThat(resolved("Logs-%{svc}", "{\"svc\":\"BILLING-Api\"}"))
          .isEqualTo("logs-billing-api");
      // Locale.ROOT: no Turkish dotless i, whatever the JVM default locale.
      Locale saved = Locale.getDefault();
      try {
        Locale.setDefault(Locale.forLanguageTag("tr-TR"));
        assertThat(resolved("%{svc}-I", "{\"svc\":\"TITLE\"}")).isEqualTo("title-i");
      } finally {
        Locale.setDefault(saved);
      }
    }

    @Test
    void missingNullAndContainersAreUnresolved() {
      assertThat(resolve("o-%{region}", "{}", T)).isEqualTo(new Unresolved("region", "missing"));
      assertThat(resolve("o-%{/a/b}", "{\"a\":{}}", T))
          .isEqualTo(new Unresolved("/a/b", "missing"));
      assertThat(resolve("o-%{region}", "{\"region\":null}", T))
          .isEqualTo(new Unresolved("region", "null"));
      assertThat(resolve("o-%{r}", "{\"r\":{\"x\":1}}", T))
          .isEqualTo(new Unresolved("r", "an object"));
      assertThat(resolve("o-%{r}", "{\"r\":[\"x\"]}", T))
          .isEqualTo(new Unresolved("r", "an array"));
    }

    @Test
    void unresolvedNamesTheFieldAndTheDlqErrorType() {
      IndexResolution result = resolve("o-%{tenant}-%{+yyyy}", "{}", T);
      assertThat(result).isInstanceOf(IndexResolution.Failure.class);
      IndexResolution.Failure failure = (IndexResolution.Failure) result;
      assertThat(failure.errorType()).isEqualTo("unresolved_index");
      assertThat(failure.reason()).isEqualTo("index pattern field '%{tenant}' is missing");
    }
  }

  @Nested
  class Dates {

    @Test
    void formattedFromTheEventTimeInUtc() {
      assertThat(resolved("logs-%{+yyyy.MM.dd}", "{}")).isEqualTo("logs-2026.11.07");
      assertThat(IndexPattern.compile("logs-%{+yyyy.MM.dd}").usesEventTime()).isTrue();
    }

    @Test
    void multipleDateTokens() {
      assertThat(resolved("logs-%{+yyyy}-%{+MM}-w%{+ww}", "{}")).isEqualTo("logs-2026-11-w45");
      assertThat(resolved("logs-%{+yyyy.MM.dd'h'HH}", "{}")).isEqualTo("logs-2026.11.07h13");
    }

    @Test
    void utcDayBoundary() {
      assertThat(resolved("d-%{+yyyy.MM.dd}", "{}", Instant.parse("2026-12-31T23:59:59.999Z")))
          .isEqualTo("d-2026.12.31");
      assertThat(resolved("d-%{+yyyy.MM.dd}", "{}", Instant.parse("2027-01-01T00:00:00Z")))
          .isEqualTo("d-2027.01.01");
    }

    @Test
    void dstTransitionsDoNotApply() {
      // Europe switches to summer time at 2026-03-29T01:00Z, the US at 2026-03-08T07:00Z.
      assertThat(resolved("h-%{+yyyy.MM.dd.HH}", "{}", Instant.parse("2026-03-29T00:30:00Z")))
          .isEqualTo("h-2026.03.29.00");
      assertThat(resolved("h-%{+yyyy.MM.dd.HH}", "{}", Instant.parse("2026-03-29T01:30:00Z")))
          .isEqualTo("h-2026.03.29.01");
      assertThat(resolved("h-%{+yyyy.MM.dd.HH}", "{}", Instant.parse("2026-11-01T06:30:00Z")))
          .isEqualTo("h-2026.11.01.06");
    }

    @Test
    void textualFieldsUseLocaleRootAndAreLowerCased() {
      assertThat(resolved("m-%{+MMM}", "{}")).isEqualTo("m-nov");
    }

    @Test
    void eventTimeIsRequiredOnlyForDatePatterns() {
      assertThat(resolved("x-%{a}", "{\"a\":\"b\"}", null)).isEqualTo("x-b");
      assertThatThrownBy(() -> resolve("x-%{+yyyy}", "{}", null))
          .isInstanceOf(NullPointerException.class);
    }
  }

  @Nested
  class InvalidNames {

    @ParameterizedTest
    @ValueSource(strings = {"a/b", "a b", "a,b", "a#b", "a:b", "a*b", "a?b", "a\"b", "a|b", "a<b"})
    void forbiddenCharactersFromFieldValues(String value) {
      IndexResolution result =
          IndexPattern.compile("x-%{v}")
              .resolve(JsonNodeFactory.instance.objectNode().put("v", value), T);
      assertThat(result).isInstanceOf(InvalidName.class);
      assertThat(((InvalidName) result).problem()).startsWith("must not contain");
    }

    @Test
    void leadingCharactersDotsAndEmpty() {
      assertThat(resolve("%{v}-x", "{\"v\":\"_a\"}", T)).isInstanceOf(InvalidName.class);
      assertThat(resolve("%{v}-x", "{\"v\":\"-a\"}", T)).isInstanceOf(InvalidName.class);
      assertThat(resolve("%{v}x", "{\"v\":\"+\"}", T)).isInstanceOf(InvalidName.class);
      assertThat(resolve("%{v}", "{\"v\":\".\"}", T)).isInstanceOf(InvalidName.class);
      assertThat(resolve("%{v}.", "{\"v\":\".\"}", T)).isInstanceOf(InvalidName.class);
      assertThat(resolve("%{v}", "{\"v\":\"\"}", T))
          .isEqualTo(new InvalidName("", "must not be empty"));
      assertThat(resolve("%{v}", "{\"v\":\"...\"}", T)).isInstanceOf(Resolved.class);
      assertThat(resolve("%{n}", "{\"n\":-1}", T)).isInstanceOf(InvalidName.class);
    }

    @Test
    void longerThan255Utf8Bytes() {
      assertThat(resolve("%{v}", "{\"v\":\"" + "a".repeat(255) + "\"}", T))
          .isInstanceOf(Resolved.class);
      assertThat(resolve("%{v}", "{\"v\":\"" + "a".repeat(256) + "\"}", T))
          .isInstanceOf(InvalidName.class);
      // 'é' is two bytes: 128 of them are 256 bytes in 128 chars.
      IndexResolution twoByte = resolve("%{v}", "{\"v\":\"" + "é".repeat(128) + "\"}", T);
      assertThat(twoByte).isInstanceOf(InvalidName.class);
      assertThat(((InvalidName) twoByte).reason()).contains("256 UTF-8 bytes");
      // '😀' is a surrogate pair, four bytes: 63 of them are 252 bytes.
      assertThat(resolve("%{v}", "{\"v\":\"" + "😀".repeat(63) + "\"}", T))
          .isInstanceOf(Resolved.class);
      assertThat(resolve("%{v}", "{\"v\":\"" + "😀".repeat(64) + "\"}", T))
          .isInstanceOf(InvalidName.class);
    }

    @Test
    void invalidNamesAreDlqUnresolvedIndex() {
      IndexResolution result = resolve("%{v}", "{\"v\":\"A B\"}", T);
      assertThat(((IndexResolution.Failure) result).errorType()).isEqualTo("unresolved_index");
      assertThat(((IndexResolution.Failure) result).reason())
          .isEqualTo("resolved index name 'a b' is invalid: must not contain ' '");
    }

    @Test
    void constantInvalidPatternIsResolvedOnceAsInvalid() {
      assertThat(IndexPattern.compile("_bad").resolve(doc("{}"), null))
          .isInstanceOf(InvalidName.class);
    }
  }

  @Nested
  class Garbage {

    @Test
    void syntaxErrorsFailAtCompile() {
      for (String pattern :
          List.of(
              "",
              " ",
              "bad%{",
              "a-%{x",
              "a-%{}",
              "a-%{+}",
              "a-%{+ }",
              "a-%{+yyyy-ppp}",
              "a-%{/a~2}",
              "a-%{a b}")) {
        assertThatThrownBy(() -> IndexPattern.compile(pattern))
            .as(pattern)
            .isInstanceOf(IllegalArgumentException.class);
      }
      assertThatThrownBy(() -> IndexPattern.compile(null))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
