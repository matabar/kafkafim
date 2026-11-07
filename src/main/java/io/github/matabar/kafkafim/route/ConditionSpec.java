package io.github.matabar.kafkafim.route;

import java.util.List;

/**
 * One {@code when} condition of a route (DESIGN §5.3), in the shape {@code route} compiles. The
 * mapping from the bound configuration happens outside this package; values arrive as the strings
 * the configuration binds. Field subjects are RFC 6901 JSON pointers starting with {@code /}.
 *
 * <p>The records check only that their components are present; {@link Router#compile} checks the
 * rest (pointer syntax, regex) and throws {@link IllegalArgumentException} on anything unusable.
 */
public sealed interface ConditionSpec {

  /**
   * {@code field: /p, equals: v}: the field is a scalar equal to {@code value}.
   *
   * <p>A JSON string equals {@code value} when the text is identical. A JSON number equals {@code
   * value} when {@code value} parses as a decimal number of the same numeric value ({@code 1},
   * {@code 1.0} and {@code 1e0} are all equal; a double is compared by its shortest decimal form,
   * so {@code 0.1} equals {@code "0.1"}); NaN and infinities equal nothing. A JSON boolean equals
   * {@code "true"} or {@code "false"} exactly. A string never equals a number ({@code "1"} in the
   * document does not equal {@code equals: 1.0}); objects, arrays, null and a missing field equal
   * nothing.
   *
   * @param field JSON pointer
   * @param value the configured value
   */
  record FieldEquals(String field, String value) implements ConditionSpec {
    /** Rejects missing components. */
    public FieldEquals {
      Specs.require(field != null, "field is required");
      Specs.require(value != null, "equals value is required");
    }
  }

  /**
   * {@code field: /p, in: [a, b]}: the field {@link FieldEquals equals} one of {@code values}.
   *
   * @param field JSON pointer
   * @param values non-empty
   */
  record FieldIn(String field, List<String> values) implements ConditionSpec {
    /** Rejects missing components and copies the list. */
    public FieldIn {
      Specs.require(field != null, "field is required");
      Specs.require(values != null && !values.isEmpty(), "in requires at least one value");
      Specs.require(
          values.stream().noneMatch(java.util.Objects::isNull), "in values must not be null");
      values = List.copyOf(values);
    }
  }

  /**
   * {@code field: /p, exists: true|false}: the field is present and not JSON {@code null} (or, for
   * {@code false}, missing or {@code null}).
   *
   * @param field JSON pointer
   * @param exists whether the field must be present
   */
  record FieldExists(String field, boolean exists) implements ConditionSpec {
    /** Rejects missing components. */
    public FieldExists {
      Specs.require(field != null, "field is required");
    }
  }

  /**
   * {@code field: /p, prefix: s}: the field is a JSON string starting with {@code prefix}.
   *
   * @param field JSON pointer
   * @param prefix compared exactly (case-sensitive)
   */
  record FieldPrefix(String field, String prefix) implements ConditionSpec {
    /** Rejects missing components. */
    public FieldPrefix {
      Specs.require(field != null, "field is required");
      Specs.require(prefix != null, "prefix is required");
    }
  }

  /**
   * {@code field: /p, matches: regex}: the field is a JSON string the whole of which matches {@code
   * regex} ({@link java.util.regex.Matcher#matches()}, so implicitly anchored). A value long enough
   * to overflow the regex engine's stack (possible with constructs like {@code (a|b)*}) does not
   * match.
   *
   * @param field JSON pointer
   * @param regex {@link java.util.regex.Pattern} syntax, compiled once
   */
  record FieldMatches(String field, String regex) implements ConditionSpec {
    /** Rejects missing components. */
    public FieldMatches {
      Specs.require(field != null, "field is required");
      Specs.require(regex != null, "matches regex is required");
    }
  }

  /**
   * {@code header: name, equals: v}: the record's last header called {@code name} has exactly the
   * UTF-8 bytes of {@code value}. A missing header or a null header value never matches.
   *
   * @param name header name, compared exactly
   * @param value the configured value
   */
  record HeaderEquals(String name, String value) implements ConditionSpec {
    /** Rejects missing components. */
    public HeaderEquals {
      Specs.require(name != null && !name.isBlank(), "header name is required");
      Specs.require(value != null, "header equals value is required");
    }
  }

  /**
   * {@code topic: name}: the record was read from {@code topic}.
   *
   * @param topic source topic, compared exactly
   */
  record TopicEquals(String topic) implements ConditionSpec {
    /** Rejects missing components. */
    public TopicEquals {
      Specs.require(topic != null && !topic.isBlank(), "topic is required");
    }
  }
}
