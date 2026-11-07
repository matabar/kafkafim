package io.github.matabar.kafkafim.config;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Parse-check of Logstash-style index patterns (DESIGN §5.3): literal text, {@code %{field}} (a
 * JSON pointer or a top-level name) and {@code %{+pattern}} ({@link DateTimeFormatter}). Resolution
 * is the {@code route} package's job; this only decides whether a pattern is well-formed.
 */
final class IndexPatternSyntax {

  /** Characters Elasticsearch rejects in index names; lower-casing happens at resolution. */
  private static final String FORBIDDEN_LITERAL_CHARS = "\\/*?\"<>|,# ";

  private IndexPatternSyntax() {}

  /** Problems with the pattern, empty when it is well-formed. */
  static List<String> errors(String pattern) {
    List<String> errors = new ArrayList<>();
    if (pattern == null || pattern.isBlank()) {
      errors.add("index pattern is empty");
      return errors;
    }
    char first = pattern.charAt(0);
    if (first == '-' || first == '_' || first == '+') {
      errors.add("index names must not start with '" + first + "'");
    }
    int position = 0;
    while (position < pattern.length()) {
      int open = pattern.indexOf("%{", position);
      String literal = open < 0 ? pattern.substring(position) : pattern.substring(position, open);
      checkLiteral(literal, errors);
      if (open < 0) {
        break;
      }
      int close = pattern.indexOf('}', open + 2);
      if (close < 0) {
        errors.add("unterminated '%{' at position " + open);
        break;
      }
      checkReference(pattern.substring(open + 2, close), errors);
      position = close + 1;
    }
    return errors;
  }

  /** Warnings for well-formed but suspicious patterns. */
  static List<String> warnings(String pattern) {
    List<String> warnings = new ArrayList<>();
    if (pattern == null) {
      return warnings;
    }
    int open = pattern.indexOf("%{+");
    while (open >= 0) {
      int close = pattern.indexOf('}', open);
      if (close < 0) {
        break;
      }
      String format = pattern.substring(open + 3, close);
      if (format.indexOf('Y') >= 0) {
        warnings.add(
            "'%{+"
                + format
                + "}' uses 'Y' (week-based year); Logstash's 'YYYY' is 'yyyy' in"
                + " DateTimeFormatter (DESIGN §14)");
      }
      open = pattern.indexOf("%{+", close);
    }
    return warnings;
  }

  private static void checkLiteral(String literal, List<String> errors) {
    for (char c : literal.toCharArray()) {
      if (FORBIDDEN_LITERAL_CHARS.indexOf(c) >= 0) {
        errors.add("index names must not contain '" + c + "'");
        return;
      }
    }
  }

  private static void checkReference(String reference, List<String> errors) {
    if (reference.isEmpty()) {
      errors.add("empty reference '%{}'");
    } else if (reference.startsWith("+")) {
      String format = reference.substring(1);
      if (format.isBlank()) {
        errors.add("empty date pattern '%{+}'");
        return;
      }
      try {
        DateTimeFormatter.ofPattern(format);
      } catch (IllegalArgumentException e) {
        errors.add("invalid DateTimeFormatter pattern '" + format + "': " + e.getMessage());
      }
    } else if (reference.startsWith("/")) {
      String error = JsonPointers.error(reference);
      if (error != null) {
        errors.add("'%{" + reference + "}': " + error);
      }
    } else if (reference.isBlank() || reference.chars().anyMatch(Character::isWhitespace)) {
      errors.add("invalid field name '%{" + reference + "}'");
    }
  }
}
