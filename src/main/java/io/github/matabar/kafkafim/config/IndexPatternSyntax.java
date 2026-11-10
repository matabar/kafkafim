package io.github.matabar.kafkafim.config;

import java.time.DateTimeException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Parse-check of Logstash-style index patterns (DESIGN §5.3): literal text, {@code %{field}} (a
 * JSON pointer or a top-level name) and {@code %{+pattern}} ({@link DateTimeFormatter}). Resolution
 * is the {@code route} package's job; this only decides whether a pattern is well-formed.
 */
final class IndexPatternSyntax {

  /** Characters Elasticsearch rejects in index names; lower-casing happens at resolution. */
  private static final String FORBIDDEN_LITERAL_CHARS = "\\/*?\"<>|,#: ";

  /**
   * A fixed instant to format date references with, to check what they produce. Formatting uses
   * {@link Locale#ROOT}, which index resolution must use too so check and runtime agree.
   */
  private static final ZonedDateTime SAMPLE =
      ZonedDateTime.of(2026, 12, 31, 23, 59, 58, 0, ZoneOffset.UTC);

  private IndexPatternSyntax() {}

  /** Problems with the pattern, empty when it is well-formed. */
  static List<String> errors(String pattern) {
    List<String> errors = new ArrayList<>();
    if (pattern == null || pattern.isBlank()) {
      errors.add("index pattern is empty");
      return errors;
    }
    if (pattern.equals(".") || pattern.equals("..")) {
      errors.add("index names must not be '.' or '..'");
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
    String leading = leadingOutput(pattern);
    if (leading != null
        && leading.startsWith(".")
        && !pattern.equals(".")
        && !pattern.equals("..")) {
      warnings.add(
          "index names starting with '.' are reserved for hidden and system indices;"
              + " Elasticsearch deprecates creating them (DESIGN §5.7)");
    }
    int open = pattern.indexOf("%{+");
    while (open >= 0) {
      int close = pattern.indexOf('}', open);
      if (close < 0) {
        break;
      }
      String format = pattern.substring(open + 3, close);
      if (format.replaceAll("'[^']*'", "").indexOf('Y') >= 0) {
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

  /**
   * What the pattern's first part produces: the literal prefix, or a leading date reference
   * formatted with {@link Locale#ROOT}. Returns {@code null} when it cannot be known at bind time
   * (a leading {@code %{field}}) or the date pattern is invalid, which {@link #errors} reports.
   */
  private static String leadingOutput(String pattern) {
    if (!pattern.startsWith("%{")) {
      return pattern;
    }
    int close = pattern.indexOf('}');
    if (!pattern.startsWith("%{+") || close < 0) {
      return null;
    }
    try {
      return DateTimeFormatter.ofPattern(pattern.substring(3, close), Locale.ROOT).format(SAMPLE);
    } catch (IllegalArgumentException | DateTimeException e) {
      return null;
    }
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
      String sample;
      try {
        sample = DateTimeFormatter.ofPattern(format, Locale.ROOT).format(SAMPLE);
      } catch (IllegalArgumentException | DateTimeException e) {
        errors.add("invalid DateTimeFormatter pattern '" + format + "': " + e.getMessage());
        return;
      }
      // What the date produces is part of the index name too, e.g. 'yyyy/MM' or 'HH:mm'.
      List<String> produced = new ArrayList<>();
      checkLiteral(sample, produced);
      for (String problem : produced) {
        errors.add("'%{+" + format + "}' produces '" + sample + "': " + problem);
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
