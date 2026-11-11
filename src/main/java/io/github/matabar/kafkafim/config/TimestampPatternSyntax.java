package io.github.matabar.kafkafim.config;

import java.time.DateTimeException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Check of {@code document.timestamp-format: pattern:<p>} (DESIGN §5.2). It builds the same
 * formatter the {@code decode} package parses with ({@code Locale.ROOT}, strict resolution, era
 * defaulting to CE) and resolves a parsed sample the same way, so a pattern that passes here
 * behaves the same at runtime. ADR-0003 keeps {@code config} and {@code decode} apart, so the two
 * must be kept in step by hand; {@code TimestampPatternParityTest} checks that they agree.
 */
final class TimestampPatternSyntax {

  /**
   * A fixed instant to format and parse back. 13:15 tells {@code HH} from {@code hh}, day 7 and
   * month 11 are distinct, and UTC is the one zone every zone and offset letter round-trips.
   */
  private static final ZonedDateTime SAMPLE =
      ZonedDateTime.of(2026, 11, 7, 13, 15, 30, 123_456_789, ZoneOffset.UTC);

  private TimestampPatternSyntax() {}

  /** Problems that make every document's timestamp fail; empty when the pattern can work. */
  static List<String> errors(String pattern) {
    List<String> errors = new ArrayList<>();
    if (pattern == null || pattern.isBlank()) {
      errors.add("'pattern:' needs a DateTimeFormatter pattern");
      return errors;
    }
    DateTimeFormatter formatter;
    try {
      formatter = formatter(pattern);
    } catch (IllegalArgumentException e) {
      errors.add("invalid DateTimeFormatter pattern '" + pattern + "': " + e.getMessage());
      return errors;
    }
    TemporalAccessor parsed = roundTrip(formatter);
    if (parsed == null || parsed.isSupported(ChronoField.INSTANT_SECONDS)) {
      return errors;
    }
    if (parsed.query(TemporalQueries.localDate()) == null) {
      errors.add(
          "pattern '"
              + pattern
              + "' never yields a date (it needs a year with month and day, a day of year, or a"
              + " week-based date), so every timestamp would fail");
    } else if (parsed.query(TemporalQueries.localTime()) == null && hasTimeField(parsed)) {
      errors.add(
          "pattern '"
              + pattern
              + "' never yields a complete time (e.g. 'hh' without 'a', or minutes without"
              + " hours), so every timestamp would fail");
    } else if (parsed.query(TemporalQueries.localTime()) == null
        && unquoted(pattern).indexOf('B') >= 0) {
      // 'B' is not a ChronoField, so hasTimeField cannot see it. Decode fails such a timestamp
      // rather than reading it as midnight; it decides from the pattern text ('B' and no hour
      // letter), which gives the same answer as this sample check.
      errors.add(
          "pattern '"
              + pattern
              + "' has a day period 'B' but no time, so every timestamp would fail");
    }
    return errors;
  }

  /** Warnings for a pattern that {@link #errors} accepts. */
  static List<String> warnings(String pattern) {
    List<String> warnings = new ArrayList<>();
    DateTimeFormatter formatter;
    try {
      formatter = formatter(pattern);
    } catch (IllegalArgumentException e) {
      return warnings;
    }
    TemporalAccessor parsed = roundTrip(formatter);
    if (parsed == null) {
      warnings.add(
          "pattern '"
              + pattern
              + "' cannot format and parse back "
              + SAMPLE.toInstant()
              + "; check it against real values");
    } else if (parsed.query(TemporalQueries.zone()) == null && !pattern.contains("'Z'")) {
      // A quoted 'Z' literal says the producer writes UTC, which is how the value is read.
      warnings.add(
          "pattern '"
              + pattern
              + "' has no offset or zone, so timestamps are read as UTC; that is right if"
              + " producers write UTC, otherwise add one (e.g. 'XXX')");
    }
    if (unquoted(pattern).indexOf('k') >= 0) {
      warnings.add(
          "pattern '"
              + pattern
              + "' uses 'k' (clock hour 1-24): '24' is the first hour of the same day (24:30 is"
              + " 00:30), not ISO-8601 end of day; use 'HH' unless producers write a 24-based"
              + " clock");
    }
    return warnings;
  }

  /** The formatter {@code decode.TimestampFormat} uses; keep the two identical. */
  static DateTimeFormatter formatter(String pattern) {
    return new DateTimeFormatterBuilder()
        .appendPattern(pattern)
        .parseDefaulting(ChronoField.ERA, 1)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);
  }

  /** The sample formatted and parsed back, or {@code null} when either step fails. */
  private static TemporalAccessor roundTrip(DateTimeFormatter formatter) {
    try {
      return formatter.parse(formatter.format(SAMPLE));
    } catch (DateTimeException e) {
      return null;
    }
  }

  private static boolean hasTimeField(TemporalAccessor parsed) {
    for (ChronoField field : ChronoField.values()) {
      if (field.isTimeBased() && parsed.isSupported(field)) {
        return true;
      }
    }
    return false;
  }

  /** The pattern without quoted literals ({@code 'T'}, and {@code ''} for a quote). */
  private static String unquoted(String pattern) {
    return pattern.replaceAll("'[^']*'", "");
  }
}
