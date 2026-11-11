package io.github.matabar.kafkafim.decode;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Locale;
import java.util.regex.Pattern;
import tools.jackson.databind.JsonNode;

/**
 * {@code document.timestamp-format}: turns the value at {@code timestamp-field} into an {@link
 * Instant}. Parsing never throws; a failure is a reason string.
 *
 * <ul>
 *   <li>{@code iso-8601}: a string such as {@code 2026-11-07T10:15:30.123Z} or {@code
 *       2026-11-07T11:15:30+01:00} ({@link DateTimeFormatter#ISO_OFFSET_DATE_TIME}). An offset or
 *       {@code Z} is required: a local date-time is rejected, never guessed.
 *   <li>{@code epoch-millis}: an integer, as a JSON number or a string of digits with an optional
 *       {@code -}. A fraction is rejected.
 *   <li>{@code epoch-seconds}: as {@code epoch-millis}, plus up to nine fraction digits (truncated
 *       to nanoseconds). A JSON number with a fraction passes through a {@code double}, which is
 *       exact to the millisecond for present-day dates; send a string for more precision.
 *   <li>{@code pattern:<p>}: a string (or an integer, read as its digits) parsed with {@code
 *       DateTimeFormatter} pattern {@code p}, {@code Locale.ROOT} and strict resolution ({@code
 *       yyyy} works because the era defaults to CE). Without an offset or zone in the pattern the
 *       time is UTC; without any time field it is midnight; a time that cannot resolve (e.g. {@code
 *       hh} without {@code a}, or a day period {@code B} in a pattern with no hour) or a pattern
 *       that yields no date fails. Era text under {@code Locale.ROOT} is {@code CE}/{@code BCE},
 *       not {@code AD}/{@code BC}. {@code k} is the clock hour 1 to 24, whose {@code 24} is the
 *       first hour of the day ({@code 24:30} is {@code 00:30} of the same date); {@code HH} rejects
 *       ISO-8601's {@code 24:00}.
 * </ul>
 *
 * <p>Every format rejects instants outside years 1 to 9999, which no {@code %{+yyyy...}} index name
 * should carry.
 */
abstract sealed class TimestampFormat {

  static final String ISO_8601 = "iso-8601";
  static final String EPOCH_MILLIS = "epoch-millis";
  static final String EPOCH_SECONDS = "epoch-seconds";
  static final String PATTERN_PREFIX = "pattern:";

  private static final Instant MIN = Instant.parse("0001-01-01T00:00:00Z");
  private static final Instant MAX = Instant.parse("9999-12-31T23:59:59.999999999Z");
  private static final Pattern EPOCH_INTEGER = Pattern.compile("-?[0-9]{1,19}");
  private static final Pattern EPOCH_DECIMAL = Pattern.compile("-?[0-9]{1,19}(\\.[0-9]{1,9})?");
  private static final int EXCERPT = 64;

  /** Either {@code instant} or {@code error} is non-null. */
  record Parse(Instant instant, String error) {
    static Parse ok(Instant instant) {
      if (instant.isBefore(MIN) || instant.isAfter(MAX)) {
        return failed("is outside years 1 to 9999");
      }
      return new Parse(instant, null);
    }

    static Parse failed(String error) {
      return new Parse(null, error);
    }
  }

  /**
   * Compiles {@code format}.
   *
   * @throws IllegalArgumentException if {@code format} is not one of the four forms
   */
  static TimestampFormat compile(String format) {
    return switch (format) {
      case ISO_8601 -> new Iso();
      case EPOCH_MILLIS -> new Epoch(false);
      case EPOCH_SECONDS -> new Epoch(true);
      default -> {
        if (!format.startsWith(PATTERN_PREFIX)) {
          throw new IllegalArgumentException(
              "timestamp format '"
                  + format
                  + "' is not one of iso-8601, epoch-millis, epoch-seconds,"
                  + " pattern:<DateTimeFormatter>");
        }
        String pattern = format.substring(PATTERN_PREFIX.length());
        if (pattern.isBlank()) {
          throw new IllegalArgumentException("'pattern:' needs a DateTimeFormatter pattern");
        }
        yield new Custom(pattern);
      }
    };
  }

  /**
   * Parses {@code value}, which is a present, non-null node; the caller handles missing and {@code
   * null}.
   */
  abstract Parse parse(JsonNode value);

  static String excerpt(String text) {
    return text.length() <= EXCERPT ? text : text.substring(0, EXCERPT) + "...";
  }

  private static String describe(JsonNode value) {
    return value.isString()
        ? "'" + excerpt(value.stringValue()) + "'"
        : value.getNodeType().name().toLowerCase(Locale.ROOT);
  }

  private static final class Iso extends TimestampFormat {
    @Override
    Parse parse(JsonNode value) {
      if (!value.isString()) {
        return Parse.failed(describe(value) + " is not an ISO-8601 string");
      }
      try {
        return Parse.ok(
            DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(value.stringValue(), Instant::from));
      } catch (DateTimeException e) {
        return Parse.failed(describe(value) + " is not an ISO-8601 date-time with an offset or Z");
      }
    }
  }

  private static final class Epoch extends TimestampFormat {
    private final boolean seconds;

    Epoch(boolean seconds) {
      this.seconds = seconds;
    }

    @Override
    Parse parse(JsonNode value) {
      String unit = seconds ? "epoch seconds" : "epoch milliseconds";
      try {
        if (value.isIntegralNumber()) {
          if (!value.canConvertToLong()) {
            return Parse.failed(value.asString() + " is out of range for " + unit);
          }
          return Parse.ok(of(value.longValue(), 0));
        }
        if (value.isFloatingPointNumber()) {
          if (!seconds) {
            return Parse.failed(value.asString() + " is not an integer number of " + unit);
          }
          return Parse.ok(ofSeconds(BigDecimal.valueOf(value.doubleValue())));
        }
        if (value.isString()) {
          String text = value.stringValue();
          if (seconds
              ? EPOCH_DECIMAL.matcher(text).matches()
              : EPOCH_INTEGER.matcher(text).matches()) {
            return Parse.ok(
                seconds ? ofSeconds(new BigDecimal(text)) : of(Long.parseLong(text), 0));
          }
        }
        return Parse.failed(describe(value) + " is not " + unit);
      } catch (ArithmeticException | NumberFormatException | DateTimeException e) {
        return Parse.failed(describe(value) + " is out of range for " + unit);
      }
    }

    private Instant of(long amount, int nanos) {
      return seconds ? Instant.ofEpochSecond(amount, nanos) : Instant.ofEpochMilli(amount);
    }

    private static Instant ofSeconds(BigDecimal value) {
      BigDecimal whole = value.setScale(0, RoundingMode.FLOOR);
      int nanos =
          value.subtract(whole).movePointRight(9).setScale(0, RoundingMode.FLOOR).intValueExact();
      return Instant.ofEpochSecond(whole.longValueExact(), nanos);
    }
  }

  private static final class Custom extends TimestampFormat {
    private final String pattern;
    private final DateTimeFormatter formatter;
    private final boolean dayPeriod;

    Custom(String pattern) {
      // config.TimestampPatternSyntax builds the same formatter to validate the pattern; keep the
      // two identical so a pattern that passes validation behaves the same here
      // (config.TimestampPatternParityTest checks it).
      this.pattern = pattern;
      this.formatter =
          new DateTimeFormatterBuilder()
              .appendPattern(pattern)
              .parseDefaulting(ChronoField.ERA, 1)
              .toFormatter(Locale.ROOT)
              .withResolverStyle(ResolverStyle.STRICT);
      // A day period 'B' is not a ChronoField, so hasTimeField cannot see it once parsed, and the
      // parse result does not say whether one was read. Decided per pattern instead: 'B' with no
      // letter that can carry the hour (h, H, k, K, A, N) can never resolve to a time. A pattern
      // with an hour in an optional section, e.g. "yyyy-MM-dd[ h:mm B]", still reads a date-only
      // value as midnight, as it does with 'a'.
      String unquoted = pattern.replaceAll("'[^']*'", "");
      this.dayPeriod =
          unquoted.indexOf('B') >= 0 && unquoted.chars().noneMatch(c -> "hHkKAN".indexOf(c) >= 0);
    }

    @Override
    Parse parse(JsonNode value) {
      String text;
      if (value.isString()) {
        text = value.stringValue();
      } else if (value.isIntegralNumber()) {
        text = value.asString();
      } else {
        return Parse.failed(describe(value) + " is not a string");
      }
      try {
        TemporalAccessor parsed = formatter.parse(text);
        if (parsed.isSupported(ChronoField.INSTANT_SECONDS)) {
          return Parse.ok(Instant.from(parsed));
        }
        LocalDate date = parsed.query(TemporalQueries.localDate());
        if (date == null) {
          return Parse.failed("pattern '" + pattern + "' yields no date from " + describe(value));
        }
        LocalTime time = parsed.query(TemporalQueries.localTime());
        if (time == null && (hasTimeField(parsed) || dayPeriod)) {
          // e.g. "hh:mm" without "a", or "B" without an hour: the time is parsed but cannot
          // resolve, and defaulting to midnight would misdate the document.
          return Parse.failed(
              "pattern '" + pattern + "' yields an incomplete time from " + describe(value));
        }
        ZoneId zone = parsed.query(TemporalQueries.zone());
        return Parse.ok(
            date.atTime(time == null ? LocalTime.MIDNIGHT : time)
                .atZone(zone == null ? ZoneOffset.UTC : zone)
                .toInstant());
      } catch (DateTimeException e) {
        return Parse.failed(describe(value) + " does not match pattern '" + pattern + "'");
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
  }
}
