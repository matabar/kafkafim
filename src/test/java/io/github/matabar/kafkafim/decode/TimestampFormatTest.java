package io.github.matabar.kafkafim.decode;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class TimestampFormatTest {

  static final JsonMapper MAPPER = JsonMapper.builder().build();

  static TimestampFormat.Parse parse(String format, String json) {
    JsonNode node = MAPPER.readTree(json);
    return TimestampFormat.compile(format).parse(node);
  }

  static void assertParses(String format, String json, String expected) {
    TimestampFormat.Parse parse = parse(format, json);
    assertThat(parse.error()).isNull();
    assertThat(parse.instant()).isEqualTo(Instant.parse(expected));
  }

  static void assertRejects(String format, String json) {
    TimestampFormat.Parse parse = parse(format, json);
    assertThat(parse.instant()).isNull();
    assertThat(parse.error()).isNotBlank();
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "\"2026-11-07T10:15:30Z\"|2026-11-07T10:15:30Z",
        "\"2026-11-07T10:15:30.123456789Z\"|2026-11-07T10:15:30.123456789Z",
        "\"2026-11-07T11:15:30+01:00\"|2026-11-07T10:15:30Z",
        "\"2026-11-07T05:15-05:00\"|2026-11-07T10:15:00Z",
        "\"2026-11-07t10:15:30z\"|2026-11-07T10:15:30Z"
      })
  void iso(String json, String expected) {
    assertParses("iso-8601", json, expected);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "\"2026-11-07T10:15:30\"",
        "\"2026-11-07\"",
        "\"2026-11-07 10:15:30Z\"",
        "\"2026-11-07T10:15:30+0100\"",
        "\"2026-11-07T10:15:30Z[UTC]\"",
        "\"2026-02-30T10:15:30Z\"",
        "\"2026-11-07T24:00:00Z\"",
        "\"\"",
        "1700000000000",
        "true",
        "{}",
        "\"+10000-01-01T00:00:00Z\""
      })
  void isoRejects(String json) {
    assertRejects("iso-8601", json);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "0|1970-01-01T00:00:00Z",
        "1700000000123|2023-11-14T22:13:20.123Z",
        "\"1700000000123\"|2023-11-14T22:13:20.123Z",
        "-1000|1969-12-31T23:59:59Z",
        "\"-1000\"|1969-12-31T23:59:59Z"
      })
  void epochMillis(String json, String expected) {
    assertParses("epoch-millis", json, expected);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "1.5",
        "1e3",
        "\"1.5\"",
        "\"\"",
        "\" 1\"",
        "\"+1\"",
        "\"0x10\"",
        "\"9223372036854775808\"",
        "9223372036854775808",
        "253402300800000",
        "true"
      })
  void epochMillisRejects(String json) {
    assertRejects("epoch-millis", json);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "1700000000|2023-11-14T22:13:20Z",
        "\"1700000000\"|2023-11-14T22:13:20Z",
        "1700000000.123|2023-11-14T22:13:20.123Z",
        "\"1700000000.123456789\"|2023-11-14T22:13:20.123456789Z",
        "1.7e9|2023-11-14T22:13:20Z",
        "-1.5|1969-12-31T23:59:58.5Z",
        "\"-1.5\"|1969-12-31T23:59:58.5Z"
      })
  void epochSeconds(String json, String expected) {
    assertParses("epoch-seconds", json, expected);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "\"1.1234567891\"",
        "\"1.\"",
        "\".5\"",
        "\"1e9\"",
        "1e300",
        "9223372036854775807",
        "\"99999999999999999999\"",
        "253402300800",
        "-62135596801",
        "null",
        "[]"
      })
  void epochSecondsRejects(String json) {
    assertRejects("epoch-seconds", json);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "pattern:yyyy-MM-dd HH:mm:ss|\"2026-11-07 10:15:30\"|2026-11-07T10:15:30Z",
        "pattern:yyyy-MM-dd HH:mm:ssXXX|\"2026-11-07 11:15:30+01:00\"|2026-11-07T10:15:30Z",
        "pattern:yyyy-MM-dd HH:mm VV|\"2026-07-01 12:00 Europe/Paris\"|2026-07-01T10:00:00Z",
        "pattern:yyyy-MM-dd|\"2026-11-07\"|2026-11-07T00:00:00Z",
        "pattern:yyyyMMdd|20261107|2026-11-07T00:00:00Z",
        "pattern:uuuu-MM-dd|\"2026-11-07\"|2026-11-07T00:00:00Z",
        "pattern:dd/MMM/yyyy:HH:mm:ss Z|\"07/Nov/2026:10:15:30 +0000\"|2026-11-07T10:15:30Z",
        "pattern:yyyy-MM-dd hh:mm a|\"2026-11-07 03:04 PM\"|2026-11-07T15:04:00Z"
      })
  void pattern(String format, String json, String expected) {
    assertParses(format, json, expected);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "pattern:yyyy-MM-dd|\"2026-02-30\"",
        "pattern:yyyy-MM-dd|\"2026-11-07 10:00\"",
        "pattern:yyyy-MM-dd|\"07.11.2026\"",
        "pattern:yyyy-MM|\"2026-11\"",
        "pattern:HH:mm|\"10:15\"",
        "pattern:yyyy-MM-dd|1.5",
        "pattern:yyyy-MM-dd|true"
      })
  void patternRejects(String format, String json) {
    assertRejects(format, json);
  }

  @Test
  void errorExcerptIsBounded() {
    String longValue = "\"" + "9".repeat(500) + "\"";
    assertThat(parse("iso-8601", longValue).error().length()).isLessThan(200);
  }
}
