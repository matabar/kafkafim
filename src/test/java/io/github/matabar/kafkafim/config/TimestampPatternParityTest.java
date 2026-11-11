package io.github.matabar.kafkafim.config;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.github.matabar.kafkafim.decode.DecodeResult;
import io.github.matabar.kafkafim.decode.DecodeSpec;
import io.github.matabar.kafkafim.decode.DecodeSpec.IdMissing;
import io.github.matabar.kafkafim.decode.DecodeSpec.OnBadTimestamp;
import io.github.matabar.kafkafim.decode.Decoder;
import io.github.matabar.kafkafim.decode.DocumentOutcome;
import io.github.matabar.kafkafim.decode.DocumentOutcome.Decoded;
import io.github.matabar.kafkafim.decode.DocumentOutcome.Unindexable;
import io.github.matabar.kafkafim.decode.ErrorType;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;

/**
 * {@link TimestampPatternSyntax} and {@code decode.TimestampFormat} build their formatters
 * separately, because ADR-0003 keeps {@code config} from depending on {@code decode}. This test
 * (test scope, which the ArchUnit rules do not import) checks that they agree, through {@link
 * Decoder}'s public API: a pattern the validator rejects fails in decode, and a pattern it accepts
 * decodes a value written by the validator's formatter back to that same value.
 */
class TimestampPatternParityTest {

  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  private static final Instant INGEST = Instant.parse("2000-01-01T00:00:00Z");

  /**
   * Instants every pattern is tried with: the validator's own sample, an AM hour on a leap day (and
   * {@code kk}'s {@code 24}), the end of a year, a Sunday at the start of a week-based year, and a
   * date whose week-based year is the previous one.
   */
  private static final List<Instant> INSTANTS =
      List.of(
          Instant.parse("2026-11-07T13:15:30.123456789Z"),
          Instant.parse("2024-02-29T00:30:00Z"),
          Instant.parse("1999-12-31T23:59:59.999Z"),
          Instant.parse("2026-01-04T09:05:07Z"),
          Instant.parse("2027-01-01T11:00:00Z"));

  @ParameterizedTest
  @ValueSource(
      strings = {
        "yyyy-MM-dd",
        "yyyyMMdd",
        "yyyy-MM-dd HH:mm:ss",
        "yyyyMMddHHmmss",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
        "uuuu-MM-dd HH:mm:ss.nnnnnnnnn",
        "dd/MMM/yyyy:HH:mm:ss Z",
        "EEE, dd MMM yyyy HH:mm:ss Z",
        "yyyy-MM-dd hh:mm:ss a",
        "yyyy-MM-dd h B",
        "yyyy-MM-dd A B",
        "yyyy-MM-dd'B'",
        "yyyy-MM-dd HH:mm VV",
        "yyyy-MM-dd HH:mm O",
        "yyyy-DDD HH:mm",
        "YYYY-'W'ww-e HH:mm",
        "G yyyy-MM-dd",
        "yyyy-MM-dd kk:mm"
      })
  void acceptedPatternDecodesWhatItFormats(String pattern) {
    assertThat(TimestampPatternSyntax.errors(pattern)).isEmpty();
    DateTimeFormatter formatter = TimestampPatternSyntax.formatter(pattern);
    Decoder decoder = decoder(pattern);
    for (Instant instant : INSTANTS) {
      String text = formatter.format(instant.atZone(ZoneOffset.UTC));
      DocumentOutcome outcome = decode(decoder, text);
      assertThat(outcome).as("'%s' decoding '%s'", pattern, text).isInstanceOf(Decoded.class);
      Instant decoded = ((Decoded) outcome).timestamp();
      assertThat(formatter.format(decoded.atZone(ZoneOffset.UTC)))
          .as("'%s' decoding '%s' as %s", pattern, text, decoded)
          .isEqualTo(text);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "yyyy-MM",
        "HH:mm",
        "YYYY-MM-dd",
        "yyyy-MM-dd hh:mm",
        "yyyy-MM-dd mm",
        "yyyy-MM-dd a",
        "yyyy-MM-dd B",
        "yyyy-MM-dd[ B]"
      })
  void rejectedPatternFailsEveryTimestamp(String pattern) {
    assertThat(TimestampPatternSyntax.errors(pattern)).isNotEmpty();
    DateTimeFormatter formatter = TimestampPatternSyntax.formatter(pattern);
    Decoder decoder = decoder(pattern);
    for (Instant instant : INSTANTS) {
      String text = formatter.format(instant.atZone(ZoneOffset.UTC));
      DocumentOutcome outcome = decode(decoder, text);
      assertThat(outcome).as("'%s' decoding '%s'", pattern, text).isInstanceOf(Unindexable.class);
      assertThat(((Unindexable) outcome).error().type()).isEqualTo(ErrorType.BAD_TIMESTAMP);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"yyyy-MM-dd {", "yyyy-MM-dd 'T", "yyyy-MM-dd p", "yyyy-MM-dd #"})
  void uncompilablePatternFailsBoth(String pattern) {
    assertThat(TimestampPatternSyntax.errors(pattern))
        .singleElement()
        .asString()
        .startsWith("invalid DateTimeFormatter pattern");
    assertThatIllegalArgumentException().isThrownBy(() -> decoder(pattern));
  }

  private static Decoder decoder(String pattern) {
    return Decoder.compile(
        new DecodeSpec(
            "/id",
            IdMissing.AUTO,
            null,
            "/ts",
            "pattern:" + pattern,
            OnBadTimestamp.DLQ,
            1_048_576));
  }

  private static DocumentOutcome decode(Decoder decoder, String timestamp) {
    String json = MAPPER.createObjectNode().put("ts", timestamp).toString();
    DecodeResult result = decoder.decode(json.getBytes(UTF_8), INGEST);
    assertThat(result).isInstanceOf(DecodeResult.Documents.class);
    return ((DecodeResult.Documents) result).outcomes().getFirst();
  }
}
