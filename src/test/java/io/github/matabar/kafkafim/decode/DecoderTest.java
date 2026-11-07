package io.github.matabar.kafkafim.decode;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

import io.github.matabar.kafkafim.decode.DecodeSpec.IdMissing;
import io.github.matabar.kafkafim.decode.DecodeSpec.OnBadTimestamp;
import io.github.matabar.kafkafim.decode.DocumentOutcome.TimestampSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DecoderTest {

  static final Instant INGEST = Instant.parse("2026-11-07T12:00:00Z");

  static DecodeSpec spec(
      IdMissing idMissing,
      String version,
      String timestamp,
      String format,
      OnBadTimestamp onBad,
      long max) {
    return new DecodeSpec("/id", idMissing, version, timestamp, format, onBad, max);
  }

  static final Decoder DEFAULT =
      Decoder.compile(spec(IdMissing.DLQ, null, null, "iso-8601", OnBadTimestamp.DLQ, 1 << 20));

  static DecodeResult decode(Decoder decoder, String json) {
    return decoder.decode(json.getBytes(UTF_8), INGEST);
  }

  static DecodeError recordFailure(Decoder decoder, byte[] value) {
    DecodeResult result = decoder.decode(value, INGEST);
    assertThat(result).isInstanceOf(DecodeResult.RecordFailure.class);
    DecodeError error = ((DecodeResult.RecordFailure) result).error();
    assertThat(error.type()).isEqualTo(ErrorType.DECODE);
    return error;
  }

  static DecodeError recordFailure(String json) {
    return recordFailure(DEFAULT, json.getBytes(UTF_8));
  }

  static DecodeResult.Documents documents(Decoder decoder, String json) {
    DecodeResult result = decode(decoder, json);
    assertThat(result).isInstanceOf(DecodeResult.Documents.class);
    return (DecodeResult.Documents) result;
  }

  static DocumentOutcome.Decoded single(Decoder decoder, String json) {
    DecodeResult.Documents docs = documents(decoder, json);
    assertThat(docs.array()).isFalse();
    assertThat(docs.outcomes()).hasSize(1);
    assertThat(docs.outcomes().getFirst()).isInstanceOf(DocumentOutcome.Decoded.class);
    return (DocumentOutcome.Decoded) docs.outcomes().getFirst();
  }

  static DecodeError singleFailure(Decoder decoder, String json) {
    DecodeResult.Documents docs = documents(decoder, json);
    assertThat(docs.outcomes()).hasSize(1);
    assertThat(docs.outcomes().getFirst()).isInstanceOf(DocumentOutcome.Failed.class);
    return ((DocumentOutcome.Failed) docs.outcomes().getFirst()).error();
  }

  static String source(DocumentOutcome outcome) {
    return new String(((DocumentOutcome.Decoded) outcome).source().toByteArray(), UTF_8);
  }

  @Nested
  class RecordLevel {

    @Test
    void nullValueIsTombstone() {
      assertThat(recordFailure(DEFAULT, null).reason()).contains("tombstone");
    }

    @Test
    void emptyValue() {
      assertThat(recordFailure(DEFAULT, new byte[0]).reason()).contains("empty");
    }

    @ParameterizedTest
    @ValueSource(strings = {" ", " \n\t\r "})
    void whitespaceOnly(String value) {
      assertThat(recordFailure(value).reason()).contains("no JSON value");
    }

    @Test
    void bomOnly() {
      assertThat(
              recordFailure(DEFAULT, new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}).reason())
          .contains("no JSON value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "42", "\"text\"", "true", "1.5"})
    void scalarsAndNull(String value) {
      assertThat(recordFailure(value).reason()).contains("not an object or an array of objects");
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "{",
          "{\"id\":1",
          "[{\"id\":1},",
          "{'id':1}",
          "{\"id\":1,}",
          "[{\"id\":1},]",
          "{\"id\":NaN}",
          "{\"id\":01}",
          "{\"id\":1} // comment",
          "{\"id\":\"a\nb\"}"
        })
    void invalidJson(String value) {
      assertThat(recordFailure(value).reason()).startsWith("invalid JSON");
    }

    @Test
    void invalidJsonLaterInAnArrayFailsTheWholeRecord() {
      assertThat(recordFailure("[{\"id\":1},{\"id\":2},{\"id\":").reason())
          .startsWith("invalid JSON");
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"id\":1}{\"id\":2}", "{\"id\":1} 2", "[] []"})
    void trailingContent(String value) {
      assertThat(recordFailure(value).reason()).contains("trailing content");
    }

    @Test
    void trailingGarbage() {
      assertThat(recordFailure("{\"id\":1} x").reason()).startsWith("invalid JSON");
    }

    @Test
    void invalidUtf8() {
      byte[] value = {'{', '"', 'i', 'd', '"', ':', '"', (byte) 0xC3, (byte) 0x28, '"', '}'};
      assertThat(recordFailure(DEFAULT, value).reason()).startsWith("invalid JSON");
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "{\"id\":1,\"id\":2}",
          "{\"id\":1,\"a\":{\"x\":1,\"x\":2}}",
          "[{\"id\":1},{\"id\":2,\"id\":3}]"
        })
    void duplicateKeysAtAnyDepthFailTheRecord(String value) {
      assertThat(recordFailure(value).reason()).contains("Duplicate");
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTF-16BE", "UTF-16LE", "UTF-16", "UTF-32BE", "UTF-32LE", "UTF-32"})
    void otherUnicodeEncodingsFailTheRecord(String charset) {
      for (String json : List.of("{\"id\":\"a\"}", "[{\"id\":\"a\"},{\"id\":\"b\"}]")) {
        byte[] value = json.getBytes(java.nio.charset.Charset.forName(charset));
        assertThat(recordFailure(DEFAULT, value).reason()).contains("not UTF-8");
      }
    }

    @Test
    void secondBomFailsTheRecord() {
      byte[] json = "{\"id\":1}".getBytes(UTF_8);
      byte[] value = new byte[json.length + 6];
      for (int i = 0; i < 6; i += 3) {
        value[i] = (byte) 0xEF;
        value[i + 1] = (byte) 0xBB;
        value[i + 2] = (byte) 0xBF;
      }
      System.arraycopy(json, 0, value, 6, json.length);
      assertThat(recordFailure(DEFAULT, value).reason()).contains("more than one byte order mark");
    }

    @Test
    void keyLongerThanTheLimitFails() {
      String key = "k".repeat(Decoder.MAX_NAME_LENGTH + 1);
      assertThat(recordFailure("{\"id\":1,\"" + key + "\":1}").reason()).startsWith("invalid JSON");
    }

    @Test
    void sourceIsUsableForEveryRandomInputThatDecodes() {
      Random random = new Random(7);
      byte[] alphabet = "{}[]\":,0123456789 \nidabc\\".getBytes(UTF_8);
      for (int i = 0; i < 20000; i++) {
        byte[] value = new byte[random.nextInt(24)];
        for (int j = 0; j < value.length; j++) {
          value[j] =
              random.nextInt(8) == 0
                  ? (byte) random.nextInt(256)
                  : alphabet[random.nextInt(alphabet.length)];
        }
        if (DEFAULT.decode(value, INGEST) instanceof DecodeResult.Documents docs) {
          for (DocumentOutcome outcome : docs.outcomes()) {
            if (outcome instanceof DocumentOutcome.Decoded doc) {
              assertThat(doc.source().length()).isPositive();
              assertThatNoException().isThrownBy(() -> doc.source().toByteArray());
            }
          }
        }
      }
    }

    @Test
    void oversizeIsRejectedWithoutParsing() {
      Decoder small =
          Decoder.compile(spec(IdMissing.DLQ, null, null, "iso-8601", OnBadTimestamp.DLQ, 10));
      // Not even valid JSON: proves the size check comes first.
      assertThat(recordFailure(small, "{{{{{{{{{{{".getBytes(UTF_8)).reason())
          .contains("11 bytes, larger than max-record-bytes 10");
      assertThat(decode(small, "{\"id\":123}")).isInstanceOf(DecodeResult.Documents.class);
    }

    @Test
    void nestingBeyondTheLimitFails() {
      String deep =
          "{\"id\":1,\"a\":"
              + "[".repeat(Decoder.MAX_NESTING_DEPTH)
              + "]".repeat(Decoder.MAX_NESTING_DEPTH)
              + "}";
      assertThat(recordFailure(deep).reason()).startsWith("invalid JSON");
      String ok = "{\"id\":1,\"a\":" + "[".repeat(100) + "]".repeat(100) + "}";
      assertThat(single(DEFAULT, ok).id()).hasValue("1");
    }

    @Test
    void numberLiteralBeyondTheLimitFails() {
      assertThat(
              recordFailure("{\"id\":" + "9".repeat(Decoder.MAX_NUMBER_LENGTH + 1) + "}").reason())
          .startsWith("invalid JSON");
    }

    @Test
    void neverThrowsOnRandomBytes() {
      Random random = new Random(42);
      for (int i = 0; i < 2000; i++) {
        byte[] value = new byte[random.nextInt(64)];
        random.nextBytes(value);
        assertThatNoException().isThrownBy(() -> DEFAULT.decode(value, INGEST));
      }
    }
  }

  @Nested
  class Documents {

    @Test
    void objectIsOneDocument() {
      DocumentOutcome.Decoded doc = single(DEFAULT, "{\"id\":\"a\",\"v\":1}");
      assertThat(doc.index()).isZero();
      assertThat(doc.id()).hasValue("a");
      assertThat(doc.node().get("v").intValue()).isEqualTo(1);
      assertThat(doc.version()).isEmpty();
      assertThat(doc.timestamp()).isEqualTo(INGEST);
      assertThat(doc.timestampSource()).isEqualTo(TimestampSource.INGEST_TIME);
    }

    @Test
    void arrayIsOneDocumentPerElementInOrder() {
      DecodeResult.Documents docs = documents(DEFAULT, "[{\"id\":1},{\"id\":2},{\"id\":3}]");
      assertThat(docs.array()).isTrue();
      assertThat(docs.outcomes()).extracting(DocumentOutcome::index).containsExactly(0, 1, 2);
      assertThat(docs.outcomes())
          .map(o -> ((DocumentOutcome.Decoded) o).id().orElseThrow())
          .containsExactly("1", "2", "3");
    }

    @Test
    void emptyArrayIsZeroDocuments() {
      DecodeResult.Documents docs = documents(DEFAULT, " [ ] ");
      assertThat(docs.array()).isTrue();
      assertThat(docs.outcomes()).isEmpty();
    }

    @Test
    void badElementFailsOnlyItself() {
      DecodeResult.Documents docs =
          documents(DEFAULT, "[{\"id\":1},42,null,\"s\",[{\"id\":9}],{\"x\":1},{\"id\":2}]");
      List<DocumentOutcome> outcomes = docs.outcomes();
      assertThat(outcomes).hasSize(7);
      assertThat(outcomes.get(0)).isInstanceOf(DocumentOutcome.Decoded.class);
      for (int i = 1; i <= 4; i++) {
        DocumentOutcome.Failed failed = (DocumentOutcome.Failed) outcomes.get(i);
        assertThat(failed.index()).isEqualTo(i);
        assertThat(failed.error().type()).isEqualTo(ErrorType.DECODE);
        assertThat(failed.error().reason())
            .contains("array element " + i)
            .contains("not an object");
      }
      assertThat(((DocumentOutcome.Failed) outcomes.get(4)).error().reason()).contains("an array");
      DocumentOutcome.Failed noId = (DocumentOutcome.Failed) outcomes.get(5);
      assertThat(noId.index()).isEqualTo(5);
      assertThat(noId.error().type()).isEqualTo(ErrorType.MISSING_ID);
      assertThat(((DocumentOutcome.Decoded) outcomes.get(6)).index()).isEqualTo(6);
    }

    @Test
    void nestedArrayElementFailsWithItsIndex() {
      DocumentOutcome.Failed failed =
          (DocumentOutcome.Failed) documents(DEFAULT, "[[{\"id\":1}]]").outcomes().getFirst();
      assertThat(failed.index()).isZero();
      assertThat(failed.error().type()).isEqualTo(ErrorType.DECODE);
    }

    @Test
    void utf8BomIsSkipped() {
      byte[] json = "{\"id\":\"é\"}".getBytes(UTF_8);
      byte[] value = new byte[json.length + 3];
      value[0] = (byte) 0xEF;
      value[1] = (byte) 0xBB;
      value[2] = (byte) 0xBF;
      System.arraycopy(json, 0, value, 3, json.length);
      DecodeResult.Documents docs = (DecodeResult.Documents) DEFAULT.decode(value, INGEST);
      DocumentOutcome.Decoded doc = (DocumentOutcome.Decoded) docs.outcomes().getFirst();
      assertThat(doc.id()).hasValue("é");
      assertThat(doc.source().toByteArray()).isEqualTo(json);
    }
  }

  @Nested
  class Sources {

    @Test
    void arraySlicesAfterBomAreExact() {
      byte[] json = " [ {\"id\":1} , {\"id\":\"é\"} ] ".getBytes(UTF_8);
      byte[] value = new byte[json.length + 3];
      value[0] = (byte) 0xEF;
      value[1] = (byte) 0xBB;
      value[2] = (byte) 0xBF;
      System.arraycopy(json, 0, value, 3, json.length);
      List<DocumentOutcome> outcomes =
          ((DecodeResult.Documents) DEFAULT.decode(value, INGEST)).outcomes();
      assertThat(source(outcomes.get(0))).isEqualTo("{\"id\":1}");
      assertThat(source(outcomes.get(1))).isEqualTo("{\"id\":\"é\"}");
    }

    @Test
    void objectSourceIsTheValueWithoutSurroundingWhitespace() {
      assertThat(source(single(DEFAULT, "  {\"id\":1, \"n\":1.10, \"b\":1e3}  ")))
          .isEqualTo("{\"id\":1, \"n\":1.10, \"b\":1e3}");
    }

    @Test
    void arrayElementSourcesAreExactSlices() {
      List<DocumentOutcome> outcomes =
          documents(DEFAULT, "[ {\"id\":1,\"s\":\"a, \\\"}\"} ,{\"id\":2, \"z\":[1, {}]}]")
              .outcomes();
      assertThat(source(outcomes.get(0))).isEqualTo("{\"id\":1,\"s\":\"a, \\\"}\"}");
      assertThat(source(outcomes.get(1))).isEqualTo("{\"id\":2, \"z\":[1, {}]}");
    }

    @Test
    void sourceKeepsNumbersAndKeyOrderByteForByte() {
      String json = "{\"z\":1,\"id\":123456789012345678901234567890,\"f\":0.1000000000000000055}";
      assertThat(source(single(DEFAULT, json))).isEqualTo(json);
    }

    @Test
    void prettyPrintedSourceIsCompactedOutsideStrings() {
      String json = "{\n  \"id\" : \"a b\",\n  \"s\" : \"x\\n \\\" y\",\r\n  \"a\" : [ 1, 2 ]\n}";
      assertThat(source(single(DEFAULT, json)))
          .isEqualTo("{\"id\":\"a b\",\"s\":\"x\\n \\\" y\",\"a\":[1,2]}")
          .doesNotContain("\n");
    }

    @Test
    void sourceViewsAgree() throws IOException {
      DocumentOutcome.Decoded doc =
          (DocumentOutcome.Decoded)
              documents(DEFAULT, "[{\"id\":1},{\"id\":22}]").outcomes().get(1);
      DocumentSource source = doc.source();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      source.writeTo(out);
      byte[] fromBuffer = new byte[source.length()];
      source.asByteBuffer().get(fromBuffer);
      assertThat(source.length()).isEqualTo(9);
      assertThat(out.toByteArray()).isEqualTo(source.toByteArray()).isEqualTo(fromBuffer);
      assertThat(source.asByteBuffer().isReadOnly()).isTrue();
    }
  }

  @Nested
  class Ids {

    final Decoder auto =
        Decoder.compile(spec(IdMissing.AUTO, null, null, "iso-8601", OnBadTimestamp.DLQ, 1 << 20));

    @ParameterizedTest
    @ValueSource(
        strings = {
          "{\"id\":\"abc\"}=abc",
          "{\"id\":42}=42",
          "{\"id\":-7}=-7",
          "{\"id\":-0}=0",
          "{\"id\":123456789012345678901234567890}=123456789012345678901234567890",
          "{\"id\":9223372036854775808}=9223372036854775808",
          "{\"id\":\"42\"}=42",
          "{\"id\":\" x \"}= x "
        })
    void usableIds(String testCase) {
      int split = testCase.lastIndexOf("}=");
      String json = testCase.substring(0, split + 1);
      assertThat(single(DEFAULT, json).id()).hasValue(testCase.substring(split + 2));
    }

    @Test
    void nestedPointerAndEscapes() {
      Decoder nested =
          Decoder.compile(
              new DecodeSpec(
                  "/meta/a~1b~0c/0",
                  IdMissing.DLQ,
                  null,
                  null,
                  "iso-8601",
                  OnBadTimestamp.DLQ,
                  1 << 20));
      assertThat(single(nested, "{\"meta\":{\"a/b~c\":[\"x\"]}}").id()).hasValue("x");
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"id\":null}", "{\"other\":1}"})
    void missingIdGoesToDlq(String json) {
      DecodeError error = singleFailure(DEFAULT, json);
      assertThat(error.type()).isEqualTo(ErrorType.MISSING_ID);
      assertThat(error.reason()).isEqualTo("no id at /id");
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"id\":null}"})
    void missingIdWithAutoHasNoId(String json) {
      assertThat(single(auto, json).id()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "{\"id\":\"\"}",
          "{\"id\":1.5}",
          "{\"id\":1.0}",
          "{\"id\":1e3}",
          "{\"id\":true}",
          "{\"id\":{}}",
          "{\"id\":[1]}"
        })
    void unusableIdIsMissingIdEvenWithAuto(String json) {
      assertThat(singleFailure(DEFAULT, json).type()).isEqualTo(ErrorType.MISSING_ID);
      assertThat(singleFailure(auto, json).type()).isEqualTo(ErrorType.MISSING_ID);
    }

    @Test
    void idLengthLimitIsInUtf8Bytes() {
      String ok = "é".repeat(Decoder.MAX_ID_BYTES / 2);
      assertThat(single(DEFAULT, "{\"id\":\"" + ok + "\"}").id()).hasValue(ok);
      DecodeError error = singleFailure(DEFAULT, "{\"id\":\"" + ok + "a\"}");
      assertThat(error.type()).isEqualTo(ErrorType.MISSING_ID);
      assertThat(error.reason()).contains("longer than 512 bytes");
      assertThat(singleFailure(DEFAULT, "{\"id\":" + "1".repeat(513) + "}").reason())
          .contains("longer than 512 bytes");
      assertThat(single(DEFAULT, "{\"id\":" + "1".repeat(512) + "}").id()).isPresent();
    }
  }

  @Nested
  class Versions {

    final Decoder versioned =
        Decoder.compile(
            spec(IdMissing.DLQ, "/version", null, "iso-8601", OnBadTimestamp.DLQ, 1 << 20));

    @ParameterizedTest
    @ValueSource(
        strings = {
          "0=0",
          "7=7",
          "\"42\"=42",
          "9223372036854775807=9223372036854775807",
          "\"9223372036854775807\"=9223372036854775807"
        })
    void usableVersions(String testCase) {
      String[] parts = testCase.split("=");
      assertThat(single(versioned, "{\"id\":1,\"version\":" + parts[0] + "}").version())
          .isEqualTo(OptionalLong.of(Long.parseLong(parts[1])));
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "-1",
          "1.5",
          "2.0",
          "9223372036854775808",
          "\"9223372036854775808\"",
          "\"-1\"",
          "\"+1\"",
          "\"1.0\"",
          "\"\"",
          "true",
          "{}"
        })
    void unusableVersionFailsAsDecode(String version) {
      DecodeError error = singleFailure(versioned, "{\"id\":1,\"version\":" + version + "}");
      assertThat(error.type()).isEqualTo(ErrorType.DECODE);
      assertThat(error.reason()).contains("version at /version is not an integer");
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"id\":1}", "{\"id\":1,\"version\":null}"})
    void missingVersionFailsAsDecode(String json) {
      DecodeError error = singleFailure(versioned, json);
      assertThat(error.type()).isEqualTo(ErrorType.DECODE);
      assertThat(error.reason()).isEqualTo("version at /version is missing");
    }
  }

  @Nested
  class Timestamps {

    static Decoder with(String format, OnBadTimestamp onBad) {
      return Decoder.compile(spec(IdMissing.DLQ, null, "/ts", format, onBad, 1 << 20));
    }

    @Test
    void parsedFromTheField() {
      DocumentOutcome.Decoded doc =
          single(
              with("iso-8601", OnBadTimestamp.DLQ), "{\"id\":1,\"ts\":\"2026-01-02T03:04:05Z\"}");
      assertThat(doc.timestamp()).isEqualTo(Instant.parse("2026-01-02T03:04:05Z"));
      assertThat(doc.timestampSource()).isEqualTo(TimestampSource.FIELD);
    }

    @ParameterizedTest
    @ValueSource(
        strings = {"{\"id\":1}", "{\"id\":1,\"ts\":null}", "{\"id\":1,\"ts\":\"yesterday\"}"})
    void badTimestampGoesToDlq(String json) {
      DecodeError error = singleFailure(with("iso-8601", OnBadTimestamp.DLQ), json);
      assertThat(error.type()).isEqualTo(ErrorType.BAD_TIMESTAMP);
      assertThat(error.reason()).startsWith("timestamp at /ts ");
    }

    @ParameterizedTest
    @ValueSource(
        strings = {"{\"id\":1}", "{\"id\":1,\"ts\":null}", "{\"id\":1,\"ts\":\"yesterday\"}"})
    void badTimestampFallsBackToIngestTime(String json) {
      DocumentOutcome.Decoded doc = single(with("iso-8601", OnBadTimestamp.INGEST_TIME), json);
      assertThat(doc.timestamp()).isEqualTo(INGEST);
      assertThat(doc.timestampSource()).isEqualTo(TimestampSource.INGEST_TIME_FALLBACK);
    }

    @Test
    void idErrorsWinOverTimestampErrors() {
      assertThat(singleFailure(with("iso-8601", OnBadTimestamp.DLQ), "{\"ts\":1}").type())
          .isEqualTo(ErrorType.MISSING_ID);
    }

    @Test
    void perElementInArrays() {
      DecodeResult.Documents docs =
          documents(
              with("epoch-millis", OnBadTimestamp.DLQ),
              "[{\"id\":1,\"ts\":0},{\"id\":2,\"ts\":\"x\"},{\"id\":3,\"ts\":1000}]");
      assertThat(docs.outcomes().get(0)).isInstanceOf(DocumentOutcome.Decoded.class);
      assertThat(((DocumentOutcome.Failed) docs.outcomes().get(1)).error().type())
          .isEqualTo(ErrorType.BAD_TIMESTAMP);
      assertThat(((DocumentOutcome.Decoded) docs.outcomes().get(2)).timestamp())
          .isEqualTo(Instant.ofEpochSecond(1));
    }
  }

  @Nested
  class Compile {

    @ParameterizedTest
    @ValueSource(strings = {"", "id", "/a~2", "/a~"})
    void invalidIdPointer(String pointer) {
      assertThatIllegalArgumentException()
          .isThrownBy(() ->
              Decoder.compile(
                  new DecodeSpec(
                      pointer, IdMissing.DLQ, null, null, "iso-8601", OnBadTimestamp.DLQ, 1)));
    }

    @Test
    void invalidVersionAndTimestampPointers() {
      assertThatIllegalArgumentException()
          .isThrownBy(() ->
              Decoder.compile(spec(IdMissing.DLQ, "v", null, "iso-8601", OnBadTimestamp.DLQ, 1)))
          .withMessageContaining("version pointer");
      assertThatIllegalArgumentException()
          .isThrownBy(() ->
              Decoder.compile(spec(IdMissing.DLQ, null, "ts", "iso-8601", OnBadTimestamp.DLQ, 1)))
          .withMessageContaining("timestamp pointer");
    }

    @ParameterizedTest
    @ValueSource(strings = {"iso8601", "epoch", "pattern:", "pattern:  ", "pattern:yyyy-MM-dd'T"})
    void invalidTimestampFormat(String format) {
      assertThatIllegalArgumentException()
          .isThrownBy(() ->
              Decoder.compile(spec(IdMissing.DLQ, null, "/ts", format, OnBadTimestamp.DLQ, 1)));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1})
    void nonPositiveMaxRecordBytes(long max) {
      assertThatIllegalArgumentException()
          .isThrownBy(() ->
              Decoder.compile(
                  spec(IdMissing.DLQ, null, null, "iso-8601", OnBadTimestamp.DLQ, max)));
    }

    @Test
    void errorTypesMatchDesignWireNames() {
      assertThat(ErrorType.values())
          .extracting(ErrorType::wireName)
          .containsExactly("decode", "missing_id", "bad_timestamp");
    }
  }
}
