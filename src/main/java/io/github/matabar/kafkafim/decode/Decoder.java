package io.github.matabar.kafkafim.decode;

import io.github.matabar.kafkafim.decode.DecodeSpec.IdMissing;
import io.github.matabar.kafkafim.decode.DecodeSpec.OnBadTimestamp;
import io.github.matabar.kafkafim.decode.DocumentOutcome.TimestampSource;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.regex.Pattern;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonPointer;
import tools.jackson.core.JsonToken;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Decodes Kafka record values into documents (DESIGN §4.4, §4.5). Thread-safe and immutable; build
 * one per pipeline with {@link #compile} and call {@link #decode} on the hot path, which never
 * throws for any input.
 *
 * <p>Rules:
 *
 * <ul>
 *   <li>A value larger than {@code maxRecordBytes} fails as a whole without being parsed. {@code
 *       null} (a tombstone), empty, whitespace-only, invalid JSON (including invalid UTF-8, a
 *       duplicate key at any depth, nesting deeper than {@value #MAX_NESTING_DEPTH}), a scalar,
 *       JSON {@code null} and trailing content after the value also fail as a whole: once the bytes
 *       are not one valid JSON value nothing inside them is trusted. A leading UTF-8 byte order
 *       mark is skipped; a second one, a UTF-16/32 byte order mark or a NUL in the first four bytes
 *       (UTF-16/32 without a mark) fails the record.
 *   <li>An object is one document; an array is one document per element. An element that is not an
 *       object (a scalar, {@code null}, a nested array) fails on its own, as do id, version and
 *       timestamp problems: the other elements of the record are still indexed. An empty array is
 *       zero documents.
 *   <li>Duplicate keys are rejected (strict detection) because Elasticsearch rejects them too, and
 *       because routing and {@code _id} would otherwise read whichever duplicate Jackson kept.
 *   <li>{@code _id}: a JSON string (1 to 512 UTF-8 bytes, Elasticsearch's limit) is used as is; a
 *       JSON integer of any size is its decimal digits, which is canonical because JSON forbids
 *       leading zeros and {@code +} ({@code -0} becomes {@code 0}). A number with a fraction or
 *       exponent, a boolean, an object or an array is {@code missing_id} in both {@code id-missing}
 *       modes: {@code auto} only covers an absent or {@code null} id.
 *   <li>Version: a JSON integer or a string of ASCII digits, in {@code [0, Long.MAX_VALUE]}.
 *       Anything else, including an absent or {@code null} value when {@code version-field} is
 *       configured, is a {@code decode} error: an external-version write without a version would
 *       let a stale document overwrite a newer one.
 *   <li>Timestamp: see {@link TimestampFormat}; absent, {@code null} or unparsable follows {@code
 *       on-bad-timestamp}.
 * </ul>
 *
 * <p>Memory: values are bounded by {@code maxRecordBytes} before parsing, so string, number and
 * token sizes are too; Jackson's {@link StreamReadConstraints} additionally cap nesting at {@value
 * #MAX_NESTING_DEPTH}, number literals at {@value #MAX_NUMBER_LENGTH} digits and keys at {@value
 * #MAX_NAME_LENGTH} UTF-8 bytes. The parsed trees are kept for routing and can take far more heap
 * than the value: the worst case, an array of empty objects, measured about 85 bytes per element,
 * roughly 28 times the value size (an 8 MiB record retained about 237 MB).
 */
public final class Decoder {

  /** Deepest accepted nesting of objects and arrays (Jackson's default, stated explicitly). */
  public static final int MAX_NESTING_DEPTH = 500;

  /** Longest accepted number literal, in characters (Jackson's default, stated explicitly). */
  public static final int MAX_NUMBER_LENGTH = 1000;

  /** Longest accepted object key, in UTF-8 bytes (Jackson's default, stated explicitly). */
  public static final int MAX_NAME_LENGTH = 50_000;

  /** Elasticsearch's {@code _id} limit, in UTF-8 bytes. */
  public static final int MAX_ID_BYTES = 512;

  private static final JsonMapper MAPPER =
      JsonMapper.builder(
              JsonFactory.builder()
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(MAX_NESTING_DEPTH)
                          .maxNumberLength(MAX_NUMBER_LENGTH)
                          .maxNameLength(MAX_NAME_LENGTH)
                          .build())
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .build())
          .build();

  private static final Pattern VERSION_DIGITS = Pattern.compile("[0-9]{1,19}");
  private static final int REASON_LIMIT = 256;
  private static final int BOM_LENGTH = 3;

  private final DecodeSpec spec;
  private final JsonPointer idPointer;
  private final JsonPointer versionPointer;
  private final JsonPointer timestampPointer;
  private final TimestampFormat timestampFormat;

  private Decoder(DecodeSpec spec) {
    this.spec = spec;
    this.idPointer = pointer("id", spec.idPointer());
    this.versionPointer =
        spec.versionPointer() == null ? null : pointer("version", spec.versionPointer());
    this.timestampPointer =
        spec.timestampPointer() == null ? null : pointer("timestamp", spec.timestampPointer());
    this.timestampFormat = TimestampFormat.compile(spec.timestampFormat());
    if (spec.maxRecordBytes() <= 0) {
      throw new IllegalArgumentException(
          "maxRecordBytes must be positive, was " + spec.maxRecordBytes());
    }
  }

  /**
   * Validates and compiles {@code spec}; called at pipeline start, never on the hot path.
   *
   * @throws IllegalArgumentException if a pointer is not an RFC 6901 pointer to a field, the
   *     timestamp format or pattern does not compile, or {@code maxRecordBytes} is not positive
   */
  public static Decoder compile(DecodeSpec spec) {
    return new Decoder(Objects.requireNonNull(spec, "spec"));
  }

  /** The spec this decoder was compiled from. */
  public DecodeSpec spec() {
    return spec;
  }

  private static JsonPointer pointer(String what, String pointer) {
    if (pointer.isEmpty() || pointer.charAt(0) != '/') {
      throw new IllegalArgumentException(
          what
              + " pointer '"
              + pointer
              + "' is not a JSON pointer to a field (must start with '/')");
    }
    for (int i = pointer.indexOf('~'); i >= 0; i = pointer.indexOf('~', i + 1)) {
      if (i + 1 >= pointer.length()
          || (pointer.charAt(i + 1) != '0' && pointer.charAt(i + 1) != '1')) {
        throw new IllegalArgumentException(
            what + " pointer '" + pointer + "': '~' must be followed by '0' or '1'");
      }
    }
    return JsonPointer.compile(pointer);
  }

  /**
   * Decodes one record value.
   *
   * @param value the record value; {@code null} for a tombstone
   * @param ingestTime the record's broker or producer timestamp, used when no timestamp field is
   *     configured and as the {@code on-bad-timestamp: ingest-time} fallback
   */
  public DecodeResult decode(byte[] value, Instant ingestTime) {
    Objects.requireNonNull(ingestTime, "ingestTime");
    if (value == null) {
      return recordFailure("record value is null (tombstone)");
    }
    if (value.length == 0) {
      return recordFailure("record value is empty");
    }
    if (value.length > spec.maxRecordBytes()) {
      return recordFailure(
          "record value is "
              + value.length
              + " bytes, larger than max-record-bytes "
              + spec.maxRecordBytes());
    }
    // Parser byte offsets are relative to `base`; Jackson's own BOM detection only triggers when
    // enough bytes follow the BOM, so it is stripped here.
    int base = hasBom(value, 0) ? BOM_LENGTH : 0;
    String notUtf8 = notUtf8(value, base);
    if (notUtf8 != null) {
      return recordFailure(notUtf8);
    }
    try (JsonParser parser = MAPPER.createParser(value, base, value.length - base)) {
      JsonToken root = parser.nextToken();
      if (root == null) {
        return recordFailure("record value contains no JSON value");
      }
      if (parser.currentTokenLocation().getByteOffset() < 0) {
        // Jackson detected a non-UTF-8 encoding and switched to a char-based parser, whose
        // locations carry no byte offsets; notUtf8 should have caught it.
        return recordFailure("record value is not UTF-8");
      }
      List<DocumentOutcome> outcomes;
      boolean array;
      switch (root) {
        case START_OBJECT -> {
          array = false;
          outcomes = List.of(element(parser, value, base, 0, ingestTime));
        }
        case START_ARRAY -> {
          array = true;
          outcomes = new ArrayList<>();
          for (JsonToken t = parser.nextToken(); t != JsonToken.END_ARRAY; t = parser.nextToken()) {
            int index = outcomes.size();
            if (t == JsonToken.START_OBJECT) {
              outcomes.add(element(parser, value, base, index, ingestTime));
            } else {
              parser.skipChildren();
              outcomes.add(
                  new DocumentOutcome.Failed(
                      index,
                      new DecodeError(
                          ErrorType.DECODE,
                          "array element " + index + " is " + describe(t) + ", not an object")));
            }
          }
        }
        default -> {
          return recordFailure(
              "record value is " + describe(root) + ", not an object or an array of objects");
        }
      }
      if (parser.nextToken() != null) {
        return recordFailure("trailing content after the JSON value");
      }
      return new DecodeResult.Documents(array, outcomes);
    } catch (JacksonException e) {
      String where = e.getLocation() == null ? "" : " at byte " + e.getLocation().getByteOffset();
      return recordFailure(
          "invalid JSON" + where + ": " + truncate(String.valueOf(e.getOriginalMessage())));
    }
  }

  /** Reads the object starting at the parser's current {@code START_OBJECT}. */
  private DocumentOutcome element(
      JsonParser parser, byte[] value, int base, int index, Instant ingestTime) {
    final int start = base + (int) parser.currentTokenLocation().getByteOffset();
    JsonNode node = parser.readValueAsTree();
    final int end = base + (int) parser.currentLocation().getByteOffset();

    Optional<String> id;
    JsonNode idNode = node.at(idPointer);
    if (idNode.isMissingNode() || idNode.isNull()) {
      if (spec.idMissing() == IdMissing.DLQ) {
        return failed(index, ErrorType.MISSING_ID, "no id at " + spec.idPointer());
      }
      id = Optional.empty();
    } else {
      String error = idError(idNode);
      if (error != null) {
        return failed(index, ErrorType.MISSING_ID, "id at " + spec.idPointer() + " " + error);
      }
      id =
          Optional.of(
              idNode.isString() ? idNode.stringValue() : idNode.bigIntegerValue().toString());
    }

    OptionalLong version = OptionalLong.empty();
    if (versionPointer != null) {
      JsonNode versionNode = node.at(versionPointer);
      long parsed = version(versionNode);
      if (parsed < 0) {
        return failed(
            index,
            ErrorType.DECODE,
            "version at "
                + spec.versionPointer()
                + " "
                + (versionNode.isMissingNode() || versionNode.isNull()
                    ? "is missing"
                    : "is not an integer in [0, " + Long.MAX_VALUE + "]"));
      }
      version = OptionalLong.of(parsed);
    }

    Instant timestamp = ingestTime;
    TimestampSource source = TimestampSource.INGEST_TIME;
    if (timestampPointer != null) {
      JsonNode tsNode = node.at(timestampPointer);
      String error;
      if (tsNode.isMissingNode() || tsNode.isNull()) {
        error = "is missing";
      } else {
        TimestampFormat.Parse parse = timestampFormat.parse(tsNode);
        error = parse.error();
        timestamp = parse.instant();
      }
      if (error == null) {
        source = TimestampSource.FIELD;
      } else if (spec.onBadTimestamp() == OnBadTimestamp.INGEST_TIME) {
        timestamp = ingestTime;
        source = TimestampSource.INGEST_TIME_FALLBACK;
      } else {
        return failed(
            index,
            ErrorType.BAD_TIMESTAMP,
            "timestamp at " + spec.timestampPointer() + " " + truncate(error));
      }
    }
    return new DocumentOutcome.Decoded(
        index, node, DocumentSource.of(value, start, end), id, version, timestamp, source);
  }

  private static boolean hasBom(byte[] value, int at) {
    return value.length >= at + BOM_LENGTH
        && value[at] == (byte) 0xEF
        && value[at + 1] == (byte) 0xBB
        && value[at + 2] == (byte) 0xBF;
  }

  /**
   * Why the value is visibly not UTF-8 JSON, or {@code null}. Jackson would otherwise auto-detect
   * UTF-16/32 and parse it with byte offsets the source slices cannot use. JSON text never contains
   * a raw NUL, and {@code 0xFE}/{@code 0xFF} never occur in UTF-8.
   */
  private static String notUtf8(byte[] value, int base) {
    if (hasBom(value, base)) {
      return "record value starts with more than one byte order mark";
    }
    int first = value.length > base ? value[base] & 0xFF : -1;
    if (first == 0xFE || first == 0xFF) {
      return "record value is not UTF-8 (UTF-16/32 byte order mark)";
    }
    for (int i = base; i < Math.min(value.length, base + 4); i++) {
      if (value[i] == 0) {
        return "record value is not UTF-8 (NUL byte; UTF-16/32?)";
      }
    }
    return null;
  }

  /** Why a present, non-null id node is unusable, or {@code null}. */
  private static String idError(JsonNode idNode) {
    if (idNode.isString()) {
      String id = idNode.stringValue();
      if (id.isEmpty()) {
        return "is an empty string";
      }
      // UTF-8 is at most 3 bytes per UTF-16 char; only count when it might exceed the limit.
      if (id.length() * 3 > MAX_ID_BYTES
          && id.getBytes(StandardCharsets.UTF_8).length > MAX_ID_BYTES) {
        return "is longer than " + MAX_ID_BYTES + " bytes";
      }
      return null;
    }
    if (idNode.isIntegralNumber()) {
      // Jackson caps literals at MAX_NUMBER_LENGTH (> MAX_ID_BYTES) characters.
      return idNode.asString().length() > MAX_ID_BYTES
          ? "is longer than " + MAX_ID_BYTES + " bytes"
          : null;
    }
    if (idNode.isNumber()) {
      return "is a number with a fraction or exponent; use a string or an integer";
    }
    return "is "
        + idNode.getNodeType().name().toLowerCase(Locale.ROOT)
        + ", not a string or an integer";
  }

  /** The version, or {@code -1} if {@code node} is not a usable one. */
  private static long version(JsonNode node) {
    if (node.isIntegralNumber()) {
      return node.canConvertToLong() && node.longValue() >= 0 ? node.longValue() : -1;
    }
    if (node.isString() && VERSION_DIGITS.matcher(node.stringValue()).matches()) {
      try {
        return Long.parseLong(node.stringValue());
      } catch (NumberFormatException e) {
        return -1;
      }
    }
    return -1;
  }

  private static DocumentOutcome failed(int index, ErrorType type, String reason) {
    return new DocumentOutcome.Failed(index, new DecodeError(type, reason));
  }

  private static DecodeResult recordFailure(String reason) {
    return new DecodeResult.RecordFailure(new DecodeError(ErrorType.DECODE, reason));
  }

  private static String describe(JsonToken token) {
    return switch (token) {
      case START_OBJECT -> "an object";
      case START_ARRAY -> "an array";
      case VALUE_STRING -> "a string";
      case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> "a number";
      case VALUE_TRUE, VALUE_FALSE -> "a boolean";
      case VALUE_NULL -> "null";
      default -> token.name();
    };
  }

  private static String truncate(String text) {
    return text.length() <= REASON_LIMIT ? text : text.substring(0, REASON_LIMIT) + "...";
  }
}
