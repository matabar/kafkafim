package io.github.matabar.kafkafim.decode;

import java.util.Objects;

/**
 * How to decode one pipeline's records: the decode-relevant part of {@code
 * kafkafim.pipelines.<name>.document} plus {@code source.max-record-bytes} (DESIGN §4.4, §4.5,
 * §5.2), with the configuration defaults already applied. {@link Decoder#compile} validates it.
 *
 * @param idPointer JSON pointer to the {@code _id}, e.g. {@code /id}
 * @param idMissing what to do when the id is absent
 * @param versionPointer JSON pointer to the external version, or {@code null} for none
 * @param timestampPointer JSON pointer to the event time, or {@code null} to use the ingest time
 * @param timestampFormat {@code iso-8601}, {@code epoch-millis}, {@code epoch-seconds} or {@code
 *     pattern:<DateTimeFormatter>}
 * @param onBadTimestamp what to do with a missing or unparsable timestamp
 * @param maxRecordBytes largest accepted record value, in bytes
 */
public record DecodeSpec(
    String idPointer,
    IdMissing idMissing,
    String versionPointer,
    String timestampPointer,
    String timestampFormat,
    OnBadTimestamp onBadTimestamp,
    long maxRecordBytes) {

  /** Rejects nulls in the required components. */
  public DecodeSpec {
    Objects.requireNonNull(idPointer, "idPointer");
    Objects.requireNonNull(idMissing, "idMissing");
    Objects.requireNonNull(timestampFormat, "timestampFormat");
    Objects.requireNonNull(onBadTimestamp, "onBadTimestamp");
  }

  /** {@code document.id-missing}. */
  public enum IdMissing {
    /** {@code error.type=missing_id}. */
    DLQ,
    /** No {@code _id}: Elasticsearch generates one, so retries duplicate. */
    AUTO
  }

  /** {@code document.on-bad-timestamp}. */
  public enum OnBadTimestamp {
    /** {@code error.type=bad_timestamp}. */
    DLQ,
    /** Use the record's ingest time instead. */
    INGEST_TIME
  }
}
